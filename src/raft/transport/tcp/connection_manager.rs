//! TCP connection manager.
//!
//! This module owns the lifecycle and management policy for TCP
//! connections between RustyRaft peers.
//!
//! The manager is responsible for:
//!
//! - maintaining peer configuration,
//! - creating outbound connections,
//! - registering inbound connections,
//! - managing reader and writer tasks,
//! - detecting failed connections,
//! - scheduling reconnect attempts,
//! - applying reconnect backoff,
//! - routing outgoing messages to writer tasks.
//!
//! It does not contain Raft protocol decisions.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::raft::state::ServerId;
use crate::raft::transport::message::RaftMessage;

use super::connection::TcpConnection;
use super::error::TcpTransportError;

/// Initial reconnect delay.
const INITIAL_RECONNECT_DELAY: Duration =
    Duration::from_millis(100);

/// Maximum reconnect delay.
const MAX_RECONNECT_DELAY: Duration =
    Duration::from_secs(30);

/// Maximum time allowed for TCP connection and handshake.
const CONNECTION_ATTEMPT_TIMEOUT: Duration =
    Duration::from_secs(5);

/// Maximum number of concurrent outbound connection attempts.
const MAX_CONCURRENT_CONNECTION_ATTEMPTS: usize = 16;

/// Maximum number of messages waiting to be sent to one peer.
const OUTGOING_CHANNEL_CAPACITY: usize = 256;

/// Maximum number of incoming Raft messages waiting to be
/// processed by the runtime.
const INCOMING_CHANNEL_CAPACITY: usize = 256;

/// Maximum number of connection events waiting to be processed.
const CONNECTION_EVENT_CAPACITY: usize = 64;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

/// State used to schedule reconnect attempts for one peer.
#[derive(Debug, Clone)]
struct ReconnectState {
    /// Number of consecutive failed connection attempts.
    attempts: u32,

    /// Earliest time at which another connection attempt may occur.
    next_retry_at: Instant,
}

impl ReconnectState {
    /// Creates reconnect state that allows an immediate attempt.
    fn new(now: Instant) -> Self {
        Self {
            attempts: 0,
            next_retry_at: now,
        }
    }

    /// Returns whether a reconnect attempt is currently allowed.
    fn ready(&self, now: Instant) -> bool {
        now >= self.next_retry_at
    }

    /// Records a failed connection attempt and calculates the next
    /// reconnect time using exponential backoff.
    ///
    /// The delay starts at 100ms and doubles after each failure until
    /// it reaches the maximum reconnect delay of 30 seconds.
    fn record_failure(&mut self, now: Instant) {
        // Count this failed connection attempt.
        self.attempts = self.attempts.saturating_add(1);

        // Convert the attempt count into a zero-based exponent.
        // attempt is purely used for temp computation - don't get confuse
        let attempt = self.attempts.saturating_sub(1);

        // Calculate the exponential multiplier.
        // The exponent is capped to avoid an unnecessarily large value.
        let multiplier = 2u32.pow(attempt.min(10));

        // Calculate the reconnect delay.
        let mut delay = INITIAL_RECONNECT_DELAY * multiplier;

        // Do not allow the reconnect delay to exceed the maximum.
        if delay > MAX_RECONNECT_DELAY {
            delay = MAX_RECONNECT_DELAY;
        }

        // Schedule the next reconnect attempt.
        self.next_retry_at = now + delay;
    }

    /// Resets the backoff after a successful connection.
    fn reset(&mut self, now: Instant) {
        self.attempts = 0;
        self.next_retry_at = now;
    }
}

/// Configuration for a Raft peer.
///
/// The address is used to reach the peer over TCP.
/// `server_id` is the peer's unique Raft identity.
#[derive(Debug, Clone, Copy)]
pub struct PeerAddress {
    pub server_id: ServerId,
    pub address: SocketAddr,
}

/// Event sent by connection tasks to the connection manager.
#[derive(Debug)]
enum ConnectionEvent {
    /// An outbound connection attempt completed.
    AttemptCompleted {
        peer_id: ServerId,
        result: Result<TcpConnection, TcpTransportError>,
    },

    /// A reader or writer task detected a connection failure.
    ///
    /// The connection ID prevents a delayed event from removing
    /// a newer connection to the same peer.
    Failed {
        peer_id: ServerId,
        connection_id: u64,
        error: TcpTransportError,
    },
}

/// State associated with one active peer connection.
///
/// The TCP stream itself is owned by the reader and writer tasks.
/// The manager only keeps the channel used to send messages to the
/// writer and the task handles used to manage their lifecycle.
struct PeerConnection<C> {
    /// Unique identity of this registered connection.
    connection_id: u64,

    /// Channel used to send outgoing messages to the writer task.
    outgoing_message_sender: mpsc::Sender<RaftMessage<C>>,

    /// Handle for the reader task.
    reader_task: JoinHandle<()>,

    /// Handle for the writer task.
    writer_task: JoinHandle<()>,
}

/// Maintains TCP connections to Raft peers.
///
/// The manager does not directly perform blocking reads or writes.
/// Each established TCP connection is split into a reader task and
/// a writer task.
pub struct TcpConnectionManager<C> {
    /// Identity of this Raft node.
    local_server_id: ServerId,

    /// Known peer addresses.
    peers: HashMap<ServerId, SocketAddr>,

    /// Currently active TCP connections.
    connections: HashMap<ServerId, PeerConnection<C>>,

    /// Reconnect state for peers whose connections are unavailable.
    reconnect_state: HashMap<ServerId, ReconnectState>,

    /// Peers with outbound connection attempts currently in flight.
    in_flight_attempts: HashSet<ServerId>,

    /// ID assigned to the next registered connection.
    next_connection_id: u64,

    /// Sends decoded Raft messages from reader tasks to the
    /// transport.
    incoming_message_sender: mpsc::Sender<RaftMessage<C>>,

    /// Receives connection events from reader, writer, and
    /// outbound connection tasks.
    connection_event_receiver: mpsc::Receiver<ConnectionEvent>,

    /// Sender used by connection tasks to report events.
    connection_event_sender: mpsc::Sender<ConnectionEvent>,

    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    connection_attempt_tasks: HashMap<ServerId, JoinHandle<()>>,
}

impl<C> TcpConnectionManager<C>
where
    C: serde::Serialize
        + serde::de::DeserializeOwned
        + Send
        + 'static,
{
    /// Creates a connection manager.
    ///
    /// The returned receiver is the central incoming-message queue
    /// used by `TcpTransport`.
    ///
    /// Reader tasks push decoded Raft messages into this queue.
    ///
    /// Send trait is required - it is safe to transfer ownership
    /// of this value to another thread - required because tokio
    /// may execute spawn task on a separate worker thread.
    ///
    /// 'static means that this type does not contain references
    /// that are required to remain valid for a shorter lifetime.
    ///
    /// We need both serialize and deserialize because we want to
    /// encode outgoing messages and decode incoming messages.
    pub fn new(
        local_server_id: ServerId,
        peers: Vec<PeerAddress>,
    ) -> (
        Self,
        mpsc::Receiver<RaftMessage<C>>,
    ) {
        // Server Id -> SocketAddress
        let mut peer_addresses = HashMap::new();

        // Server Id -> Reconnect state
        let mut reconnect_state = HashMap::new();

        let now = Instant::now();

        // For each known peer:
        // 1) populate the peer address map
        // 2) populate the reconnect state
        for peer in peers {
            if peer.server_id == local_server_id {
                continue;
            }

            peer_addresses.insert(peer.server_id, peer.address);
            reconnect_state.insert(
                peer.server_id,
                ReconnectState::new(now),
            );
        }

        // Create bounded channels. The mpsc gives us back
        // transmitter (tx) and receiver (rx).

        // This channel is used to send and receive Raft messages
        // from peers.
        let (incoming_message_sender, incoming_rx) =
            mpsc::channel(INCOMING_CHANNEL_CAPACITY);

        // This channel is used to send and receive connection events.
        let (connection_event_sender, connection_event_receiver) =
            mpsc::channel(CONNECTION_EVENT_CAPACITY);
        let (shutdown_tx, shutdown_rx) = 
            tokio::sync::watch::channel(false);

        let manager = Self {
            local_server_id,
            peers: peer_addresses,
            connections: HashMap::new(),
            reconnect_state,
            in_flight_attempts: HashSet::new(),
            next_connection_id: 1,
            incoming_message_sender,
            connection_event_receiver,
            connection_event_sender,
            shutdown_rx,
            shutdown_tx,
            connection_attempt_tasks: HashMap::new(),
        };

        (manager, incoming_rx)
    }

    /// Returns the local Raft server identity.
    pub fn local_server_id(&self) -> ServerId {
        self.local_server_id
    }

    /// Returns whether a connection to the given peer exists.
    pub fn contains(&self, peer_id: ServerId) -> bool {
        self.connections.contains_key(&peer_id)
    }

    /// Returns the number of currently active connections.
    pub fn len(&self) -> usize {
        self.connections.len()
    }

    /// Returns whether there are no active connections.
    pub fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }

    /// Returns whether this node owns the outbound connection to
    /// the given peer.
    ///
    /// The lower `ServerId` initiates the TCP connection.
    pub fn owns_outbound_connection(
        &self,
        peer_id: ServerId,
    ) -> bool {
        self.local_server_id < peer_id
    }

    /// Maintains outbound connections.
    ///
    /// This is about the health of the peer connections.
    ///
    /// The connection reader/writer tasks run independently
    /// in the background. When one detects a failure, it sends
    /// an event to the connection manager.
    ///
    /// Background connection tasks detect failures, send events,
    /// and `maintain_connections()` processes those events to
    /// update connection state and reconnect logic.
    ///
    /// This method performs reconnect attempts whose backoff timers
    /// have expired. Actual connection attempts run in independent
    /// Tokio tasks, so one slow peer does not block other peers.
    ///
    /// It deliberately does not sleep. The caller controls how often
    /// this method is invoked.
    pub async fn maintain_connections(&mut self, now: Instant) {
        // Do not schedule or process new work after shutdown.
        if self.is_shutting_down() {
            return;
        }

        self.process_connection_events().await;

        // Shutdown may have started while processing events.
        if self.is_shutting_down() {
            return;
        }

        let available_slots = MAX_CONCURRENT_CONNECTION_ATTEMPTS
            .saturating_sub(self.in_flight_attempts.len());

        if available_slots == 0 {
            return;
        }

        let mut peers: Vec<ServerId> = self.peers.keys().copied().collect();
        peers.sort();

        let mut scheduled = 0;

        for peer_id in peers {
            if self.is_shutting_down() {
                break;
            }

            // Stop once the bounded number of attempts is scheduled.
            if scheduled >= available_slots {
                break;
            }

            // The lower server ID owns the outbound connection.
            if !self.owns_outbound_connection(peer_id) {
                continue;
            }

            // Skip peers that already have an established connection.
            if self.connections.contains_key(&peer_id) {
                continue;
            }

            // Avoid duplicate outbound connection attempts.
            if self.in_flight_attempts.contains(&peer_id) {
                continue;
            }

            // Check whether the reconnect backoff has expired.
            let should_attempt = self
                .reconnect_state
                .get(&peer_id)
                .map(|state| state.ready(now))
                .unwrap_or(false);

            if !should_attempt {
                continue;
            }

            // Look up the address registered for this peer.
            let address = match self.peers.get(&peer_id) {
                Some(address) => *address,
                None => continue,
            };

            // Mark the peer before spawning the task.
            self.in_flight_attempts.insert(peer_id);

            let local_server_id = self.local_server_id;
            let event_sender = self.connection_event_sender.clone();
            let mut shutdown_rx = self.shutdown_rx.clone();

            // Establish the connection and perform the handshake
            // independently so other peers remain available.
            let handle = tokio::spawn(async move {
                let result = tokio::select! {
                    _ = shutdown_rx.changed() => {
                        return;
                    }

                    result = tokio::time::timeout(
                        CONNECTION_ATTEMPT_TIMEOUT,
                        async {
                            // Connect to the peer.
                            let stream =
                                tokio::net::TcpStream::connect(address)
                                    .await?;

                            // Perform the connection handshake and
                            // protocol validation.
                            TcpConnection::establish(
                                stream,
                                local_server_id,
                                peer_id,
                            )
                            .await
                        },
                    ) => {
                        result.unwrap_or_else(|_| {
                            Err(TcpTransportError::ConnectionFailed(
                                format!(
                                    "connection attempt to {:?} timed out",
                                    peer_id,
                                ),
                            ))
                        })
                    }
                };

                // Let the manager update connection and reconnect state.
                let _ = event_sender
                    .send(ConnectionEvent::AttemptCompleted {
                        peer_id,
                        result,
                    })
                    .await;
            });

            // Retain the handle so shutdown can cancel and await it.
            self.connection_attempt_tasks.insert(peer_id, handle);

            scheduled += 1;
        }
    }

    /// Processes connection events reported by connection tasks.
    ///
    /// The tasks themselves do not modify the connection manager.
    /// They only report results through the event channel.
    async fn process_connection_events(&mut self) {
        // try_recv receives a value without waiting for a future event.
        while let Ok(event) =
            self.connection_event_receiver.try_recv()
        {
            match event {
                ConnectionEvent::AttemptCompleted {
                    peer_id,
                    result,
                } => {
                    self.in_flight_attempts.remove(&peer_id);

                    match result {
                        Ok(connection) => {
                            // An inbound connection might have been
                            // registered while this attempt was running.
                            if self.connections.contains_key(&peer_id) {
                                continue;
                            }

                            // here manager will do mutation to the reconnection
                            // state as we are good to connect, we will 
                            // reset that and in case of error we will record
                            // a failure - which means one less attempt retry
                            // attempt from the total reamining retries (if any)
                            match self.register_connection(connection).await {
                                Ok(()) => {
                                    if let Some(state) =
                                        self.reconnect_state.get_mut(&peer_id)
                                    {
                                        state.reset(Instant::now());
                                    }
                                }
                                Err(error) => {
                                    tracing::debug!(
                                        ?peer_id,
                                        %error,
                                        "failed to register connection"
                                    );

                                    if let Some(state) =
                                        self.reconnect_state.get_mut(&peer_id)
                                    {
                                        state.record_failure(Instant::now());
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            tracing::debug!(
                                ?peer_id,
                                %error,
                                "outbound connection attempt failed"
                            );

                            if let Some(state) =
                                self.reconnect_state.get_mut(&peer_id)
                            {
                                state.record_failure(Instant::now());
                            }
                        }
                    }
                }
                // The connection failed event is send by - eithe rreader
                // or writer task and as response for such event the manager
                // has to close every thing - stop reader and writer task
                ConnectionEvent::Failed {
                    peer_id,
                    connection_id,
                    error,
                } => {
                    tracing::warn!(
                        ?peer_id,
                        connection_id,
                        %error,
                        "TCP connection failed"
                    );

                    // Ignore failures reported by an older connection.
                    let is_current = self
                        .connections
                        .get(&peer_id)
                        .map(|connection| {
                            connection.connection_id == connection_id
                        })
                        .unwrap_or(false);

                    if is_current {
                        self.connection_failed(peer_id, connection_id);
                    }
                }
            }
        }
    }

    /// Creates an outbound connection to a peer immediately.
    ///
    /// This method can also be used during initial startup when the
    /// caller wants to establish a connection eagerly.
    pub async fn connect(
        &mut self,
        peer_id: ServerId,
        address: SocketAddr,
    ) -> Result<(), TcpTransportError> {
        if self.is_shutting_down() {
            return Err(TcpTransportError::TransportShuttingDown);
        }
        // Case 1. We should not connect our self
        if peer_id == self.local_server_id {
            return Err(TcpTransportError::UnknownPeer(
                "cannot connect to the local server".to_string(),
            ));
        }

        // Case 2. We must be aware of the peer, we are having
        // some faith in who ever populated the peers known to
        // me
        if !self.peers.contains_key(&peer_id) {
            return Err(TcpTransportError::UnknownPeer(
                format!("peer {:?} is not configured", peer_id),
            ));
        }

        // Case 3. This is the contract to avoid race conditions
        // We are allowing outbond connection based on the following
        // policy - server with lower only can initiate outbound connection
        if !self.owns_outbound_connection(peer_id) {
            return Err(TcpTransportError::UnknownPeer(
                format!(
                    "server {:?} does not own the outbound connection \
                     to peer {:?}",
                    self.local_server_id,
                    peer_id,
                ),
            ));
        }

        // Case 4. Already connected.
        if self.connections.contains_key(&peer_id) {
            return Ok(());
        }

        // Case 5. If not so far, then attempt to connect 
        // in a timeout 
        let result = tokio::time::timeout(
            CONNECTION_ATTEMPT_TIMEOUT,
            async {
                let stream =
                    tokio::net::TcpStream::connect(address).await?;

                TcpConnection::establish(
                    stream,
                    self.local_server_id,
                    peer_id,
                )
                .await
            },
        )
        .await
        .unwrap_or_else(|_| {
            Err(TcpTransportError::ConnectionFailed(
                format!(
                    "connection attempt to {:?} timed out",
                    peer_id,
                ),
            ))
        })?;

        // start the registeration - this is a multi step process
        // we create channels and we spawn reader and writer task
        // reset the reconnect states
        self.register_connection(result).await
    }

    /// Registers an already-established connection.
    ///
    /// This is primarily used by the listener after accepting an
    /// inbound TCP connection and completing the handshake.
    ///
    /// The peer identity comes from the handshake. The manager
    /// verifies that the peer is part of the configured cluster
    /// before registering the connection.
    pub async fn register_connection(
        &mut self,
        connection: TcpConnection,
    ) -> Result<(), TcpTransportError> {

        if self.is_shutting_down() {
            return Err(TcpTransportError::TransportShuttingDown);
        }

        let peer_id = connection.peer_server_id();

        // I can't make a connection to myself, reject all such
        // registrations.
        if peer_id == self.local_server_id {
            return Err(TcpTransportError::UnknownPeer(
                "connection points to the local server".to_string(),
            ));
        }

        // This peer must be known to me. If I don't know it,
        // reject the connection.
        if !self.peers.contains_key(&peer_id) {
            return Err(TcpTransportError::UnknownPeer(
                format!("peer {:?} is not configured", peer_id),
            ));
        }

        // Never replace an existing connection implicitly.
        // Replacing a healthy connection can interrupt message delivery.
        if self.connections.contains_key(&peer_id) {
            return Err(TcpTransportError::ConnectionFailed(
                format!(
                    "connection to peer {:?} already exists",
                    peer_id,
                ),
            ));
        }

        // Get access to the reader and writer halves so that they
        // can work independently.
        let (
            _local_server_id,
            peer_id,
            read_half,
            write_half,
        ) = connection.split();

        // Decoupling the RAFT and transport
        // Create an async channel with a sender and receiver.
        // The sender enqueues outgoing RaftMessages. The writer
        // receives them, serializes them, and writes them to TCP.
        // This decouples message submission from network writes.
        //
        // If the bounded queue is full, send().await waits until
        // space becomes available, providing backpressure instead
        // of allowing an unlimited backlog.
        let (
            outgoing_message_sender,
            outgoing_message_receiver,
        ) = mpsc::channel(OUTGOING_CHANNEL_CAPACITY);

        // Assign a unique ID to this registered connection.
        // Reader/writer failures include this ID so stale events
        // cannot remove a replacement connection.
        let connection_id = self.next_connection_id;
        self.next_connection_id =
            self.next_connection_id.wrapping_add(1).max(1);

        // Clone the sender used to put incoming messages into the
        // central incoming-message queue.
        // These messages are consumed by the manager, we will hand over
        // this to the reader - purely for messages, event are on other
        // channel
        let incoming_message_sender =
            self.incoming_message_sender.clone();

        // Clone the connection event sender for the reader task.
        let reader_event_sender =
            self.connection_event_sender.clone();

        let reader_shutdown_rx = self.shutdown_rx.clone();
        // Give ownership of the TCP read half to the reader task.
        // The task also receives handles for incoming messages
        // and connection events.
        
        let reader_task = tokio::spawn(async move {
            Self::run_reader(
                peer_id,
                connection_id,
                read_half,
                incoming_message_sender,
                reader_event_sender,
                reader_shutdown_rx,
            )
            .await;
        });

        // Clone the connection event sender for the writer task.
        let writer_event_sender =
            self.connection_event_sender.clone();

        let writer_shutdown_rx = self.shutdown_rx.clone();

        // The writer task owns the write half of the TCP stream.
        // It consumes messages from the outgoing channel.
        let writer_task = tokio::spawn(async move {
            Self::run_writer(
                peer_id,
                connection_id,
                write_half,
                outgoing_message_receiver,
                writer_event_sender,
                writer_shutdown_rx,
            )
            .await;
        });

        // Keep handles to the peer connection and its worker tasks.
        let peer_connection = PeerConnection {
            connection_id,
            outgoing_message_sender,
            reader_task,
            writer_task,
        };

        self.connections.insert(peer_id, peer_connection);
        // Reset the reconnect state
        if let Some(state) = self.reconnect_state.get_mut(&peer_id) {
            state.reset(Instant::now());
        }

        Ok(())
    }

    /// Runs the reader task for one peer.
    ///
    /// The reader task owns the read half of the TCP connection and
    /// continuously waits for incoming Raft messages.
    ///
    /// Successfully decoded messages are sent to the central
    /// incoming channel.
    async fn run_reader(
        peer_id: ServerId,
        connection_id: u64,
        mut read_half: tokio::net::tcp::OwnedReadHalf,
        incoming_tx: mpsc::Sender<RaftMessage<C>>,
        event_tx: mpsc::Sender<ConnectionEvent>,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) {
        loop {
            if *shutdown_rx.borrow() {
                break;
            }

            let result = tokio::select! {
                biased;

                _ = shutdown_rx.changed() => break,
                result = TcpConnection::read_frame(&mut read_half) => result,
            };

            let message = match result {
                Ok(message) => message,
                Err(error) => {
                    if !*shutdown_rx.borrow() {
                        let _ = event_tx
                            .send(ConnectionEvent::Failed {
                                peer_id,
                                connection_id,
                                error,
                            })
                            .await;
                    }
                    break;
                }
            };

            let sent = tokio::select! {
                biased;

                _ = shutdown_rx.changed() => false,
                result = incoming_tx.send(message) => result.is_ok(),
            };

            if !sent {
                break;
            }
        }
    }
    /// Runs the writer task for one peer.
    ///
    /// The writer task owns the write half of the TCP connection and
    /// waits for messages from the peer-specific outgoing channel.
    async fn run_writer(
        peer_id: ServerId,
        connection_id: u64,
        mut write_half: tokio::net::tcp::OwnedWriteHalf,
        mut outgoing_rx: mpsc::Receiver<RaftMessage<C>>,
        event_tx: mpsc::Sender<ConnectionEvent>,
        mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ) {
        loop {
            if *shutdown_rx.borrow() {
                break;
            }

            let message = tokio::select! {
                biased;

                _ = shutdown_rx.changed() => break,
                message = outgoing_rx.recv() => match message {
                    Some(message) => message,
                    None => break,
                },
            };

            let result = tokio::select! {
                biased;

                _ = shutdown_rx.changed() => break,
                result = TcpConnection::write_frame(
                    &mut write_half,
                    message,
                ) => result,
            };

            if let Err(error) = result {
                if !*shutdown_rx.borrow() {
                    let _ = event_tx
                        .send(ConnectionEvent::Failed {
                            peer_id,
                            connection_id,
                            error,
                        })
                        .await;
                }
                break;
            }
        }
    }

    /// Sends a Raft message through the writer task associated with
    /// the destination peer.
    ///
    /// This method does not perform a network write itself.
    /// Instead, it places the message into the peer's bounded
    /// outgoing channel.
    ///
    /// The writer task owns the actual TCP write half.
    pub async fn send(
        &mut self,
        message: RaftMessage<C>,
    ) -> Result<(), TcpTransportError> {
        let peer_id = message.to;

        // Get the connection handler for message.to.
        let connection = self
            .connections
            .get(&peer_id)
            .ok_or_else(|| {
                TcpTransportError::ConnectionFailed(
                    format!("no connection to peer {:?}", peer_id),
                )
            })?;

        // We have two workers: reader and writer.
        // The outgoing channel decouples sending from writing.
        connection
            .outgoing_message_sender
            .send(message)
            .await
            .map_err(|_| TcpTransportError::ConnectionClosed)
    }

    /// Marks a connection as failed.
    ///
    /// The connection is removed and reconnect backoff is updated.
    /// The actual reconnect attempt happens when maintenance observes
    /// that the backoff has expired.
    fn connection_failed(
        &mut self,
        peer_id: ServerId,
        connection_id: u64,
    ) {
        // Check the connection ID again before removing anything.
        // This guards against stale failure events.
        let is_current = self
            .connections
            .get(&peer_id)
            .map(|connection| connection.connection_id == connection_id)
            .unwrap_or(false);

        if !is_current {
            return;
        }

        // The handler give us `abort` option 
        // On connection failure, abort the two workers:
        // the reader and the writer.
        if let Some(connection) = self.connections.remove(&peer_id) {
            connection.reader_task.abort();
            connection.writer_task.abort();
        }

        // Start reconnect backoff.
        if let Some(state) = self.reconnect_state.get_mut(&peer_id) {
            state.record_failure(Instant::now());
        }
    }

    /// Removes a peer connection without scheduling a reconnect.
    ///
    /// This is useful when the caller intentionally removes a peer
    /// or is shutting down the node.
    pub fn remove(&mut self, peer_id: ServerId) {
        if let Some(connection) = self.connections.remove(&peer_id) {
            connection.reader_task.abort();
            connection.writer_task.abort();
        }
    }

    /// Shuts down and removes a peer connection.
    ///
    /// The reader and writer tasks are stopped. The TCP stream is
    /// closed when the owned halves are dropped by those tasks.
    pub async fn shutdown(&mut self) {
        // Signal all shutdown-aware tasks to stop.
        let _ = self.shutdown_tx.send(true);

        // Cancel and await outstanding connection attempts.
        let attempts = 
                std::mem::take(&mut self.connection_attempt_tasks);
        for (_, mut handle) in attempts {
            if tokio::time::timeout(
                SHUTDOWN_TIMEOUT,
                &mut handle,
            )
            .await
            .is_err()
            {
                handle.abort();
                let _ = handle.await;
            }
        }

        self.in_flight_attempts.clear();

        // Remove connections and wait for their workers to stop.
        let connections = 
                std::mem::take(&mut self.connections);
        for (_, connection) in connections {
            drop(connection.outgoing_message_sender);

            let mut reader = connection.reader_task;
            let mut writer = connection.writer_task;

            if tokio::time::timeout(
                SHUTDOWN_TIMEOUT,
                &mut reader,
            )
            .await
            .is_err()
            {
                reader.abort();
                let _ = reader.await;
            }

            if tokio::time::timeout(
                SHUTDOWN_TIMEOUT,
                &mut writer,
            )
            .await
            .is_err()
            {
                writer.abort();
                let _ = writer.await;
            }
        }
    }

    /// Returns a reference to the outgoing channel for a peer.
    ///
    /// The manager normally sends through `send()`. This method is
    /// kept for cases where the transport needs direct access to the
    /// peer's writer channel.
    pub fn writer(
        &self,
        peer_id: ServerId,
    ) -> Option<&mpsc::Sender<RaftMessage<C>>> {
        self.connections
            .get(&peer_id)
            .map(|connection| &connection.outgoing_message_sender)
    }

    fn is_shutting_down(&self) -> bool {
        *self.shutdown_rx.borrow()
    }

}
