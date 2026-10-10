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

use std::collections::HashMap;
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

/// Maximum number of messages waiting to be sent to one peer.
const OUTGOING_CHANNEL_CAPACITY: usize = 256;

/// Maximum number of incoming Raft messages waiting to be
/// processed by the runtime.
const INCOMING_CHANNEL_CAPACITY: usize = 256;

/// Maximum number of connection failure events waiting for the
/// connection manager to process.
const CONNECTION_EVENT_CAPACITY: usize = 64;

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
        self.attempts =
            self.attempts.saturating_add(1);

        // Convert the attempt count into a zero-based exponent.
        let attempt =
            self.attempts.saturating_sub(1);

        // Calculate the exponential multiplier.
        // The exponent is capped to avoid an unnecessarily large value.
        let multiplier =
            2u32.pow(attempt.min(10));

        // Calculate the reconnect delay.
        let mut delay =
            INITIAL_RECONNECT_DELAY * multiplier;

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

/// Event sent by a reader or writer task when a connection fails.
#[derive(Debug)]
enum ConnectionEvent {
    /// The connection to the given peer has failed.
    Failed(ServerId),
}

/// State associated with one active peer connection.
///
/// The TCP stream itself is owned by the reader and writer tasks.
/// The manager only keeps the channel used to send messages to the
/// writer and the task handles used to manage their lifecycle.
struct PeerConnection<C> {
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
    connections:
        HashMap<ServerId, PeerConnection<C>>,

    /// Reconnect state for peers whose connections are unavailable.
    reconnect_state:
        HashMap<ServerId, ReconnectState>,

    /// Sends decoded Raft messages from reader tasks to the
    /// transport.
    incoming_message_sender:
        mpsc::Sender<RaftMessage<C>>,

    /// Receives connection failure events from reader and writer
    /// tasks.
    connection_event_receiver:
        mpsc::Receiver<ConnectionEvent>,

    /// Sender used by reader and writer tasks to report failures.
    connection_event_sender:
        mpsc::Sender<ConnectionEvent>,
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
    /// Send trait is required  - it is safe to transfer ownership
    /// of this value to another thread - required because tokio
    /// may execute spawn task on a separate worker thread
    /// 
    /// 'static  - means that this type does not contains
    /// references that are requried valie from short lifetime
    /// We need both serialize and desrialize bcz we want to 
    /// encode outgoing message and decode incoming message
    pub fn new(
        local_server_id: ServerId,
        peers: Vec<PeerAddress>,
    ) -> (
        Self,
        mpsc::Receiver<RaftMessage<C>>,
    ) {
        let mut peer_addresses =
            HashMap::new();

        let mut reconnect_state =
            HashMap::new();

        let now = Instant::now();

        for peer in peers {
            if peer.server_id == local_server_id {
                continue;
            }

            peer_addresses.insert(
                peer.server_id,
                peer.address,
            );

            reconnect_state.insert(
                peer.server_id,
                ReconnectState::new(now),
            );
        }

        // Create bounded channels, The mpsc gives us back
        // transmitter (tx) and reciver (rx)
        let (
            incoming_message_sender,
            incoming_rx,
        ) = mpsc::channel(
            INCOMING_CHANNEL_CAPACITY,
        );

        let (
            connection_event_sender,
            connection_event_receiver,
        ) = mpsc::channel(
            CONNECTION_EVENT_CAPACITY,
        );

        let manager = Self {
            local_server_id,
            peers: peer_addresses,
            connections: HashMap::new(),
            reconnect_state,
            incoming_message_sender,
            connection_event_receiver,
            connection_event_sender,
        };

        (
            manager,
            incoming_rx,
        )
    }

    /// Returns the local Raft server identity.
    pub fn local_server_id(&self) -> ServerId {
        self.local_server_id
    }

    /// Returns whether a connection to the given peer exists.
    pub fn contains(
        &self,
        peer_id: ServerId,
    ) -> bool {
        self.connections
            .contains_key(&peer_id)
    }

    /// Returns the number of currently active connections.
    pub fn len(&self) -> usize {
        self.connections.len()
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
    /// This is about the health of the peer connections
    /// 
    /// The connection reader/writer tasks run indepedently 
    /// in the background; when one of them finishes or fails, 
    /// they send an event such as ConnectionClosed or
    /// ConnectionFailed into the connection_event_receiver 
    /// 
    /// This method periodicially comes alongs and first drain
    /// those events in the proces connection events 
    /// - so that manager can remove dead connections 
    /// - mark the peers as disconnected
    /// 
    /// So in short - 
    ///  Background connection task detect failures -> send events
    ///  -> maintain_connection() picks up those events and 
    ///     update the state and reconnect logic runs
    ///
    /// This method performs any reconnect attempts whose backoff
    /// timers have expired.
    ///
    /// It deliberately does not sleep. The caller controls how often
    /// this method is invoked.
    pub async fn maintain_connections(
        &mut self,
        now: Instant,
    ) {
        self.process_connection_events();

        let peers: Vec<ServerId> =
            self.peers.keys().copied().collect();

        for peer_id in peers {
            if !self.owns_outbound_connection(peer_id) {
                continue;
            }

            if self.connections.contains_key(&peer_id) {
                continue;
            }

            let should_attempt = self
                .reconnect_state
                .get(&peer_id)
                .map(|state| state.ready(now))
                .unwrap_or(false);

            if !should_attempt {
                continue;
            }

            self.try_connect(
                peer_id,
                now,
            )
            .await;
        }
    }

    /// Processes connection failure events reported by reader and
    /// writer tasks.
    ///
    /// The tasks themselves do not modify the connection manager.
    /// They only report that their connection has failed.
    fn process_connection_events(&mut self) {
        // try_recv : try to recieve a value from the rx (reciver)
        // without waiting
        while let Ok(event) =
            self.connection_event_receiver.try_recv()
        {
            match event {
                ConnectionEvent::Failed(peer_id) => {
                    self.connection_failed(
                        peer_id,
                    );
                }
            }
        }
    }

    /// Attempts to establish one outbound connection.
    ///
    /// A failed attempt updates reconnect backoff but does not
    /// propagate the connection failure into the Raft protocol.
    async fn try_connect(
        &mut self,
        peer_id: ServerId,
        now: Instant,
    ) {
        let address =
            match self.peers.get(&peer_id) {
                Some(address) => *address,
                None => return,
            };

        let result = self
            .connect(
                peer_id,
                address,
            )
            .await;

        if result.is_ok() {
            // The connection succeeded, so reset the reconnect
            // state back to zero attempts.
            if let Some(state) =
                self.reconnect_state
                    .get_mut(&peer_id)
            {
                state.reset(now);
            }

            return;
        }

        // The connection attempt failed, so record the failure.
        if let Some(state) =
            self.reconnect_state
                .get_mut(&peer_id)
        {
            state.record_failure(now);
        }
    }

    /// Creates an outbound connection to a peer immediately.
    ///
    /// This method is used by the maintenance logic. It may also be
    /// useful during initial startup when the caller wants to
    /// establish connections eagerly.
    pub async fn connect(
        &mut self,
        peer_id: ServerId,
        address: SocketAddr,
    ) -> Result<(), TcpTransportError> {
        if peer_id == self.local_server_id {
            return Err(
                TcpTransportError::UnknownPeer(
                    "cannot connect to the local server"
                        .to_string(),
                ),
            );
        }

        if !self.peers.contains_key(&peer_id) {
            return Err(
                TcpTransportError::UnknownPeer(
                    format!(
                        "peer {:?} is not configured",
                        peer_id,
                    ),
                ),
            );
        }

        if !self.owns_outbound_connection(peer_id) {
            return Err(
                TcpTransportError::UnknownPeer(
                    format!(
                        "server {:?} does not own the outbound \
                         connection to peer {:?}",
                        self.local_server_id,
                        peer_id,
                    ),
                ),
            );
        }

        if self.connections.contains_key(&peer_id) {
            return Ok(());
        }

        let stream =
            tokio::net::TcpStream::connect(
                address,
            )
            .await?;

        // connection manager pass the ownership of the stream down to the
        // connection.rs (TcpConnection) - thus we have mut TcpSTream
        let connection =
            TcpConnection::establish(
                stream,
                self.local_server_id,
                peer_id,
            )
            .await?;

        // 
        self.register_connection(
            connection,
        )
        .await?;

        Ok(())
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
        let peer_id =
            connection.peer_server_id();

        // I can't make a connection to myself, reject all just registrations
        if peer_id == self.local_server_id {
            return Err(
                TcpTransportError::UnknownPeer(
                    "connection points to the local server"
                        .to_string(),
                ),
            );
        }

        // This peer of connection must be a known peer to me, if i don't
        // know then reject the connection
        if !self.peers.contains_key(&peer_id) {
            return Err(
                TcpTransportError::UnknownPeer(
                    format!(
                        "peer {:?} is not configured",
                        peer_id,
                    ),
                ),
            );
        }

        // Replace an existing connection to the same peer.
        //
        // This can happen when a new inbound connection arrives
        // while an older connection is still registered.
        if self.connections.contains_key(&peer_id) {
            self.remove(peer_id);
        }

        // Get the access to the reader and writer stream - so that we can
        // work indepedently on these two halfs
        let (
            _local_server_id,
            peer_id,
            read_half,
            write_half,
        ) = connection.split();

        // Create a async channel (bounded) with two ends
        // outgoing_message_sender (sender) and outgoing_message_receiver (receiver)
        // The reason we have channel is bcz we want writing and sending
        // task to be taken by two different entity
        // outgoing_message_sender enqueue the outgoing RaftMessages
        // whlie outgoing_message_receiver take that message -> serialize this -> send to the
        // tcpConnection. This decoupled both the manager and the writer
        // if queue is full, an async send using .send().await wait until
        // space become available, providing backpressure instead of unlimited
        // backlog
        let (
            outgoing_message_sender,
            outgoing_message_receiver,
        ) = mpsc::channel(
            OUTGOING_CHANNEL_CAPACITY,
        );

        // We are creating more clones of senders
        // These are the sender which sends messages into the incoming 
        // message queue 
        let incoming_message_sender =
            self.incoming_message_sender.clone();

        // We are creating a clone of the connection event sender as well
        // we will pass the incoming message sender and connection event
        // sender to the asyn task which is reader task.

        let connection_event_sender =
            self.connection_event_sender.clone();

        // We are giving handle to the reader half of the tcp stream to this 
        // reader task, this is the stream that connection.rs replied to us
        // and we have created extra handles for the incoming messages and
        // connection event sender ... remember these are sender to the messgage
        // queue
        let reader_task =
            tokio::spawn(async move {
                Self::run_reader(
                    peer_id,
                    read_half,
                    incoming_message_sender,
                    connection_event_sender,
                )
                .await;
            });

        // Again one more clone of sender (mpsc) connection event 
        // we will pass this handle to the writer 
        let connection_event_sender =
            self.connection_event_sender.clone();
        
        // writer task - consumes the write half of the tcp stream
        let writer_task =
            // note that we are sending ownership of receiver and sender here
            tokio::spawn(async move {
                Self::run_writer(
                    peer_id,
                    write_half,
                    outgoing_message_receiver,
                    connection_event_sender,
                )
                .await;
            });

        // keep handle to the peer connection
        let peer_connection =
            PeerConnection {
                outgoing_message_sender,
                reader_task,
                writer_task,
            };

        self.connections.insert(
            peer_id,
            peer_connection,
        );

        if let Some(state) =
            self.reconnect_state
                .get_mut(&peer_id)
        {
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
        mut read_half:
            tokio::net::tcp::OwnedReadHalf,
        incoming_message_sender:
            mpsc::Sender<RaftMessage<C>>,
        connection_event_sender:
            mpsc::Sender<ConnectionEvent>,
    ) {
        // starts an infinite loop
        loop {
            let result =
                TcpConnection::read_frame(
                    &mut read_half,
                )
                .await;

            match result {
                Ok(message) => {
                    // try to send the message to the incoming message
                    // queue and wait until it is sent. if sending fails
                    // then we will stop the reader loop
                    // .await is required because we have a queue .. a bounded
                    // queue 
                    if incoming_message_sender
                        .send(message)
                        .await
                        .is_err()
                    {
                        // The transport is no longer receiving
                        // messages, so there is no reason to keep
                        // the reader task alive.
                        // 
                        break;
                    }
                }

                Err(_) => {
                    let _ =
                        connection_event_sender
                            .send(
                                ConnectionEvent::Failed(
                                    peer_id,
                                ),
                            )
                            .await;

                    break;
                }
            }
        }
    }

    /// Runs the writer task for one peer.
    ///
    /// The writer task owns the write half of the TCP connection and
    /// waits for messages from the peer-specific outgoing channel.
    async fn run_writer(
        peer_id: ServerId,
        mut write_half:
            tokio::net::tcp::OwnedWriteHalf,
        mut outgoing_message_receiver:
            mpsc::Receiver<RaftMessage<C>>,
        connection_event_sender:
            mpsc::Sender<ConnectionEvent>,
    ) {
        while let Some(message) =
            outgoing_message_receiver.recv().await
        {
            if TcpConnection::write_frame(
                &mut write_half,
                message,
            )
            .await
            .is_err()
            {
                let _ =
                    connection_event_sender
                        .send(
                            ConnectionEvent::Failed(
                                peer_id,
                            ),
                        )
                        .await;

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
        let peer_id =
            message.to;

        let connection =
            self.connections
                .get(&peer_id)
                .ok_or_else(|| {
                    TcpTransportError::ConnectionFailed(
                        format!(
                            "no connection to peer {:?}",
                            peer_id,
                        ),
                    )
                })?;

        connection
            .outgoing_message_sender
            .send(message)
            .await
            .map_err(|_| {
                TcpTransportError::ConnectionClosed
            })
    }

    /// Marks a connection as failed.
    ///
    /// The connection is removed immediately and the reconnect
    /// state is updated. The actual reconnect attempt happens when
    /// `maintain_connections()` observes that the backoff has
    /// expired.
    fn connection_failed(
        &mut self,
        peer_id: ServerId,
    ) {
        if let Some(connection) =
            self.connections.remove(&peer_id)
        {
            connection.reader_task.abort();
            connection.writer_task.abort();
        }

        let now =
            Instant::now();

        if let Some(state) =
            self.reconnect_state
                .get_mut(&peer_id)
        {
            state.record_failure(now);
        }
    }

    /// Removes a peer connection without scheduling a reconnect.
    ///
    /// This is useful when the caller intentionally removes a peer
    /// or is shutting down the node.
    pub fn remove(
        &mut self,
        peer_id: ServerId,
    ) {
        if let Some(connection) =
            self.connections.remove(&peer_id)
        {
            connection.reader_task.abort();
            connection.writer_task.abort();
        }
    }

    /// Shuts down and removes a peer connection.
    ///
    /// The reader and writer tasks are stopped. The TCP stream is
    /// closed when the owned halves are dropped by those tasks.
    pub fn shutdown(
        &mut self,
        peer_id: ServerId,
    ) {
        self.remove(peer_id);
    }

    /// Returns a reference to the outgoing channel for a peer.
    ///
    /// The manager normally sends through `send()`. This method is
    /// kept for cases where the transport needs direct access to the
    /// peer's writer channel.
    pub fn writer(
        &self,
        peer_id: ServerId,
    ) -> Option<
        &mpsc::Sender<RaftMessage<C>>
    > {
        self.connections
            .get(&peer_id)
            .map(|connection|
                &connection.outgoing_message_sender
            )
    }
}