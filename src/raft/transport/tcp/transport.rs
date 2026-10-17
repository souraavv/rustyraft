
 //! TCP transport implementation.

use std::net::SocketAddr;
use std::time::Instant;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};

use crate::raft::{
    ServerId,
    transport::{
        RaftMessage,
        Transport,
        tcp::{
            TcpTransportError,
            connection::TcpConnection,
            connection_manager::{
                PeerAddress,
                TcpConnectionManager,
            },
            listener::TcpListenerService,
        },
    },
};

const CONNECTION_MAINTENANCE_INTERVAL: Duration =
    Duration::from_millis(100);

const ESTABLISHED_CONNECTION_CAPACITY: usize = 64;

pub struct TcpTransport<C> {
    /// Identity of the local Raft server.
    local_server_id: ServerId,

    /// Address on which this transport listens.
    listener_address: SocketAddr,

    /// Owns active connections and reader/writer tasks.
    connection_manager: TcpConnectionManager<C>,

    /// Raft messages received from connected peers.
    incoming_rx: mpsc::Receiver<RaftMessage<C>>,

    /// Established inbound connections awaiting registration.
    established_rx: mpsc::Receiver<TcpConnection>,

    /// Background task accepting inbound TCP connections.
    listener_task: JoinHandle<()>,
}

impl<C> TcpTransport<C>
where
    C: serde::Serialize
        + serde::de::DeserializeOwned
        + Send
        + 'static,
{
    /// Sends a Raft message through the connection manager.
    pub async fn send_message(
        &mut self,
        message: RaftMessage<C>,
    ) -> Result<(), TcpTransportError> {
        self.connection_manager.send(message).await
    }

    /// Receives messages while registering inbound connections
    /// and maintaining outbound peer connections.
    ///
    /// The name says receive message.
    /// Message type can be different - these are events for which
    /// we have to respond.
    pub async fn receive_message(
        &mut self,
    ) -> Option<RaftMessage<C>> {
        let mut maintenance =
            time::interval(CONNECTION_MAINTENANCE_INTERVAL);

        let mut established_open = true;

        loop {
            // We are using select because we have multiple things
            // to address but we don't want to block on any. So
            // tokio::select! waits for whichever branch is ready.
            // This is why it is inside a loop.
            tokio::select! {
                // On the incoming message receiver channel we are
                // waiting for the message (recv).

                // Branch 1 - Got a new message?
                message = self.incoming_rx.recv() => {
                    return message;
                }

                // Branch 2 - Got a new connection?
                connection = self.established_rx.recv(),
                    if established_open =>
                {
                    // The reason for established_open is that we
                    // don't want to poll this branch once the
                    // channel is closed.
                    //
                    // If the condition is false, select! disables
                    // this branch.

                    match connection {
                        // Got some connection.
                        Some(connection) => {
                            // Ask the connection manager to register it.
                            //
                            // Registration validates the peer and starts
                            // reader/writer tasks. It does not await the
                            // TCP connection attempt for another peer.
                            if let Err(error) = self
                                .connection_manager
                                .register_connection(connection)
                                .await
                            {
                                tracing::warn!(
                                    ?error,
                                    "Failed to register inbound connection"
                                );
                            }
                        }
                        None => {
                            established_open = false;

                            tracing::warn!(
                                "Inbound connection channel closed"
                            );
                        }
                    }
                }

                // Branch 3 - Maintenance tick.
                //
                // This produces a tick every
                // CONNECTION_MAINTENANCE_INTERVAL.
                //
                // Outbound connection attempts run in background tasks,
                // so maintenance does not wait for TCP handshakes.
                _ = maintenance.tick() => {
                    self.connection_manager
                        .maintain_connections(Instant::now())
                        .await;
                }
            }
        }
    }

    /// Registers the next established inbound connection.
    ///
    /// Use this method when managing inbound registration explicitly.
    /// Do not call it concurrently with receive_message(), because
    /// both methods consume established_rx.
    pub async fn accept(
        &mut self,
    ) -> Result<(), TcpTransportError> {
        // This is from the listener. The listener sends events
        // through established_sender, and here we receive them.
        let connection = self
            .established_rx
            .recv()
            .await
            .ok_or(TcpTransportError::ConnectionClosed)?;

        self.connection_manager
            .register_connection(connection)
            .await
    }

    /// Creates the TCP transport and starts its listener task.
    /// The main method for this returns the TcpTransport.
    pub async fn bind(
        local_server_id: ServerId,
        listener_address: SocketAddr,
        peers: Vec<PeerAddress>,
    ) -> Result<Self, TcpTransportError> {
        let listener = TcpListenerService::bind(
            local_server_id,
            listener_address,
        )
        .await?;

        let listener_address = listener.local_addr()?;

        // The listener publishes established connections through
        // this bounded channel.
        //
        // The listener sends over this channel, and the transport
        // receives the established connections from it.
        let (established_sender, established_rx) =
            mpsc::channel(ESTABLISHED_CONNECTION_CAPACITY);

        // Spawn the listener task and give it the sender handle.
        let listener_task = listener.spawn(established_sender);

        // The connection manager owns established connections
        // and the incoming Raft message queue.
        let (connection_manager, incoming_rx) =
            TcpConnectionManager::new(local_server_id, peers);

        Ok(Self {
            local_server_id,
            listener_address,
            connection_manager,
            incoming_rx,
            established_rx,
            listener_task,
        })
    }

    /// Maintains outbound peer connections.
    ///
    /// Connection attempts are scheduled asynchronously by the manager.
    pub async fn maintain_connections(&mut self) {
        self.connection_manager
            .maintain_connections(Instant::now())
            .await;
    }

    // ----------------------------------
    // ---------- helpers ---------------
    // ----------------------------------

    pub fn local_server_id(&self) -> ServerId {
        self.local_server_id
    }

    pub fn local_addr(
        &self,
    ) -> Result<SocketAddr, TcpTransportError> {
        Ok(self.listener_address)
    }

    pub fn connection_manager(
        &self,
    ) -> &TcpConnectionManager<C> {
        &self.connection_manager
    }

    pub fn connection_manager_mut(
        &mut self,
    ) -> &mut TcpConnectionManager<C> {
        &mut self.connection_manager
    }
}

impl<C> Drop for TcpTransport<C> {
    fn drop(&mut self) {
        // Stop accepting new inbound TCP connections.
        self.listener_task.abort();
    }
}

impl<C> Transport<C> for TcpTransport<C>
where
    C: serde::Serialize
        + serde::de::DeserializeOwned
        + Send
        + 'static,
{
    type Error = TcpTransportError;

    async fn send(
        &mut self,
        message: RaftMessage<C>,
    ) -> Result<(), TcpTransportError> {
        self.send_message(message).await
    }

    async fn receive(
        &mut self,
    ) -> Option<RaftMessage<C>> {
        self.receive_message().await
    }
}
