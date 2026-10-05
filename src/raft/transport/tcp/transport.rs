//! TCP transport implementation.
//!
//! `TcpTransport` is the transport-facing facade for the real
//! network transport.
//!
//! It owns:
//! - the TCP listener,
//! - the connection manager,
//! - the incoming Raft message queue.
//!
//! It does not contain Raft protocol decisions.

use std::net::SocketAddr;
use std::time::Instant;

use tokio::time::{
    self, 
    Duration,
};

use crate::raft::{ServerId, transport::{RaftMessage, Transport, tcp::{TcpTransportError, connection_manager::{self, PeerAddress, TcpConnectionManager}, listener::TcpListenerService}}};


/// How often the TCP transport checks connection health and
/// performs reconnect attempts.
const CONNECTION_MAINTENANCE_INTERVAL: Duration =
    Duration::from_millis(100);

pub struct TcpTransport<C> {
    local_server_id: ServerId,

    listener: TcpListenerService,

    connection_manager: TcpConnectionManager<C>,

    incoming_rx: 
        tokio::sync::mpsc::Receiver<RaftMessage<C>>,
}

impl<C> TcpTransport<C>
where 
    C: serde::Serialize
        + serde::de::DeserializeOwned
        + Send
        + 'static, 
{

    pub async fn send_message(
        &mut self, 
        message: RaftMessage<C>,
    ) -> Result<(), TcpTransportError> {
        self.connection_manager
            .send(message)
            .await
    }

    pub async fn receive_message(
        &mut self, 
    ) -> Option<RaftMessage<C>> {
        let mut maintenance =
            time::interval(
                CONNECTION_MAINTENANCE_INTERVAL
            );

        loop {
            tokio::select! {
                message = 
                    self.incoming_rx.recv() => 
                {
                    return message;
                }

                result =
                    self.listener.accept(
                        &mut self.connection_manager,
                    ) => 
                {
                    if let Err(error) = result {
                        tracing::warn!(
                            ?error,
                            "Failed to accept incoming Raft connection"
                        );
                    }
                }

                _ = maintenance.tick() =>
                {
                    self.connection_manager
                        .maintain_connections(
                            Instant::now(),
                        )
                        .await;
                }
            }
        }
    }

    pub async fn accept(
        &mut self,
    ) -> Result<(), TcpTransportError> {
        self.listener
            .accept(
                &mut self.connection_manager,
            )
            .await
    }

    pub async fn bind(
        local_server_id: ServerId,
        listener_address: SocketAddr,
        peers: Vec<PeerAddress>,
    ) -> Result<Self, TcpTransportError> {

        // S1. Get the listener and bind it with the expected listener_address
        let listener = 
            TcpListenerService::bind(
                local_server_id,
                listener_address,
            )
            .await?;

        // S2. Get a connection manager and handle to the incoming receiver
        let (
            connection_manager,
            incoming_rx,
        ) = TcpConnectionManager::new(
            local_server_id,
            peers,
        );

        Ok(Self {
            local_server_id,
            listener,
            connection_manager, 
            incoming_rx,
        })
    }

    /// Maintains outbound peer connections.
    ///
    /// This drains connection failure events and performs
    /// reconnect attempts whose backoff timers have expired.
    pub async fn maintain_connections(
        &mut self,
    ) {
        self.connection_manager
            .maintain_connections(
                Instant::now(),
            )
            .await;
    }

    // ----------------------------------
    // ---------- helpers ---------------
    // ----------------------------------

    pub fn local_server_id(
        &self,
    ) -> ServerId {
        self.local_server_id
    }

    /// Returns the address on which this transport is listening
    pub fn local_addr(
        &self,
    ) -> Result<SocketAddr, TcpTransportError> {
        self.listener.local_addr()
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

impl<C> Transport<C> for TcpTransport<C> 
where 
    C: serde::Serialize
        + serde::de::DeserializeOwned
        + Send
        + 'static,
{

    async fn send(
        &mut self, 
        message: RaftMessage<C>,
    ) -> Result<(), TcpTransportError>
    {
        self.send_message(message)
            .await
    }

    async fn receive(
        &mut self,
    ) -> Option<RaftMessage<C>> {
        self.receive_message()
            .await
    }
}