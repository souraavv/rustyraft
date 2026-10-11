//! TCP listener for incoming Raft peer connections.
//!
//! The listener is responsible only for accepting incoming TCP
//! connections.
//!
//! It does not:
//! - read Raft messages,
//! - process Raft protocol messages,
//! - make Raft decisions,
//! - manage reconnects.
//!
//! Once a TCP connection is accepted, the connection performs the
//! handshake and is then registered with `TcpConnectionManager`.

use std::net::SocketAddr;

use tokio::net::{
    TcpListener,
    TcpStream,
};

use crate::raft::state::ServerId;

use super::connection::TcpConnection;
use super::connection_manager::TcpConnectionManager;
use super::error::TcpTransportError;

pub struct TcpListenerService {
    /// Identity of the local Raft server.
    local_server_id: ServerId,

    /// The TCP listener.
    listener: TcpListener,
}

impl TcpListenerService {
    /// Creates a TCP listener bound to the given address.
    pub async fn bind(
        local_server_id: ServerId,
        address: SocketAddr,
    ) -> Result<Self, TcpTransportError> {
        let listener =
            TcpListener::bind(address).await?;

        Ok(Self {
            local_server_id,
            listener,
        })
    }

    /// Returns the address on which this listener is bound.
    pub fn local_addr(
        &self,
    ) -> Result<SocketAddr, TcpTransportError> {
        Ok(self.listener.local_addr()?)
    }

    /// Accepts one incoming TCP connection without performing
    /// the handshake.
    ///
    /// This method separates accepting the socket from establishing
    /// the Raft connection. The transport driver can select on this
    /// method without risking cancellation of an in-progress handshake.
    ///
    /// After accepting the socket, the caller is responsible for
    /// completing the handshake and registering the connection.
    pub async fn accept_stream(
        &self,
    ) -> Result<
        (TcpStream, SocketAddr),
        TcpTransportError,
    > {
        Ok(self.listener.accept().await?)
    }

    /// Accepts one incoming TCP connection and registers it with
    /// the connection manager.
    ///
    /// The peer must complete the TCP handshake successfully before
    /// the connection is registered.
    ///
    /// Note: this method performs the handshake inline. If it is used
    /// inside a tokio::select! branch, that branch can be cancelled
    /// while the handshake is in progress. For the background transport
    /// driver, prefer accept_stream() and complete the handshake in
    /// a separate task.
    pub async fn accept<C>(
        &self,
        connection_manager: &mut TcpConnectionManager<C>,
    ) -> Result<(), TcpTransportError>
    where
        C: serde::Serialize
            + serde::de::DeserializeOwned
            + Send
            + 'static,
    {
        // Accept the socket from the remote peer.
        let (stream, _address) =
            self.accept_stream().await?;

        // Complete the handshake before registering the connection.
        let connection =
            TcpConnection::accept(
                stream,
                self.local_server_id,
            )
            .await?;

        // The manager owns the lifecycle of established connections,
        // including the reader and writer tasks.
        connection_manager
            .register_connection(connection)
            .await?;

        Ok(())
    }

    /// Returns the identity of the local Raft server.
    pub fn local_server_id(
        &self,
    ) -> ServerId {
        self.local_server_id
    }
}