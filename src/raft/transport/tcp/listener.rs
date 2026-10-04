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

use std::net::{SocketAddr, TcpListener};

use crate::raft::state::ServerId;

use super::connection::TcpConnection;
use super::connection_manager::TcpConnectionManager;
use super::error::TcpTransportError;

pub struct TcpListenerService {
    local_server_id: ServerId,

    /// The TCP listener
    listener: TcpListener,
}

impl TcpListenerService {

    /// Creates a TCP listener bound to the given address.
    pub fn bind(
        local_server_id: ServerId,
        address: SocketAddr,
    ) -> Result<Self, TcpTransportError> {

        let listener = TcpListener::bind(address)?;

        Ok(Self {
            local_server_id,
            listener,
        })
    }

    pub fn local_addr(
        &self,
    ) -> Result<SocketAddr, TcpTransportError> {
        Ok(self.listener.local_addr()?)
    }

    /// Accepts one incoming TCP connection and registers it with
    /// the connection manager.
    ///
    /// The peer must complete the TCP handshake successfully before
    /// the connection is registered.
    pub fn accept(
        &self, 
        connection_manager: &mut TcpConnectionManager,
    ) -> Result<(), TcpTransportError> {

        let (stream, _address) = 
            self.listener.accept()?;


        let connection 
            = TcpConnection::accept(
                stream,
                self.local_server_id,
            )?;

        connection_manager.register_connection(connection)?;

        Ok(())

    }


}