//! TCP connection manager.
//!
//! This module owns the lifecycle and management policy for TCP
//! connections between RustyRaft peers.
//! 
//! # Why is this separate from `TcpConnection`?
//!
//! There are two different responsibilities involved in networking:
//!
//! 1. Managing connections.
//! 2. Using an established connection.
//!
//! `TcpConnection` owns the mechanics of one established connection:
//!
//! - performing the handshake,
//! - reading and writing frames,
//! - encoding and decoding messages,
//! - enforcing frame-size limits,
//! - shutting down the connection.
//!
//! `TcpConnectionManager` owns the lifecycle and policy around those
//! connections:
//!
//! - deciding whether a connection should exist,
//! - creating outbound connections,
//! - registering inbound connections,
//! - detecting failed connections,
//! - scheduling reconnect attempts,
//! - applying reconnect backoff,
//! - removing dead connections.
//! 
//! This separation follows an important design principle:
//!
//! > Separate policy from mechanism.
//!
//! `TcpConnection` answers:
//!
//!     "How do I communicate over this TCP stream?"
//!
//! `TcpConnectionManager` answers:
//!
//!     "Which connection should exist and when should I maintain it?"


use std::collections::HashMap;
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

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

#[derive(Debug, Clone)]
struct ReconnectState {
    /// Number of consecutive failed connection attempts.
    attempts: u32, 

    /// Earliest time at which another connection attempt may occur.
    next_retry_at: Instant,
}

impl ReconnectState {

    fn new(now: Instant) -> Self {
        Self {
            attempts: 0,
            next_retry_at: now,
        }
    }

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
        let attempt = self.attempts.saturating_sub(1);

        // Calculate the exponential multiplier:
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

    fn reset(&mut self, now: Instant) {
        self.attempts = 0;
        self.next_retry_at = now;
    }
}

/// Configuration for a Raft peer.
/// address to reach on the TCP and serverId is the unique Id
#[derive(Debug, Clone, Copy)]
pub struct PeerAddress {
    pub server_id: ServerId,
    pub address: SocketAddr,
}

/// Maintains TCP connections to Raft peers.
pub struct TcpConnectionManager {
     /// Identity of this Raft node.
    local_server_id: ServerId, 

    /// Known peer addresses.
    peers: HashMap<ServerId, SocketAddr>,

    /// Currently active TCP connections.
    connections: HashMap<ServerId, TcpConnection>,

     /// Reconnect state for peers whose connections are unavailable.
    reconnect_state: HashMap<ServerId, ReconnectState>,
}

impl TcpConnectionManager {

    pub fn new(
        local_server_id: ServerId, 
        peers: Vec<PeerAddress>,
    ) -> Self {
        let mut peer_addresses = HashMap::new();
        let mut reconnect_state = HashMap::new();

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

        Self {
            local_server_id, 
            peers: peer_addresses,
            connections: HashMap::new(),
            reconnect_state,
        }
    }

    /// Maintains outbound connections.
    ///
    /// This method performs any reconnect attempts whose backoff
    /// timers have expired.
    ///
    /// It deliberately does not sleep. The caller controls how often
    /// this method is invoked.
    pub fn maintain_connections(
        &mut self, 
        now: Instant,
    ) {
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

            self.try_connect(peer_id, now);
        }
    }

    fn try_connect(
        &mut self, 
        peer_id: ServerId,
        now: Instant,
    ) {
        let address = match self.peers.get(&peer_id) {
            Some(address) => *address,
            None => return,
        };

        let result = self.connect(
            peer_id, 
            address,
        );

        if result.is_ok() {
            // we are successfully able to connect thus resetting 
            // reconnect state back to attemps = 0
            if let Some(state) 
                = self.reconnect_state.get_mut(&peer_id) 
            {
                state.reset(now);
            }

            return;
        }

        // If still failed to connect then record the failure and attempts
        if let Some(state) =
            self.reconnect_state.get_mut(&peer_id) 
        {
            state.record_failure(now);
        }
    }

    pub fn connect(
        &mut self, 
        peer_id: ServerId,
        address: SocketAddr,
    ) -> Result<(), TcpTransportError> {
        
        if peer_id == self.local_server_id {
            return Err(
                TcpTransportError::UnknownPeer(
                    "cannot connect to the local server".to_string()
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
            TcpStream::connect(address)?;

        let connection = 
            TcpConnection::establish(
                stream, 
                self.local_server_id, 
                peer_id,
            )?;
        
        self.register_connection(connection)?;

        Ok(())
    }

    pub fn register_connection(
        &mut self, 
        connection: TcpConnection,
    ) -> Result<(), TcpTransportError> {
        let peer_id = 
            connection.peer_server_id();

        if peer_id == self.local_server_id {
            return Err(
                TcpTransportError::UnknownPeer(
                    "connection points to the local server"
                        .to_string(),
                ),
            );
        }

        if let Some(mut old_connection) =
            self.connections.insert(
                peer_id, 
                connection,
            )
        {
            let _ = old_connection.shutdown();
        }

        if let Some(state) = 
            self.reconnect_state.get_mut(&peer_id)
        {
            state.reset(Instant::now());
        }
            
        Ok(())
    }

    pub fn send<C>(
        &mut self, 
        message: &RaftMessage<C>,
    ) -> Result<(), TcpTransportError>
    where 
        C: serde::Serialize,
    {
        let peer_id = message.to; 

        let connection = 
            self.connections
                .get_mut(&peer_id)
                .ok_or_else(|| {
                    TcpTransportError::ConnectionFailed(
                        format!(
                            "no connection to peer {:?}",
                            peer_id,
                        ),
                    )
                })?;

        match connection.send(message) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.connection_failed(
                    peer_id,
                );

                Err(error)
            }
        }
    }


    pub fn receive<C> (
        &mut self, 
        peer_id: ServerId,
    ) -> Result<RaftMessage<C>, TcpTransportError> 
    where   
        C: serde::de::DeserializeOwned,
    {

        let connection = 
            self.connections
                .get_mut(&peer_id)
                .ok_or_else(|| {
                    TcpTransportError::ConnectionFailed(
                        format!(
                            "no connection to peer {:?}",
                            peer_id,
                        ),
                    )
                })?;

        match connection.receive() {
            Ok(message) => Ok(message),

            Err(error) => {
                self.connection_failed(
                    peer_id,
                );

                Err(error)
            }
        }

    }

    fn connection_failed(
        &mut self, 
        peer_id: ServerId,
    ) {
        if let Some(mut connection) 
            = self.connections.remove(&peer_id) {
            let _ = connection.shutdown();
        }

        let now = Instant::now();

        if let Some(state) = 
            self.reconnect_state.get_mut(&peer_id) {
            state.record_failure(now);
        }

    }

    pub fn remove(
        &mut self, 
        peer_id: ServerId,
    ) -> Option<TcpConnection> {
        self.connections.remove(&peer_id)
    }

    pub fn shutdown(
        &mut self, 
        peer_id: ServerId,
    ) -> Result<(), TcpTransportError> {
        match self.connections.remove(&peer_id) {
            Some(mut connection) => {
                connection.shutdown()?;
                Ok(())
            }

            None => Ok(())
        }
    }
    
    pub fn get_mut(
        &mut self, 
        peer_id: ServerId,
    ) -> Option<&mut TcpConnection> {
        self.connections.get_mut(&peer_id)
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
        self.connections.contains_key(&peer_id)
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
    
}

