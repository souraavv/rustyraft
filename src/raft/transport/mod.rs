//! Transport abstractions for Raft.
//!
//! The Raft node creates and consumes RPC messages.
//! Transport is responsible for moving those messages between nodes.
//!
//! Keeping transport outside the Raft node allows us to use:
//!   - deterministic in-memory transport
//!   - fault-injected transport
//!   - real network transport
//!
//! The transport layer should not contain Raft protocol decisions.

pub mod in_memory;
pub mod message;
pub mod tcp;

pub use in_memory::InMemoryTransport;
pub use message::{RaftMessage, RaftMessagePayload};
pub use tcp::TcpTransport;

use crate::raft::transport::tcp::TcpTransportError;

/// Sends messages b/w raft server
///
/// This is what Raft runtime all worry about.
///
/// Raft Runtime need not to bother if it is TCP based or in memory.
/// All it cares about is sending and receiving messages.
///
/// The interface is asynchronous because real network transports
/// may need to wait for:
/// - network I/O,
/// - connection availability,
/// - bounded-channel backpressure.
///
/// The in-memory transport can complete these operations immediately.
#[allow(async_fn_in_trait)]
pub trait Transport<C>  {
    type Error;
    /// Sends one Raft message
    async fn send(
        &mut self, 
        message: RaftMessage<C>,
    ) -> Result<(), TcpTransportError>;

    /// Receives the next Raft message
    /// 
    /// The real TCP transport waits asynchronously for a message
    /// the in-memory one returns immediately when a message is available
    async fn receive(
        &mut self,
    ) -> Option<RaftMessage<C>>;
}