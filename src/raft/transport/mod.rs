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

pub use in_memory::InMemoryTransport;

pub use message::{
    RaftMessage,
    RaftMessagePayload,
};

/// Sends messages b/w raft server
pub trait Transport<C>  {
    fn send(
        &mut self, 
        message: RaftMessage<C>,
    );

    fn receive(
        &mut self,
    ) -> Option<RaftMessage<C>>;
}