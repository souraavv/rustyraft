//! Raft RPC definitions.
//!
//! This module contains the messages exchanged between Raft servers.
//!
//! Transport is deliberately kept outside this module. RPC messages
//! should be usable with:
//!   - real network transport
//!   - in-memory transport
//!   - deterministic test transport
//!   - fault-injected transport

pub mod append_entries;
pub mod request_vote;

pub use append_entries::{
    AppendEntriesRequest, 
    AppendEntriesResponse,
};

pub use request_vote::{
    RequestVoteRequest, 
    RequestVoteResponse,
};
