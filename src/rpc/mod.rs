//! Raft RPC definitions
//! 
//! This module contains the message exchange b/w RAFT servers
//! 
//! Transport is deliberately kept outside this module.
//! RPC message should be usable with: 
//!     - real network transport, in-memory transport, determiistic transport
//!     - fault injected transport
//! 

pub mod request_vote;
pub mod append_entries;

pub use append_entries::{AppendEntriesResponse, AppendEntriesRequest};
pub use request_vote::{RequestVoteRequest, RequestVoteResponse};