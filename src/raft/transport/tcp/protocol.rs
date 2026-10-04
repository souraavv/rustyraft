//! TCP wire protocol
//! 
//! This module define the protocol exchanged between
//! RustRaft node over a TCP connection 
//! 

use crate::raft::state::ServerId;

use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u16 = 1;
pub const MAX_SUPPORTED_PROTOCOL_VERSION: u16 = PROTOCOL_VERSION;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Handshake {
    pub protocol_version: u16, 
    pub server_id: ServerId,
}

impl Handshake {
    /// Creates a handshake for the current protocol version.
    pub fn new(server_id: ServerId) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            server_id,
        }
    }

    /// Returns whether this handshake uses a supported protocol
    /// version.
    pub fn is_supported(&self) -> bool {
        self.protocol_version <= MAX_SUPPORTED_PROTOCOL_VERSION
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum MessageType {
    Handshake,
    RequestVote,
    RequestVoteResponse,
    AppendEntries,
    AppendEntriesResponse,
    InstallSnapshot,
}



