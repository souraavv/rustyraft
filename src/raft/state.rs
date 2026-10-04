use serde::{Deserialize, Serialize};

use crate::raft::replication::ReplicationState;

/// Raft Term number
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, 
    Default, Serialize, Deserialize)]
pub struct Term(u64);

impl Term {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u64 {
        self.0
    }

    pub fn next(self) -> Self {
        Self(self.0.checked_add(1).expect("Raft term exhausted"))
    }
}

/// Unique identity of the server
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    PartialOrd,
    Ord,
)]
pub struct ServerId(u64);

impl ServerId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Index of an entry in the RAFT log
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize)]
pub struct LogIndex(u64);

impl LogIndex {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u64 {
        self.0
    }

    pub fn next(self) -> Self {
        Self(self.0.checked_add(1).expect("Raft log index exhausted"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Role {
    #[default]
    Follower,
    Candidate,
    Leader,
}

/// Persistent state maintained by each RAFT server
///
/// This state must be written to stable storage before server responds
/// to an RPC or any other opr

#[derive(Debug)]
pub struct PersistentState<L> {
    pub current_term: Term,
    pub voted_for: Option<ServerId>,
    pub log: L,
}

impl<L> PersistentState<L> {
    pub fn new(
        current_term: Term, 
        voted_for: Option<ServerId>,
        log: L,
    ) -> Self {
        Self {
            current_term,
            voted_for,
            log,
        }
    }
}

/// Volatile state
#[derive(Debug)]
pub struct VolatileState {
    pub commit_index: LogIndex,
    pub last_apply_index: LogIndex,
}

impl VolatileState {
    pub fn new() -> Self {
        Self {
            commit_index: LogIndex::ZERO, 
            last_apply_index: LogIndex::ZERO,
        }
    }
}

impl Default for VolatileState {
    fn default() -> Self {
        Self::new()
    }
}


/// Volatile state maintaind when server is the leader
/// Re-init when server becomes leader
#[derive(Debug)]
pub struct LeaderState {
    pub replication: ReplicationState,
}

impl LeaderState {
    /// Creates leader state for a newly elected leader.
    ///
    /// ReplicationState initializes next_index and match_index for
    /// every follower.
    pub fn new(
        followers: &[ServerId],
        last_log_index: LogIndex,
    ) -> Self {
        Self {
            replication: ReplicationState::new(
                followers,
                last_log_index,
            ),
        }
    }
}