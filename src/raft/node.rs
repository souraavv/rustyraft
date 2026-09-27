//! Raft Server node
//!
//! A Raft node owns the in-memory state of a single Raft server,
//!
//! by design we kept the network connections, timers, or durable storage
//! outside the node

use crate::raft::log::RaftLog;
use crate::raft::state::{LeaderState, PersistentState, Role, ServerId, Term, VolatileState};

/// A single Raft server.
///
/// The node contains the state required to participate in the Raft protocol
/// Protocols behaviors suchs as elections, log replications, and commitment
/// will be implemented in a separate mod
#[derive(Debug)]
pub struct RaftNode<C> {
    id: ServerId,
    role: Role,

    // Each Raft server has some persistent state and other volatile
    persistent: PersistentState<RaftLog<C>>,
    volatile: VolatileState,

    // leader specific volatile state - only applicable on leader, None for rest
    leader: Option<LeaderState>,
}

impl<C> RaftNode<C> {
    /// Create a new Raft Server
    ///
    /// A new server starts as followr with term = 0, no vote, an empty log
    /// commit index = 0, last applied index = 0
    pub fn new(id: ServerId) -> Self {
        Self {
            id,
            role: Role::Follower,

            persistent: PersistentState {
                current_term: Term::ZERO,
                voted_for: None,
                log: RaftLog::new(),
            },

            volatile: VolatileState {
                commit_index: crate::raft::state::LogIndex::ZERO,
                last_apply_index: crate::raft::state::LogIndex::ZERO,
            },

            leader: None,
        }
    }

    /// ---- Getters ----
    pub fn id(&self) -> ServerId {
        self.id
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn current_term(&self) -> Term {
        self.persistent.current_term
    }

    pub fn voted_for(&self) -> Option<ServerId> {
        self.persistent.voted_for
    }

    pub fn log(&self) -> &RaftLog<C> {
        &self.persistent.log
    }

    pub fn commit_index(&self) -> crate::raft::state::LogIndex {
        self.volatile.commit_index
    }

    pub fn last_applied(&self) -> crate::raft::state::LogIndex {
        self.volatile.last_apply_index
    }
}
