//! Raft Server node
//!
//! A Raft node owns the in-memory state of a single Raft server,
//!
//! by design we keep the network connections and timers outside the node
//! and provide persistent storage through the storage abstraction.

mod actions;
mod append_entries;
mod commit;
mod election;
mod replication;
mod request_vote;
mod timers;

use crate::raft::{HeartbeatTimer, LogEntry};
use crate::raft::election::{
    ElectionState,
    ElectionTimer,
};
use crate::raft::log::RaftLog;
use crate::raft::state::{
    LeaderState,
    LogIndex,
    Role,
    ServerId,
    Term,
    VolatileState,
};
use crate::raft::state_machine::{
    NoopStateMachine,
    StateMachine,
};
use crate::raft::storage::{
    PersistentMetadata,
    RaftStorage,
};

use core::fmt;
use std::error::Error;
use std::fmt::{Debug, Display};
use std::marker::PhantomData;

#[derive(Debug)]
pub enum NodeError<E> {
    Storage {
        operation: &'static str,
        source: E,
    },
}

// Human readable representation of value (not developers - for that we have
// debug)
// E: Display means - we can display the nodeError when the underlying
// E also knows how to display itself
impl<E: Display> Display for NodeError<E> {
    fn fmt(
        &self,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Self::Storage {
                operation,
                source,
            } => write!(
                formatter,
                "storage operation '{}' failed: {}",
                operation,
                source,
            ),
        }
    }
}

// Telling Rust error handling eco-system that NodeError is Error type
// and allowing one error to expose another error as its cause
// Implementing the Error make the type usable through standard error handling
// patterns, including returning it through an error interface Box<dyn Error>
impl<E> Error for NodeError<E>
where
    // 'static bound helps us to prevent accepting error types that contain
    // short-lived borrows
    E: Error + 'static,
{
    fn source(
        &self,
    ) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Storage {
                source,
                ..
            } => Some(source),
        }
    }
}

/// A single Raft server.
///
/// The node contains the state required to participate in the Raft protocol
/// Protocol behaviors such as elections, log replication, and commitment
/// will be implemented in separate modules.
///
/// Thinking design out loud
///    - I have my own identity and role
///    - I have my persistent state
///    - I have my volatile state
///    - I will also become candidate during election so my election
///      should have some metadata about which term I'm standing, who I am
///      and who all voted me
///    - Optionally if I become the leader I might have more info to hold
///
/// We will keep this generic so that we don't care about log data type (C)
#[derive(Debug)]
pub struct RaftNode<
    C,
    St,
    S = NoopStateMachine,
> {
    // My identity - id and role
    id: ServerId,
    role: Role,
    known_leader: Option<ServerId>,

    current_term: Term,
    voted_for: Option<ServerId>,

    // Each Raft server has some persistent state and other volatile state.
    // Whom I voted for, the current term, and logs are persistent.
    storage: St,
    // last applied and commit index are volatile.
    volatile: VolatileState,

    // When I start the election I maintain this state - I record
    // which term I'm standing for, my ID, and whom all voted for me.
    election: Option<ElectionState>,
    election_timer: ElectionTimer,

    // Heartbeat timeouts
    heartbeat_timer: HeartbeatTimer,
    // A newly elected leader must send
    // an initial heartbeat immediately.
    initial_heartbeat_pending: bool,
    // If I win the election then as a leader
    // I will maintain volatile state.
    // I use this to remember the replication state, which is basically
    // the progress of each replica (next_index, match_index), useful to
    // sync their log entries with me during AppendEntries RPC.
    leader: Option<LeaderState>,

    // The state machine receives committed commands in log order.
    state_machine: S,
    _command: PhantomData<fn() -> C>,
}

/// Generic Raft Node with any type implementing the state machine trait.
impl<C, St, S> RaftNode<C, St, S>
where
    St: RaftStorage<C>,
    S: StateMachine<C>,
    St::Error: Debug,
{
    // new construction with a state machine
    pub fn with_storage(
        id: ServerId,
        storage: St,
        state_machine: S,
    ) -> Result<Self, NodeError<St::Error>> {
        let metadata = storage
            .load_metadata()
            .map_err(|source| NodeError::Storage {
                operation: "load_metadata",
                source,
            })?;

        Ok(Self {
            id,
            role: Role::Follower,
            known_leader: None,

            current_term: metadata.current_term(),
            voted_for: metadata.voted_for(),

            storage,

            volatile: VolatileState {
                commit_index: LogIndex::ZERO,
                last_apply_index: LogIndex::ZERO,
            },

            state_machine,

            leader: None,
            election: None,
            election_timer: ElectionTimer::new(5),
            heartbeat_timer: HeartbeatTimer::new(5),
            initial_heartbeat_pending: false,
            _command: PhantomData,
        })
    }

    fn set_persistent_metadata(
        &mut self,
        current_term: Term,
        voted_for: Option<ServerId>,
    ) -> Result<(), NodeError<St::Error>> {
        self.storage
            .save_metadata(
                PersistentMetadata::new(
                    current_term,
                    voted_for,
                ),
            )
            .map_err(|source| NodeError::Storage {
                operation: "save_metadata",
                source,
            })?;

        // update memory only after persistence succeeds
        self.current_term = current_term;
        self.voted_for = voted_for;

        Ok(())
    }

    fn append_log_entry(
        &mut self,
        entry: LogEntry<C>,
    ) -> Result<LogIndex, NodeError<St::Error>> {
        self.storage
            .append_log_entry(entry)
            .map_err(|source| NodeError::Storage {
                operation: "append_log_entry",
                source,
            })
    }

    fn truncate_log_from(
        &mut self,
        index: LogIndex,
    ) -> Result<(), NodeError<St::Error>> {
        self.storage
            .truncate_log_from(index)
            .map_err(|source| NodeError::Storage {
                operation: "truncate_log_from",
                source,
            })
    }

    /// ---- Helpers - Getters ----
    pub fn id(&self) -> ServerId {
        self.id
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn known_leader(&self) -> Option<ServerId> {
        self.known_leader
    }

    pub fn current_term(&self) -> Term {
        self.current_term
    }

    pub fn voted_for(&self) -> Option<ServerId> {
        self.voted_for
    }

    pub fn log(&self) -> &RaftLog<C> {
        self.storage.log()
    }

    pub fn last_log_index(&self) -> LogIndex {
        self.storage.log().last_index()
    }

    pub fn log_at(
        &self,
        index: LogIndex,
    ) -> Option<&LogEntry<C>> {
        self.log().get(index)
    }

    pub fn commit_index(&self) -> LogIndex {
        self.volatile.commit_index
    }

    pub fn last_applied(&self) -> LogIndex {
        self.volatile.last_apply_index
    }

    pub fn election(&self) -> Option<&ElectionState> {
        self.election.as_ref()
    }

    pub fn last_log_term(&self) -> Term {
        self.storage
            .log()
            .last_term()
            .unwrap_or(Term::ZERO)
    }

    // method is consuming the entire RaftNode
    // we want to take the ownership of its persistent state
    // we are moving this out of the node
    pub fn into_storage(
        self,
    ) -> St {
        self.storage
    }

    pub fn from_storage(
        id: ServerId,
        storage: St,
        state_machine: S,
    ) -> Result<Self, NodeError<St::Error>> {
        Self::with_storage(id, storage, state_machine)
    }
}

// Simplistic model of state machine where concrete type is fixed
// to NoopStateMachine
impl<C, St> RaftNode<C, St, NoopStateMachine>
where
    St: RaftStorage<C>,
    St::Error: Debug,
{
    /// Create a new Raft Server
    ///
    /// A new server starts as follower with term = 0, no vote, an empty log,
    /// commit index = 0, last applied index = 0.
    ///
    /// As a new node - I always start as follower, I'm at 0 in terms of
    /// log and current_term, and I've never voted for anyone.
    ///
    /// My volatile state is also at 0.
    pub fn new(
        id: ServerId,
        storage: St,
    ) -> Result<Self, NodeError<St::Error>> {
        Self::with_storage(
            id,
            storage,
            NoopStateMachine,
        )
    }
}