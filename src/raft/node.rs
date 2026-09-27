//! Raft Server node
//!
//! A Raft node owns the in-memory state of a single Raft server,
//!
//! by design we kept the network connections, timers, or durable storage
//! outside the node

use crate::raft::Role::{Candidate, Follower};
use crate::raft::election::should_grant_vote;
use crate::raft::log::RaftLog;
use crate::raft::election::ElectionState;
use crate::raft::rpc::{RequestVoteRequest, RequestVoteResponse};
use crate::raft::state::{
    LeaderState, 
    PersistentState, 
    Role, 
    ServerId, 
    Term, 
    VolatileState
};

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
    election: Option<ElectionState>,
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
            election: None,
        }
    }

    /// Hanldes a RequestVote RPC
    /// 
    /// The node owns the state changes required by the RPC while election
    /// module owns the vote decision itself
    pub fn handle_request_vote(
        &mut self, 
        request: RequestVoteRequest
    ) -> RequestVoteResponse {
       tracing::debug!(
            server_id = self.id.value(),
            candidate_id = request.candidate_id.value(),
            candidate_term = request.term.value(),
            current_term = self.current_term().value(),
            "Handling RequestVote"
        );

        // A request from an older term is simply reject
        // Receiver kept its current term and rejects the request
        if request.term < self.persistent.current_term {
            tracing::debug!(
                server_id = self.id.value(),
                candidate_id = request.candidate_id.value(),
                candidate_term = request.term.value(),
                current_term = self.current_term().value(),
                "Rejecting RequestVote from older term"
            );
            return RequestVoteResponse::rejected(
                self.persistent.current_term,
            );
        }

        // A newer term means this node has stale Raft state
        // Move to the newer term and clear the previous vote because
        // votes are tracked independently for each terms
        if request.term > self.persistent.current_term {
            tracing::info!(
                server_id = self.id.value(),
                old_term = self.persistent.current_term.value(),
                new_term = request.term.value(),
                "Updating term from RequestVote"
            );

            self.persistent.current_term = request.term;
            self.persistent.voted_for = None;
            // I can't stay at any role including Leader (stale) 
            // If i discover someone requesting vote for higher term
            // I should de-promote my self as follower immediately on such
            // events
            self.role = Follower;
            self.leader = None; 
        }

        let grant_vote = should_grant_vote(
            self.persistent.current_term,
            self.persistent.voted_for,
            request.candidate_id,
            request.term,
            request.last_log_index,
            request.last_log_term,
            self.persistent.log.last_index(),
            self.persistent.log.last_term()
                .unwrap_or(Term::ZERO),
        );

        // election decided we can't vote to this candidate for this term
        if !grant_vote {
            tracing::debug!(
                server_id = self.id.value(),
                candidate_id = request.candidate_id.value(),
                term = self.persistent.current_term.value(),
                "Vote rejected"
            );
            return RequestVoteResponse::rejected(
                self.persistent.current_term,
            );
        }

        // The candidate passed all voting rules. Record the vote so this
        // node cannot vote for a different candidate in the same term.
        self.persistent.voted_for = Some(request.candidate_id);

        tracing::info!(
            server_id = self.id.value(),
            candidate_id = request.candidate_id.value(),
            term = self.persistent.current_term.value(),
            "Vote granted"
        );

        RequestVoteResponse::granted(
            self.persistent.current_term,
        )
    }

    /// Starts a new election
    /// 
    /// the node increment its term, becomes a candidate, votes for itself
    /// and create the state used to collect votes
    /// 
    /// Sending RequestVote Rpc 
    pub fn start_election(&mut self) {

        let new_term = self.persistent.current_term.next();

        // A new election always happens in a new term. The node moves
        // to that term before participating in the election.
        self.persistent.current_term = new_term;

        // Starting an election makes this node a candidate. It will
        // remain a candidate until it wins, loses, or learns about
        // another server with a newer term.
        self.role = Role::Candidate;

        // A candidate immediately votes for itself. This vote is also
        // recorded in ElectionState so it counts toward the majority.
        self.persistent.voted_for = Some(self.id);

        // Any previous leader-specific state is no longer relevant
        // because this node is no longer acting as the leader.
        self.leader = None;

        // Create the state used to collect votes for this election.
        self.election = Some(ElectionState::new(
            self.id,
            new_term,
        ));

        tracing::info!(
            server_id = self.id.value(),
            term = new_term.value(),
            "Raft node became candidate"
        );
    }


    /// ---- Helpers - Getters ----
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

    pub fn election(&self) -> Option<&ElectionState> {
        self.election.as_ref()
    }
}
