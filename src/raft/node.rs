//! Raft Server node
//!
//! A Raft node owns the in-memory state of a single Raft server,
//!
//! by design we kept the network connections, timers, or durable storage
//! outside the node

use crate::cluster;
use crate::raft::Role::{Follower};
use crate::raft::log::RaftLog;

use crate::raft::election::{
    should_grant_vote,
    ElectionState,
};

use crate::raft::rpc::{
    RequestVoteRequest,
    RequestVoteResponse,
};

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
/// 
/// Thinking desing out loud
///    - I have my own identity and role
///    - I have my persistent state
///    - I have my volatile state
///    - I will also become candidate during election so my election 
///      should have some metadata about which term I'm standing, who I am
///      and who all voted me
///    - Optionally if i become the leader I might have more info to hold
/// 
/// We will keep this generic so that we don't care log data type (C)
/// 
#[derive(Debug)]
pub struct RaftNode<C> {
    // My identity - id and role
    id: ServerId,
    role: Role,

    // Each Raft server has some persistent state and other volatile
    // whom I voted, what was current term and logs are persitent
    persistent: PersistentState<RaftLog<C>>,
    // last applied and commit index are volalite
    volatile: VolatileState,

    // When I start the election I maintain this state - i record for
    // which term i will become the candiate, my Id and whom all voted me..
    election: Option<ElectionState>,
    // If I win the election then as a leader 
    // I will maintain a  volatile state 
    // I will use this to remember the replication state which is basically
    // the progress of each replica (next_index, match_index) useful to 
    // sync there log entires with me during append entries RPC
    leader: Option<LeaderState>,
}

impl<C> RaftNode<C> {

    /// Create a new Raft Server
    ///
    /// A new server starts as followr with term = 0, no vote, an empty log
    /// commit index = 0, last applied index = 0
    /// 
    /// As a new node - I always starts as follower, I'm at 0 in term of
    /// log, current_term and I've never voted any one
    /// 
    /// My volalite state is also at 0
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

    /// Handles a response to a RequestVote RPC.
    /// 
    /// A candidate records granted votes until it has majority
    /// A response from a newer term always cause the node to step down
    /// because its current term is stale
    pub fn handle_request_vote_response(
        &mut self, 
        voter_id: ServerId,
        response: RequestVoteResponse,
        cluster_servers: &[ServerId],
    ) {
        tracing::debug!(
            server_id = self.id.value(),
            response_term = response.term.value(),
            current_term = self.current_term().value(),
            vote_granted = response.vote_granted,
            "Handling RequestVote response"
        );

        // A response from a newer term means our election is stale.
        // Move to that term and return to follower state.
        if response.term > self.persistent.current_term {
            tracing::info!(
                server_id = self.id.value(),
                old_term = self.persistent.current_term.value(),
                new_term = response.term.value(),
                "Stepping down because a newer term was observed"
            );

            self.persistent.current_term = response.term;
            self.persistent.voted_for = None;
            self.role = Role::Follower;
            self.leader = None;
            self.election = None;

            return;
        }

        // Only the election belonging to our current term can affect
        // the result of the current election.
        if response.term < self.persistent.current_term {
            tracing::debug!(
                server_id = self.id.value(),
                response_term = response.term.value(),
                current_term = self.persistent.current_term.value(),
                "Ignoring stale RequestVote response"
            );

            return;
        }

        // A node that is no longer a candidate cannot use an old vote
        // response to become leader.
        if self.role != Role::Candidate {
            tracing::debug!(
                server_id = self.id.value(),
                role = ?self.role,
                "Ignoring vote response because node is not a candidate"
            );

            return;
        }

        // A rejected vote does not change the election state. We keep
        // waiting for responses from the other servers.
        if !response.vote_granted {
            tracing::debug!(
                server_id = self.id.value(),
                "Vote was not granted"
            );

            return;
        }

        // we are using { .. } so that we can borrow the election as mut 
        // and end the borrow as soon as outer { } of this let has_majority
        // ends.. the method become_leader also mutate the election to None
        // so we can't have two writers.. to solve the we added simple scope
        // thingy
        let has_majority = {
            let election = match self.election.as_mut() {
                Some(election) => election,
                None => {
                    tracing::warn!(
                        server_id = self.id.value(),
                        "Received vote without an active election"
                    );
                    return;
                }
            };

            election.record_vote(voter_id);
            election.has_majority(cluster_servers.len())
        };

        if has_majority {
            self.become_leader(cluster_servers);
        }

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


    /// Create the leader state after winning the election
    /// 
    /// Every other server start with next_index immediately the leaders
    /// last log entry. match_index start at ZERO 
    fn become_leader(
        &mut self, 
        cluster_servers: &[ServerId],
    ) {
        let followers: Vec<ServerId> = cluster_servers
            .iter()
            .copied()
            .filter(|server_id| *server_id != self.id)
            .collect();

        let last_log_index = self.persistent.log.last_index();

        self.role = Role::Leader;

        // leader assume each follower has log until its last log index
        // later when it will discover differently it will share the append
        // entries accordingly
        self.leader = Some(LeaderState::new(
            &followers,
            last_log_index
        ));

        self.election = None;

        tracing::info!(
            server_id = self.id.value(),
            term = self.current_term().value(),
            last_log_index = last_log_index.value(),
            follower_count = followers.len(),
            "Raft node became leader"
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
