//! Raft Server node
//!
//! A Raft node owns the in-memory state of a single Raft server,
//!
//! by design we keep the network connections and timers outside the node
//! and provide persistent storage through the storage abstraction.

use crate::raft::action::RaftAction;
use crate::raft::{HeartbeatTimer, LogEntry};
use crate::raft::Role::{Follower};
use crate::raft::commit::find_commit_index;
use crate::raft::log::RaftLog;

use crate::raft::election::{
    should_grant_vote,
    ElectionState,
    ElectionTimer,
};

use crate::raft::replication::FollowerProgress;
use crate::raft::rpc::{
    AppendEntriesRequest,
    AppendEntriesResponse,
    RequestVoteRequest,
    RequestVoteResponse,
};

use crate::raft::state::{
    LeaderState,
    LogIndex,
    Role, 
    ServerId, 
    Term, 
    VolatileState
};

use crate::raft::state_machine::{
    NoopStateMachine,
    StateMachine,
};

use crate::raft::storage::{
    PersistentMetadata,
    RaftStorage,
};

use std::fmt::Debug;
use std::marker::PhantomData;

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
pub struct RaftNode<
    C,
    St,
    S = NoopStateMachine,
> {
    // My identity - id and role
    id: ServerId,
    role: Role,

    // Each Raft server has some persistent state and other volatile
    // whom I voted, what was current term and logs are persitent
    storage: St,
    // last applied and commit index are volalite
    volatile: VolatileState,

    // When I start the election I maintain this state - i record for
    // which term i will become the candiate, my Id and whom all voted me..
    election: Option<ElectionState>,
    election_timer: ElectionTimer,

    // Heart beat timeouts
    heartbeat_timer: HeartbeatTimer,
    // A newly elected leader must send
    // an initial heartbeat immediately.
    initial_heartbeat_pending: bool,
    // If I win the election then as a leader 
    // I will maintain a  volatile state 
    // I will use this to remember the replication state which is basically
    // the progress of each replica (next_index, match_index) useful to 
    // sync there log entires with me during append entries RPC
    leader: Option<LeaderState>,

    // The state machine receives committed commands in log order.
    state_machine: S,
    _command: PhantomData<fn() -> C>,
}

/// Generic Raft Node with Any type having trait of State machine
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
    ) -> Self {
        Self {
            id,
            role: Role::Follower,

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
        }
    }

    fn persistent_metadata(&self) -> PersistentMetadata {
        self.storage
            .load_metadata()
            .expect("in-memory storage cannot fail")
    }

    fn set_persistent_metadata(
        &mut self,
        current_term: Term,
        voted_for: Option<ServerId>,
    ) {
        self.storage
            .save_metadata(
                PersistentMetadata::new(
                    current_term,
                    voted_for,
                ),
            )
            .expect("in-memory storage cannot fail");
    }

    fn append_log_entry(
        &mut self,
        entry: LogEntry<C>,
    ) -> LogIndex {
        self.storage
            .append_log_entry(entry)
            .expect("in-memory storage cannot fail")
    }

    fn truncate_log_from(
        &mut self,
        index: LogIndex,
    ) {
        self.storage
            .truncate_log_from(index)
            .expect("in-memory storage cannot fail");
    }


    // ------------------------------
    // --------- Actions ------------
    // ------------------------------
    pub fn request_vote_actions(
        &self,
        cluster_servers: &[ServerId],
    ) -> Vec<RaftAction<C>> {

        // create a request vote request
        let request = self.build_request_vote();

        // iterate through the cluster server known to me and create
        // an action (note with this change i added clone to the 
        // requestvoterequest)
        cluster_servers
            .iter()
            .copied()
            .filter(|server_id| *server_id != self.id)
            .map(|server_id| {
                RaftAction::SendRequestVote {
                    target: server_id,
                    request: request.clone(),
                }
            })
            .collect()
    }

    /// Build AppendEntries actions for all followers.
    ///
    /// AppendEntries is used both for heartbeats and for log replication.
    /// The node decides when the requests are required and builds the
    /// protocol actions. The runtime is responsible for executing them
    /// through the transport.
    pub fn append_entries_actions(
        &mut self,
    ) -> Vec<RaftAction<C>>
    where
        C: Clone,
    {
        // If I'm not a leader any more - stale network packet handling
        if self.role != Role::Leader {
            return Vec::new();
        }

        // If this is not the initial heartbeat and the heartbeat timer
        // has not been expired yet then empty action
        if !self.initial_heartbeat_pending
            && !self.heartbeat_timer.expired()
        {
            return Vec::new();
        }

        // get the list of the followers
        let follower_ids = match self.leader.as_ref() {
            Some(leader) => leader.replication.follower_ids(),
            None => {
                tracing::warn!(
                    server_id = self.id.value(),
                    "Leader state missing while building AppendEntries actions"
                );

                return Vec::new();
            }
        };

        let mut actions = Vec::new();

        // Create a append entry action for each of the replica
        // this action will be consumed by the RaftRuntime which then 
        // will forward this to the transport layer
        for follower_id in follower_ids {
            if let Some(request) =
                self.build_append_entries(follower_id)
            {
                actions.push(RaftAction::SendAppendEntries {
                    target: follower_id,
                    request,
                });
            }
        }

        // Sending append entry also has association with the the hearbeat
        // we reset the hearbeat .. that way we dont overflood the hearbeat
        // when we are already sending too many append entries
        // AppendEntry request do reset the election timer on the candidates
        // anyway
        self.heartbeat_timer.reset();
        self.initial_heartbeat_pending = false;

        tracing::debug!(
            server_id = self.id.value(),
            append_entries_count = actions.len(),
            "Built AppendEntries actions"
        );

        actions
    }


    // --------------------------------------------
    // --------------- Handlers -------------------
    // --------------------------------------------

    /// 
    /// Handles a RequestVote RPC.
    ///
    /// The node owns the state changes required by the RPC while the election
    /// module owns the vote decision itself. The response is returned as a
    /// RaftAction so the runtime can deliver it through the transport layer.
    pub fn handle_request_vote(
        &mut self,
        from: ServerId,
        request: RequestVoteRequest,
    ) -> Vec<RaftAction<C>> {
        tracing::debug!(
            server_id = self.id.value(),
            candidate_id = request.candidate_id.value(),
            candidate_term = request.term.value(),
            current_term = self.current_term().value(),
            "Handling RequestVote"
        );

        // A request from an older term is simply rejected.
        // Receiver keeps its current term and rejects the request.
        if request.term < self.current_term() {
            tracing::debug!(
                server_id = self.id.value(),
                candidate_id = request.candidate_id.value(),
                candidate_term = request.term.value(),
                current_term = self.current_term().value(),
                "Rejecting RequestVote from older term"
            );

            return vec![
                RaftAction::SendRequestVoteResponse {
                    target: from,
                    response: RequestVoteResponse::rejected(
                        self.current_term(),
                    ),
                },
            ];
        }

        // A newer term means this node has stale Raft state.
        // Move to the newer term and clear the previous vote because
        // votes are tracked independently for each term.
        if request.term > self.current_term() {
            tracing::info!(
                server_id = self.id.value(),
                old_term = self.current_term().value(),
                new_term = request.term.value(),
                "Updating term from RequestVote"
            );

            self.set_persistent_metadata(
                request.term,
                None,
            );

            // I can't stay at any role including Leader (stale).
            // If I discover someone requesting a vote for a higher term,
            // I should demote myself to follower immediately.
            self.role = Follower;
            self.leader = None;
            self.election = None;
        }

        let grant_vote = should_grant_vote(
            self.current_term(),
            self.voted_for(),
            request.candidate_id,
            request.term,
            request.last_log_index,
            request.last_log_term,
            self.last_log_index(),
            self.last_log_term(),
        );

        // Election decided we can't vote for this candidate for this term.
        if !grant_vote {
            tracing::debug!(
                server_id = self.id.value(),
                candidate_id = request.candidate_id.value(),
                term = self.current_term().value(),
                "Vote rejected"
            );

            return vec![
                RaftAction::SendRequestVoteResponse {
                    target: from,
                    response: RequestVoteResponse::rejected(
                        self.current_term(),
                    ),
                },
            ];
        }

        // The candidate passed all voting rules. Record the vote so this
        // node cannot vote for a different candidate in the same term.
        self.set_persistent_metadata(
            self.current_term(),
            Some(request.candidate_id),
        );

        // Granting a valid vote is election activity.
        // Reset the timer so that follower does not immediately
        // start another election.
        self.election_timer.reset();

        tracing::info!(
            server_id = self.id.value(),
            candidate_id = request.candidate_id.value(),
            term = self.current_term().value(),
            "Vote granted"
        );

        vec![
            RaftAction::SendRequestVoteResponse {
                target: from,
                response: RequestVoteResponse::granted(
                    self.current_term(),
                ),
            },
        ]
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
    ) -> Vec<RaftAction<C>> {
        tracing::debug!(
            server_id = self.id.value(),
            response_term = response.term.value(),
            current_term = self.current_term().value(),
            vote_granted = response.vote_granted,
            "Handling RequestVote response"
        );

        // A response from a newer term means our election is stale.
        // Move to that term and return to follower state.
        if response.term > self.current_term() {
            tracing::info!(
                server_id = self.id.value(),
                old_term = self.current_term().value(),
                new_term = response.term.value(),
                "Stepping down because a newer term was observed"
            );

            self.set_persistent_metadata(
                response.term,
                None,
            );

            self.role = Role::Follower;
            self.leader = None;
            self.election = None;

            return Vec::new();
        }

        // Only the election belonging to our current term can affect
        // the result of the current election.
        if response.term < self.current_term() {
            tracing::debug!(
                server_id = self.id.value(),
                response_term = response.term.value(),
                current_term = self.current_term().value(),
                "Ignoring stale RequestVote response"
            );

            return Vec::new();
        }

        // A node that is no longer a candidate cannot use an old vote
        // response to become leader.
        if self.role != Role::Candidate {
            tracing::debug!(
                server_id = self.id.value(),
                role = ?self.role,
                "Ignoring vote response because node is not a candidate"
            );

            return Vec::new();
        }

        // A rejected vote does not change the election state. We keep
        // waiting for responses from the other servers.
        if !response.vote_granted {
            tracing::debug!(
                server_id = self.id.value(),
                "Vote was not granted"
            );

            return Vec::new();
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

                    return Vec::new();
                }
            };

            election.record_vote(voter_id);

            tracing::debug!(
                server_id = self.id.value(),
                voter_id = voter_id.value(),
                cluster_size = cluster_servers.len(),
                "Recorded RequestVote response"
            );

            election.has_majority(cluster_servers.len())
        };

        if has_majority {
            self.become_leader(cluster_servers);
        }

        Vec::new()
    }

    // ---------------------------------------------------------
    // ----------- Append Entries handlers -------------------
    // --------------------------------------------------------

    /// Handles an AppendEntries RPC from the leader.
    ///
    /// AppendEntries is used both for log replication and heartbeats.
    /// The follower first verifies that the leader's previous log entry
    /// matches its own log before modifying anything.
    pub fn handle_append_entries(
        &mut self,
        from: ServerId,
        request: AppendEntriesRequest<C>,
    ) -> Vec<RaftAction<C>> {
        tracing::debug!(
            server_id = self.id.value(),
            leader_id = request.leader_id.value(),
            request_term = request.term.value(),
            current_term = self.current_term().value(),
            prev_log_index = request.prev_log_index.value(),
            entry_count = request.entries.len(),
            "Handling AppendEntries"
        );

        // A request from an older term cannot come from the current
        // leader, so reject it without modifying local state.
        if request.term < self.current_term() {
            tracing::debug!(
                server_id = self.id.value(),
                leader_id = request.leader_id.value(),
                request_term = request.term.value(),
                current_term = self.current_term().value(),
                "Rejecting AppendEntries from older term"
            );

            return vec![
                RaftAction::SendAppendEntriesResponse {
                    target: from,
                    response: AppendEntriesResponse::failure(
                        self.current_term(),
                    ),
                },
            ];
        }

        // A newer term means this node has stale state. Move to the
        // leader's term and become a follower before processing the RPC.
        if request.term > self.current_term() {
            tracing::info!(
                server_id = self.id.value(),
                old_term = self.current_term().value(),
                new_term = request.term.value(),
                leader_id = request.leader_id.value(),
                "Updating term from AppendEntries"
            );

            self.set_persistent_metadata(
                request.term,
                None,
            );

            // reset back to the follower
            self.role = Role::Follower;
            self.leader = None;
            self.election = None;
        } else if self.role != Role::Follower {
            // A valid AppendEntries from the current term establishes
            // that another server is acting as leader. A candidate or
            // leader must therefore stop its current election/leadership.
            tracing::info!(
                server_id = self.id.value(),
                leader_id = request.leader_id.value(),
                term = request.term.value(),
                "Stepping down after AppendEntries"
            );

            // reset back to the follower
            self.role = Role::Follower;
            self.leader = None;
            self.election = None;
        }

        // kept after the request.term < self.persistent.current_term
        // because we dont' want stale incoming packets to reset the timer
        // in nutshell - old leader message should not reset the election
        // timer
        self.election_timer.reset();

        // The previous log entry is the consistency point between the
        // leader and follower. If it does not exist or has a different
        // term, the follower must reject the request.
        if request.prev_log_index != LogIndex::ZERO {
            let previous_entry_matches = self
                .storage
                .log()
                .matches(
                    request.prev_log_index,
                    request.prev_log_term,
                );

            if !previous_entry_matches {
                tracing::debug!(
                    server_id = self.id.value(),
                    leader_id = request.leader_id.value(),
                    prev_log_index =
                        request.prev_log_index.value(),
                    prev_log_term =
                        request.prev_log_term.value(),
                    local_last_index =
                        self.last_log_index().value(),
                    "AppendEntries log consistency check failed"
                );

                // If our logs doesn't match I will simply reply the leader
                // that I can proceed.. and this is where leader will start
                // backtracking from nextIndex until it found.. and that's
                // where recovery start..
                return vec![
                    RaftAction::SendAppendEntriesResponse {
                        target: from,
                        response: AppendEntriesResponse::failure(
                            self.current_term(),
                        ),
                    },
                ];
            }
        }

        let entry_count = request.entries.len();

        // The previous entry matches, so the leader and follower agree
        // up to this point. Reconcile the entries that follow it.

        // incoming entries: request.entries (one or many)
        // They will go at prev_log_index + offset + 1
        for (offset, entry) in request.entries.into_iter().enumerate() {
            let index = LogIndex::new(
                request.prev_log_index.value()
                    + offset as u64
                    + 1,
            );

            // Fetch the term at the next log entry (log = Vec<LogEntry<C>>)
            // each entry contains term and Command
            // we are interested in term at that index (start the current
            // length which is essentially the prev_log_index provided
            // by the leader)
            match self.storage.log().term_at(index) {
                Some(local_term) if local_term == entry.term => {
                    // This entry already matches the leader's entry.
                    // Nothing needs to be changed.
                }

                Some(_) => {
                    // A different term at this index means the follower
                    // has a conflicting entry. Remove it and everything
                    // after it before appending the leader's entries.
                    tracing::debug!(
                        server_id = self.id.value(),
                        index = index.value(),
                        "Truncating conflicting log entries"
                    );

                    self.truncate_log_from(index);
                    self.append_log_entry(entry);
                }

                None => {
                    // The follower does not have this entry yet, so
                    // append the missing leader entry.
                    self.append_log_entry(entry);
                }
            }
        }

        // A leader tells follower how far the log is committed
        // A follower can't commit beyond that
        if request.leader_commit > self.volatile.commit_index {
            // can't exceed my length anyway thus min of the leader commit_index
            let new_commit_index = std::cmp::min(
                request.leader_commit,
                self.last_log_index(),
            );

            tracing::debug!(
                server_id = self.id.value(),
                old_commit_index =
                    self.volatile.commit_index.value(),
                new_commit_index =
                    new_commit_index.value(),
                leader_commit =
                    request.leader_commit.value(),
                "Advancing follower commit index"
            );

            self.volatile.commit_index = new_commit_index;
        }

        if let Err(_) =
            self.apply_committed_entries()
        {
            tracing::error!(
                server_id = self.id.value(),
                "Failed to apply committed entries"
            );
        }

        // The request was accepted, so now determine the highest
        // log index established by this AppendEntries RPC.
        let replicated_index =
            LogIndex::new(
                request
                    .prev_log_index
                    .value()
                    .checked_add(
                        entry_count as u64,
                    )
                    .expect(
                        "AppendEntries index exhausted",
                    ),
            );

        tracing::debug!(
            server_id = self.id.value(),
            leader_id = request.leader_id.value(),
            term = self.current_term().value(),
            last_log_index =
                self.last_log_index().value(),
            replicated_index =
                replicated_index.value(),
            commit_index =
                self.volatile.commit_index.value(),
            "AppendEntries accepted"
        );

        vec![
            RaftAction::SendAppendEntriesResponse {
                target: from,
                response: AppendEntriesResponse::success(
                    self.current_term(),
                    replicated_index,
                ),
            },
        ]
    }

    /// Handles an AppendEntries resopnse from a follower
    ///
    /// Successfull replication advances the follower progress (succes_progress)
    /// Failed replication moves next_index backward so the leader
    /// can retry from an earlier log position
    pub fn handle_append_entries_response(
        &mut self,
        follower_id: ServerId,
        response: AppendEntriesResponse,
    ) -> Vec<RaftAction<C>> {

        if self.role != Role::Leader {
            tracing::debug!(
                server_id = self.id.value(),
                follower_id = follower_id.value(),
                "Ignoring AppendEntries response because node is not leader"
            );

            return Vec::new();
        }

        // Some else become leader
        // A response from a newer term means this leader has stale
        // state and must step down before processing the response.
        if response.term > self.current_term() {
            self.step_down_for_newer_term(response.term);
            return Vec::new();
        }

        // Stale network packets
        // A response from an older term belongs to an earlier
        // interaction and cannot affect the current leader state.
        if response.term < self.current_term() {
            tracing::debug!(
                server_id = self.id.value(),
                follower_id = follower_id.value(),
                response_term = response.term.value(),
                current_term = self.current_term().value(),
                "Ignoring stale AppendEntries response"
            );

            return Vec::new();
        }

        let leader = match self.leader.as_mut() {
            Some(leader) => leader,
            None => {
                tracing::warn!(
                    server_id = self.id.value(),
                    "Leader state missing while handling response"
                );

                return Vec::new();
            }
        };

        // handle success or failure
        leader.replication.handle_response(
            follower_id,
            &response,
        );

        // update follower match_index
        self.update_commit_index();

        // apply_commited_entries()
        if let Err(_) = self.apply_committed_entries() {
            tracing::error!(
                server_id = self.id.value(),
                "Failed to apply committed entries"
            );
        }

        Vec::new()
    }

    // -----------------------------------------------
    // ---------------- Request builder helpers ------
    // -----------------------------------------------
    
    pub fn build_request_vote(
        &self
    ) -> RequestVoteRequest {
        RequestVoteRequest::new(
            self.current_term(),
            self.id,
            self.last_log_index(),
            self.last_log_term(),
        )
    }


    /// Build an AppendEntries request for a follower
    /// 
    /// The leader specific replication state determines which log entry 
    /// the follower needs the next
    pub fn build_append_entries(
        &self, 
        follower_id: ServerId,
    ) -> Option<AppendEntriesRequest<C>>
    where 
        C: Clone,
    {
        let leader = self.leader.as_ref()?;

        leader.replication.build_append_entries(
            follower_id,
            self.id, 
            self.current_term(),
            self.storage.log(), 
            self.volatile.commit_index,
        )
    }

    // --------------------------------
    // ------ Election helpers --------
    // --------------------------------

    /// Starts a new election
    /// 
    /// the node increment its term, becomes a candidate, votes for itself
    /// and create the state used to collect votes
    /// 
    /// Sending RequestVote Rpc 
    pub fn start_election(&mut self) {

        let new_term = self.current_term().next();

        // A new election always happens in a new term. The node moves
        // to that term before participating in the election.
        self.set_persistent_metadata(
            new_term,
            Some(self.id),
        );

        // Starting an election makes this node a candidate. It will
        // remain a candidate until it wins, loses, or learns about
        // another server with a newer term.
        self.role = Role::Candidate;

        // A candidate immediately votes for itself. This vote is also
        // recorded in ElectionState so it counts toward the majority.

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
        self.role = Role::Leader;

        self.election = None;

        // A newly elected leader must immediately send
        // AppendEntries to every other server.
        self.initial_heartbeat_pending = true;

        let followers: Vec<ServerId> =
            cluster_servers
                .iter()
                .copied()
                .filter(|server_id| *server_id != self.id)
                .collect();

        let last_log_index =
            self.last_log_index();

        tracing::info!(
            server_id = self.id.value(),
            term = self.current_term().value(),
            last_log_index = last_log_index.value(),
            follower_count = followers.len(),
            "Raft node became leader"
        );

        for follower_id in &followers {
            tracing::debug!(
                server_id = self.id.value(),
                follower_id = follower_id.value(),
                next_index = last_log_index.next().value(),
                "Initializing replication state for follower"
            );
        }

        self.leader = Some(
            LeaderState {
                replication:
                    crate::raft::replication::ReplicationState::new(
                        &followers,
                        last_log_index,
                    ),
            }
        );
    }


    /// Appends a client command to the leader's log
    /// 
    /// Only the leader accept the new commands. The command is appended
    /// locally first; replicated to the followers happen separately
    pub fn append_entry(
        &mut self, 
        command: C,
    ) -> Option<LogIndex> {
        if self.role != Role::Leader {
            tracing::debug!(
                server_id = self.id.value(),
                role = ?self.role,
                "Rejecting log entry because node is not leader"
            );

            return None;
        }

        let term = self.current_term();
        let index = self.append_log_entry(
            LogEntry::new(term, command),
        );

        tracing::info!(
            server_id = self.id.value(),
            term = term.value(),
            log_index = index.value(),
            "Appended command to leader log"
        );

        Some(index)
    }


    fn step_down_for_newer_term(&mut self, term: Term) {
        tracing::info!(
            server_id = self.id.value(),
            old_term = self.current_term().value(),
            new_term = term.value(),
            "Stepping down because a newer term was observed"
        );

        self.set_persistent_metadata(
            term,
            None,
        );
        self.role = Role::Follower;
        self.leader = None;
        self.election = None;
    }

    /// Update the commit index
    /// Leader will get the commit index it maintains for the replicas
    /// or followers and will also append its commit index which is always 
    /// the last one (new one) 
    pub fn update_commit_index(&mut self) {
        // Late coming packets - fault tolerant if condition - so that
        // we don't break the safety of Raft 
        if self.role != Role::Leader {
            tracing::debug!(
                server_id = self.id.value(),
                role = ?self.role,
                "Ignoring commit update because node is not leader"
            );
            return;
        }

        let mut match_indexes = {
            let leader = match self.leader.as_ref() {
                Some(leader) => leader,
                None => {
                    tracing::warn!(
                        server_id = self.id.value(),
                        "Leader state missing while updating commit index"
                    );
                    return;
                }
            };
            leader.replication.match_indexes()
        };

        // Leader should also consider his own counts towards the majority
        match_indexes.push(self.last_log_index());
        
        let new_commit_index = find_commit_index(
            self.volatile.commit_index,
            self.current_term(),
            &match_indexes,
            |index| self.storage.log().term_at(index),
        );

        // there is a chance to persist
        if new_commit_index > self.volatile.commit_index {
            tracing::info!(
                server_id = self.id.value(),
                old_commit_index =
                    self.volatile.commit_index.value(),
                new_commit_index =
                    new_commit_index.value(),
                "Advanced commit index"
            );
            
            self.volatile.commit_index = new_commit_index;
        }
    }

    pub fn follower_progress(
        &self,
        follower_id: ServerId,
    ) -> Option<FollowerProgress> {
        self.leader.as_ref()?
            .replication
            .progress(follower_id)
    }


    /// Applies committed log entries that have not been applied yet.
    ///
    /// Entries are applied strictly in log order. The apply index is
    /// advanced only after the state machine successfully applies an entry.
    pub fn apply_committed_entries(
        &mut self,
    ) -> Result<(), S::Error> {

        while self.volatile.last_apply_index < self.volatile.commit_index {
            let index = self.volatile.last_apply_index.next();

            let entry = match self.storage.log().get(index) {
                Some(entry) => entry,
                None => {
                    tracing::error!(
                        server_id = self.id.value(),
                        index = index.value(),
                        commit_index =
                            self.volatile.commit_index.value(),
                        "Committed log entry is missing"
                    );
                    break;
                }
            };

            tracing::debug!(
                server_id = self.id.value(),
                log_index = index.value(),
                "Applying committed log entry"
            );

            self.state_machine.apply(&entry.command)?;

            self.volatile.last_apply_index = index;

            tracing::debug!(
                server_id = self.id.value(),
                last_apply_index = index.value(),
                "Applied committed log entry"
            );
        }
        Ok(())
    }

    pub fn tick(
        &mut self,
        cluster_servers: &[ServerId],
    ) -> Vec<RaftAction<C>>
    where
        C: Clone,
    {
        // Leaders use the heartbeat timer. They do not participate
        // in election timeout processing.
        if self.role == Role::Leader {
            self.heartbeat_timer.tick();

            if self.heartbeat_timer.expired()
                || self.initial_heartbeat_pending
            {
                return self.append_entries_actions();
            }

            return Vec::new();
        }

        // Followers and candidates use the election timer.
        self.election_timer.tick();

        if !self.election_timer.expired() {
            return Vec::new();
        }

        tracing::info!(
            server_id = self.id.value(),
            term = self.current_term().value(),
            role = ?self.role,
            "Election timeout expired"
        );

        self.start_election();
        self.election_timer.reset();

        self.request_vote_actions(cluster_servers)
    } 

    pub fn from_storage(
        id: ServerId,
        storage: St,
        state_machine: S,
    ) -> Self {
        Self {
            id,
            role: Role::Follower,
            storage,
            volatile: VolatileState::new(),
            state_machine,
            leader: None,
            election: None,
            election_timer: ElectionTimer::new(5),
            heartbeat_timer: HeartbeatTimer::new(5),
            initial_heartbeat_pending: false,
            _command: PhantomData,
        }
    }

    /// ---- Helpers - Getters ----
    pub fn id(&self) -> ServerId {
        self.id
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn current_term(&self) -> Term {
        self.persistent_metadata().current_term()
    }

    pub fn voted_for(&self) -> Option<ServerId> {
        self.persistent_metadata().voted_for()
    }

    pub fn log(&self) -> &RaftLog<C> {
        self.storage.log()
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

    pub fn last_log_index(&self) -> LogIndex {
        self.storage
            .last_log_index()
            .expect("in-memory storage cannot fail")
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
    /// A new server starts as followr with term = 0, no vote, an empty log
    /// commit index = 0, last applied index = 0
    /// 
    /// As a new node - I always starts as follower, I'm at 0 in term of
    /// log, current_term and I've never voted any one
    /// 
    /// My volalite state is also at 0
    pub fn new(
        id: ServerId,
        storage: St,
    ) -> Self {
        Self {
            id,
            role: Role::Follower,

            storage,

            volatile: VolatileState {
                commit_index: crate::raft::state::LogIndex::ZERO,
                last_apply_index: crate::raft::state::LogIndex::ZERO,
            },

            leader: None,
            election: None,
            election_timer: ElectionTimer::new(5), 
            heartbeat_timer: HeartbeatTimer::new(5), 
            state_machine: NoopStateMachine,
            initial_heartbeat_pending: false,
            _command: PhantomData,
        }
    }
}
