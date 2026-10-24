use crate::raft::action::RaftAction;
use crate::raft::election::should_grant_vote;
use crate::raft::rpc::{
    RequestVoteRequest,
    RequestVoteResponse,
};
use crate::raft::state::{
    Role,
    ServerId,
};
use crate::raft::state_machine::StateMachine;
use crate::raft::storage::RaftStorage;

use std::fmt::Debug;

use super::{
    NodeError,
    RaftNode,
};

impl<C, St, S> RaftNode<C, St, S>
where
    St: RaftStorage<C>,
    S: StateMachine<C>,
    St::Error: Debug,
{
    /// Handles a RequestVote RPC.
    ///
    /// The node owns the state changes required by the RPC while the election
    /// module owns the vote decision itself. The response is returned as a
    /// RaftAction so the runtime can deliver it through the transport layer.
    pub fn handle_request_vote(
        &mut self,
        from: ServerId,
        request: RequestVoteRequest,
    ) -> Result<Vec<RaftAction<C>>, NodeError<St::Error>> {
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

            return Ok(vec![
                RaftAction::SendRequestVoteResponse {
                    target: from,
                    response: RequestVoteResponse::rejected(
                        self.current_term(),
                    ),
                },
            ]);
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
            )?;

            // I can't stay at any role including Leader (stale).
            // If I discover someone requesting a vote for a higher term,
            // I should demote myself to follower immediately.
            self.role = Role::Follower;
            self.leader = None;
            self.election = None;
            self.known_leader = None;
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

            return Ok(vec![
                RaftAction::SendRequestVoteResponse {
                    target: from,
                    response: RequestVoteResponse::rejected(
                        self.current_term(),
                    ),
                },
            ]);
        }

        // The candidate passed all voting rules. Record the vote so this
        // node cannot vote for a different candidate in the same term.
        self.set_persistent_metadata(
            self.current_term(),
            Some(request.candidate_id),
        )?;

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

        Ok(vec![
            RaftAction::SendRequestVoteResponse {
                target: from,
                response: RequestVoteResponse::granted(
                    self.current_term(),
                ),
            },
        ])
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
    ) -> Result<Vec<RaftAction<C>>, NodeError<St::Error>> {
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
            )?;

            self.role = Role::Follower;
            self.leader = None;
            self.election = None;
            self.known_leader = None;

            return Ok(Vec::new());
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

            return Ok(Vec::new());
        }

        // A node that is no longer a candidate cannot use an old vote
        // response to become leader.
        if self.role != Role::Candidate {
            tracing::debug!(
                server_id = self.id.value(),
                role = ?self.role,
                "Ignoring vote response because node is not a candidate"
            );

            return Ok(Vec::new());
        }

        // A rejected vote does not change the election state. We keep
        // waiting for responses from the other servers.
        if !response.vote_granted {
            tracing::debug!(
                server_id = self.id.value(),
                "Vote was not granted"
            );

            return Ok(Vec::new());
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

                    return Ok(Vec::new());
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

        Ok(Vec::new())
    }
}