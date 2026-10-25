use crate::raft::action::RaftAction;
use crate::raft::rpc::{
    AppendEntriesRequest,
    RequestVoteRequest,
};
use crate::raft::state::{
    Role,
    ServerId,
};
use crate::raft::state_machine::StateMachine;
use crate::raft::storage::RaftStorage;

use std::fmt::Debug;

use super::{
    RaftNode,
};

impl<C, St, S> RaftNode<C, St, S>
where
    St: RaftStorage<C>,
    S: StateMachine<C>,
    St::Error: Debug,
{
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

    // -----------------------------------------------
    // ---------------- Request builder helpers ------
    // -----------------------------------------------

    pub fn build_request_vote(
        &self,
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
}