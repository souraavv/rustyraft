use crate::raft::action::RaftAction;
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
    pub fn randomize_election_timeout(
        &mut self,
        min_timeout_ticks: u64,
        max_timeout_ticks: u64,
    ) {
        self.election_timer = crate::raft::election::ElectionTimer::randomized(
            min_timeout_ticks,
            max_timeout_ticks,
        );
    }

    pub fn tick(
        &mut self,
        cluster_servers: &[ServerId],
    ) -> Result<Vec<RaftAction<C>>, NodeError<St::Error>>
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
                return Ok(self.append_entries_actions());
            }

            return Ok(Vec::new());
        }

        // Followers and candidates use the election timer.
        self.election_timer.tick();

        if !self.election_timer.expired() {
            return Ok(Vec::new());
        }

        tracing::info!(
            server_id = self.id.value(),
            term = self.current_term().value(),
            role = ?self.role,
            "Election timeout expired"
        );

        self.start_election()?;
        self.election_timer.reset();

        Ok(self.request_vote_actions(cluster_servers))
    }
}