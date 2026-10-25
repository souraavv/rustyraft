use crate::raft::state::{
    LeaderState,
    Role,
    ServerId,
    Term,
};
use crate::raft::state_machine::StateMachine;
use crate::raft::LogEntry;
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
    /// Starts a new election
    ///
    /// the node increment its term, becomes a candidate, votes for itself
    /// and create the state used to collect votes
    ///
    /// Sending RequestVote Rpc
    pub fn start_election(
        &mut self,
    ) -> Result<(), NodeError<St::Error>> {

        let new_term = self.current_term().next();

        // A new election always happens in a new term. The node moves
        // to that term before participating in the election.
        self.set_persistent_metadata(
            new_term,
            Some(self.id),
        )?;

        // Starting an election makes this node a candidate. It will
        // remain a candidate until it wins, loses, or learns about
        // another server with a newer term.
        self.role = Role::Candidate;
        self.known_leader = None;
        // A candidate immediately votes for itself. This vote is also
        // recorded in ElectionState so it counts toward the majority.

        // Any previous leader-specific state is no longer relevant
        // because this node is no longer acting as the leader.
        self.leader = None;

        // Create the state used to collect votes for this election.
        self.election = Some(
            crate::raft::election::ElectionState::new(
                self.id,
                new_term,
            ),
        );

        tracing::info!(
            server_id = self.id.value(),
            term = new_term.value(),
            "Raft node became candidate"
        );

        Ok(())
    }

    /// Create the leader state after winning the election
    ///
    /// Every other server start with next_index immediately the leaders
    /// last log entry. match_index start at ZERO
    pub(super) fn become_leader(
        &mut self,
        cluster_servers: &[ServerId],
    ) -> Result<(), NodeError<St::Error>> {
        // After recovery, commitIndex starts at ZERO while the durable
        // log may contain entries from earlier terms. Append a current-term
        // no-op only when there is a prefix that still needs commitment.
        // That lets the normal majority rule commit the prefix without
        // requiring an extra client command after restart.
        if self.volatile.commit_index < self.last_log_index() {
            let noop_index = self.append_log_entry(
                LogEntry::new_noop(self.current_term()),
            )?;

            tracing::info!(
                server_id = self.id.value(),
                term = self.current_term().value(),
                log_index = noop_index.value(),
                "Appended leader no-op entry to commit prior log prefix"
            );
        }
        self.role = Role::Leader;
        self.known_leader = Some(self.id);
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
            },
        );

        Ok(())
    }

    pub(super) fn step_down_for_newer_term(
        &mut self,
        term: Term,
    ) -> Result<(), NodeError<St::Error>> {
        tracing::info!(
            server_id = self.id.value(),
            old_term = self.current_term().value(),
            new_term = term.value(),
            "Stepping down because a newer term was observed"
        );

        self.set_persistent_metadata(
            term,
            None,
        )?;

        self.role = Role::Follower;
        self.leader = None;
        self.election = None;
        self.known_leader = None;

        Ok(())
    }
}