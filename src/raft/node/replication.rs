use crate::raft::replication::FollowerProgress;
use crate::raft::state::{
    LogIndex,
    Role,
};
use crate::raft::state_machine::StateMachine;
use crate::raft::storage::RaftStorage;
use crate::raft::LogEntry;

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
    /// Appends a client command to the leader's log
    ///
    /// Only the leader accept the new commands. The command is appended
    /// locally first; replicated to the followers happen separately
    pub fn append_entry(
        &mut self,
        command: C,
    ) -> Result<Option<LogIndex>, NodeError<St::Error>> {
        if self.role != Role::Leader {
            tracing::debug!(
                server_id = self.id.value(),
                role = ?self.role,
                "Rejecting log entry because node is not leader"
            );

            return Ok(None);
        }

        let term = self.current_term();
        let index = self.append_log_entry(
            LogEntry::new(term, command),
        )?;

        tracing::info!(
            server_id = self.id.value(),
            term = term.value(),
            log_index = index.value(),
            "Appended command to leader log"
        );

        Ok(Some(index))
    }

    pub fn follower_progress(
        &self,
        follower_id: crate::raft::state::ServerId,
    ) -> Option<FollowerProgress> {
        self.leader.as_ref()?
            .replication
            .progress(follower_id)
    }
}