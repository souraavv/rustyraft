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

    /// Appends a client command to the leader's log.
    ///
    /// Only the leader accepts new commands. The command is appended
    /// locally first; replication to followers happens separately.
    pub fn append_entry(
        &mut self, 
        command: C
    ) -> Result<Option<LogIndex>, NodeError<St::Error>> {
        self.append_command(None, command)
    }

    /// Appends a client command together with its request ID.
    ///
    /// The request ID is stored in the log entry so the runtime can
    /// associate the command with its client request later.
    pub fn append_client_entry(
        &mut self, 
        request_id: String, 
        command: C,
    ) -> Result<Option<LogIndex>, NodeError<St::Error>> {
        self.append_command( Some(request_id), command)
    }

    /// Appends a client command to the leader's log
    ///
    /// Only the leader accept the new commands. The command is appended
    /// locally first; replicated to the followers happen separately
    pub fn append_command(
        &mut self,
        request_id: Option<String>,
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
        let entry = match request_id {
            Some(request_id) => {
                LogEntry::new_client_command(term, request_id, command)
            },
            None => LogEntry::new(term, command),
        };


        let index = self.append_log_entry(entry)?;

        tracing::info!(
            server_id = self.id.value(),
            term = term.value(),
            log_index = index.value(),
            "Appended command to leader log"
        );

        Ok(Some(index))
    }

    /// Finds the log index associated with a client request ID.
    ///
    /// This searches the local log, regardless of whether the entry
    /// has been committed yet.
    pub fn client_entry_index(
        &self,
        request_id: &str,
    ) -> Option<LogIndex> {
        self.storage
            .log()
            .iter()
            .enumerate()
            .find_map(|(position, entry)| {
                if entry.request_id() == Some(request_id) {
                    Some(LogIndex::new(position as u64 + 1))
                } else {
                    None
                }
            })
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