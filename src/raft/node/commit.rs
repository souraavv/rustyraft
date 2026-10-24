use crate::raft::commit::find_commit_index;
use crate::raft::state::Role;
use crate::raft::state_machine::StateMachine;
use crate::raft::storage::RaftStorage;

use std::fmt::Debug;

use super::RaftNode;

impl<C, St, S> RaftNode<C, St, S>
where
    St: RaftStorage<C>,
    S: StateMachine<C>,
    St::Error: Debug,
{
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
}