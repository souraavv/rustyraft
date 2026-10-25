use crate::raft::action::RaftAction;
use crate::raft::rpc::{
    AppendEntriesRequest,
    AppendEntriesResponse,
};
use crate::raft::state::{
    LogIndex,
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
    /// Handles an AppendEntries RPC from the leader.
    ///
    /// AppendEntries is used both for heartbeats and for log replication.
    /// The follower first verifies that the leader's previous log entry
    /// matches its own log before modifying anything.
    pub fn handle_append_entries(
        &mut self,
        from: ServerId,
        request: AppendEntriesRequest<C>,
    ) -> Result<Vec<RaftAction<C>>, NodeError<St::Error>> {
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

            return Ok(vec![
                RaftAction::SendAppendEntriesResponse {
                    target: from,
                    response: AppendEntriesResponse::failure(
                        self.current_term(),
                    ),
                },
            ]);
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
            )?;

            // reset back to the follower
            self.role = Role::Follower;
            self.leader = None;
            self.election = None;
            self.known_leader = None;
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
            self.known_leader = None;
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
                return Ok(vec![
                    RaftAction::SendAppendEntriesResponse {
                        target: from,
                        response: AppendEntriesResponse::failure(
                            self.current_term(),
                        ),
                    },
                ]);
            }
        }

        // A matching AppendEntries establishes the leader for this term.
        self.known_leader = Some(request.leader_id);
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

                    self.truncate_log_from(index)?;
                    self.append_log_entry(entry)?;
                }

                None => {
                    // The follower does not have this entry yet, so
                    // append the missing leader entry.
                    self.append_log_entry(entry)?;
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

        Ok(vec![
            RaftAction::SendAppendEntriesResponse {
                target: from,
                response: AppendEntriesResponse::success(
                    self.current_term(),
                    replicated_index,
                ),
            },
        ])
    }

    /// Handles an AppendEntries response from a follower
    ///
    /// Successful replication advances the follower progress (success_progress)
    /// Failed replication moves next_index backward so the leader
    /// can retry from an earlier log position
    pub fn handle_append_entries_response(
        &mut self,
        follower_id: ServerId,
        response: AppendEntriesResponse,
    ) -> Result<Vec<RaftAction<C>>, NodeError<St::Error>> {

        if self.role != Role::Leader {
            tracing::debug!(
                server_id = self.id.value(),
                follower_id = follower_id.value(),
                "Ignoring AppendEntries response because node is not leader"
            );

            return Ok(Vec::new());
        }

        // Some else become leader
        // A response from a newer term means this leader has stale
        // state and must step down before processing the response.
        if response.term > self.current_term() {
            self.step_down_for_newer_term(response.term)?;
            return Ok(Vec::new());
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

            return Ok(Vec::new());
        }

        let leader = match self.leader.as_mut() {
            Some(leader) => leader,
            None => {
                tracing::warn!(
                    server_id = self.id.value(),
                    "Leader state missing while handling response"
                );

                return Ok(Vec::new());
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

        Ok(Vec::new())
    }
}