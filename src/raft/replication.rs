//! Raft log replication state and decisions
//! 
//! This module contains the leader-side replication logic
//! 
//! This layer determines what leader needs to send and how replication
//! state changes after a follower responds
//! 

use crate::raft::{
    LogEntry, RaftLog,
};

use crate::raft::rpc::{
    AppendEntriesRequest, AppendEntriesResponse,
};

use crate::raft::state::{
    LogIndex, 
    ServerId,
    Term,
};

/// Replication progress of a follower
/// 
/// next_index - is the next log entry the leader should send to the follower
/// match_index - is the highest log entry known to be replicated on the
/// follower
#[derive(Debug, Clone, Copy)]
pub struct FollowerProgress {
    pub next_index: LogIndex,
    pub match_index: LogIndex,
}

impl FollowerProgress {
    /// Create replication state for a follower
    pub fn new(next_index: LogIndex) -> Self {
        Self {
            next_index,
            match_index: LogIndex::ZERO,
        }
    }

    /// Record that the follower successfully replicated through 
    /// the given index
    /// 
    /// the follower's match index can only move forward
    /// 
    /// e.g. if leader sends a replicated index = 5, first i will see
    /// if my matching index was < 5, then I will also move to 5
    /// And then we will compute the next index and based on replication_idx
    /// if current index is less than that then we will move to that, else
    /// ignore, keep it higher anyway we will override.
    /// All leader cares is the next index.
    /// 
    /// match index - what is the highest entry I know this follower has ?
    /// next index - What entry should I try sending next ?
    pub fn record_success(&mut self, replicated_index: LogIndex) {
        // handle the network dealyed packets i.e., ingore if the replicated
        // index < match_index
        if replicated_index > self.match_index {
            self.match_index = replicated_index;
        }

        // temporary
        let new_next_index = replicated_index.next();

        // Safely from delayed old packets - tolerant to those
        if new_next_index > self.next_index {
            self.next_index = new_next_index;
        }
    }

    /// Move the next index backwards after a replication failure
    /// 
    /// The index can never move before the position preceding the first
    /// log entry
    pub fn record_failure(&mut self) {
        if self.next_index > LogIndex::ZERO {
            self.next_index = LogIndex::new(self.next_index.value() - 1);
        }
    }
}

/// Leader side replication state
/// 
/// Leader holds the follower progress 
#[derive(Debug)]
pub struct ReplicationState {
    progress: Vec<(ServerId, FollowerProgress)>,
}

impl ReplicationState {

    pub fn new(
        followers: &[ServerId],
        leader_last_index: LogIndex,
    ) -> Self {
        
        let next_index = leader_last_index.next();

        let progress = followers
            .iter()
            .copied()
            .map(|server_id| {
                (server_id, FollowerProgress::new(next_index))
            })
            .collect();

        Self {
            progress
        }
    }

    /// Returns replication progress for a follower.
    pub fn progress(
        &self,
        server_id: ServerId, 
    ) -> Option<FollowerProgress> {
        self.progress
            .iter()
            .find(|(id, _)| *id == server_id)
            .map(|(_, progress)| *progress) 
    }

    /// Creates an AppendEntries request for a follower
    /// 
    /// next_index identifies the first entry the follower is expected to 
    /// be missing. The entry immediate before it is used as the consistency
    /// check 
    pub fn build_append_entries<C: Clone>(
        &self, 
        server_id: ServerId,  // follower ID
        leader_id: ServerId,
        term: Term, 
        log: &RaftLog<C>,
        leader_commit: LogIndex,
    ) -> Option<AppendEntriesRequest<C>> {

        let progress = self.progress(server_id)?;
        let next_index = progress.next_index;

        let prev_log_index = if next_index == LogIndex::ZERO {
            LogIndex::ZERO
        } else {
            LogIndex::new(
                next_index.value() - 1
            )
        };

        let prev_log_term = log
            .term_at(prev_log_index)
            .unwrap_or(Term::ZERO);

        let entries: Vec<LogEntry<C>> = log
            .iter()
            .skip(next_index.value().saturating_sub(1) as usize)
            .cloned()
            .collect();

        tracing::debug!(
            leader_id = leader_id.value(),
            follower_id = server_id.value(),
            term = term.value(),
            next_index = next_index.value(),
            prev_log_index = prev_log_index.value(),
            entry_count = entries.len(),
            "Built AppendEntries request"
        );

        Some(AppendEntriesRequest::new(
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        ))
    }

    ///
    /// When a server (likely a follower) we have response 
    /// 
    pub fn handle_response(
        &mut self, 
        server_id: ServerId,
        response: &AppendEntriesResponse,
        replicated_index: LogIndex,
    ) -> bool {
        if response.success {
            tracing::debug!(
                server_id = server_id.value(),
                replicated_index = replicated_index.value(),
                "AppendEntries succeeded"
            );

            // basically updating the match_index and next_index
            self.record_success(
                server_id, 
                replicated_index,
            )
        } else {
            tracing::debug!(
                server_id = server_id.value(),
                "AppendEntries failed; backing up next_index"
            );

            self.record_failure(server_id)
        }
    }

    /// Records a successful AppendEntries response.
    pub fn record_success(
        &mut self, 
        server_id: ServerId,
        replicated_index: LogIndex,
    ) -> bool {
        match self
            .progress
            .iter_mut()
            .find(|(id, _)| *id == server_id) {
                Some((_, progress)) => {
                    progress.record_success(replicated_index);
                    true
                }
                None => false,
            }
    }

    /// Records a failed AppendEntries response.
    pub fn record_failure(
        &mut self,
        server_id: ServerId,
    ) -> bool {
        match self
            .progress
            .iter_mut()
            .find(|(id, _)| *id == server_id) {
                Some((_, progress)) => {
                    progress.record_failure();
                    true
                }
                None => false,
            }
    }

    // --- helpers (getters) ----

    /// Returns the highest index replicated on the specified follower.
    pub fn match_index(
        &self,
        server_id: ServerId,
    ) -> Option<LogIndex> {
        self.progress(server_id)
            .map(|progress| progress.match_index)
    }

    /// Returns the replication position of every follower.
    ///
    /// The leader uses these positions to determine whether a log entry
    /// has been replicated on a majority of the cluster.
    pub fn match_indexes(&self) -> Vec<LogIndex> {
        self.progress
            .iter()
            .map(|(_, progress)| progress.match_index)
            .collect()
    }

    /// Returns the next index that should be sent to the specified
    /// follower.
    pub fn next_index(
        &self,
        server_id: ServerId,
    ) -> Option<LogIndex> {
        self.progress(server_id)
            .map(|progress| progress.next_index)
    }

    // '_ means anonymous lifetime - the returned iterator may borrow 
    // somethign, and that borrow lasts for at least as long as the 
    // lifetime of &self borrow - basically iterator should not outlive
    // ReplicationState borrow
    pub fn follower_ids(
        &self,
    ) -> impl Iterator<Item = ServerId> + '_ {
        self.progress
            .iter()
            .map(|(server_id, _)| *server_id)
    }
}
