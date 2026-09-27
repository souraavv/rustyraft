//! Raft log replication state and decisions
//! 
//! This module contains the leader-side replication logic
//! 
//! This layer determines what leader needs to send and how replication
//! state changes after a follower responds
//! 

use crate::raft::state::{LogIndex, ServerId};

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
    pub fn record_success(&mut self, replicated_index: LogIndex) {
        if replicated_index > self.match_index {
            self.match_index = replicated_index;
        }

        let next_index = replicated_index.next();

        if next_index > self.next_index {
            self.next_index = next_index;
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

    /// Returns the next index that should be sent to the specified
    /// follower.
    pub fn next_index(
        &self,
        server_id: ServerId,
    ) -> Option<LogIndex> {
        self.progress(server_id)
            .map(|progress| progress.next_index)
    }
}
