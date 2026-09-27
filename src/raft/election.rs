//! Raft leader election and decision rules
//! 
//! This module intentionally does not owns
//!  - network communication
//!  - timers or sleeping
//!  - durable storage
//!  - the RaftNode
//! 
//! Keeping election decision independent from those concerns allows 
//! us to test this with different setup smoothly
//! 

use std::collection::HashSet;

use crate::raft::state::{
    LogIndex,
    Term,
    ServerId,
};

/// State maintained while a server is participating in an election
/// 
/// A candidate vote for itself when starting an election and then collect
/// votes from other servers

#[derive(Debug)]
pub struct ElectionState {
    votes_recieved: HashSet<ServerId>,
}

impl ElectionState {
    /// Start a new election
    /// 
    /// The candidate immediately records its own vote
    pub fn new(candidate_id: ServerId) -> Self {
        let mut votes_recieved = HashSet::new();
        votes_recieved.insert(candidate_id);

        Self {
            votes_recieved
        }
    }

    /// Records a vote recieved from another server
    /// 
    /// tolerant to duplicate votes bcz of hashSet
    pub fn record_vote(&mut self, server_id: ServerId) {
        self.votes_recieved.insert(server_id);
    }

    /// total vote count
    pub fn vote_count(&self) -> usize {
        self.votes_recieved.len()
    }

    /// Returns true when candidate has recieved votes from a majority of
    /// the cluster
    pub fn has_majority(&self, cluster_size: usize) -> bool {
        self.vote_count() > cluster_size / 2
    }

    /// Returns true if this server has already voted.
    pub fn has_vote_from(&self, server_id: ServerId) -> bool {
        self.votes_recieved.contains(&server_id)
    }

}


/// Determine whether a candidate's log is at least as up-to-date as
/// recievers log
/// 
/// We compare that by last term entries, but if they match then tie is
/// break using the length of the log entries i.e., the last_index in the log
pub fn is_log_up_to_date(
    candidate_last_index: LogIndex,
    candidate_last_term: Term, 
    local_last_index: LogIndex,
    local_last_term: Term,
) -> bool {
    if candidate_last_term > local_last_term {
        return true;
    }

    if candidate_last_term < local_last_term {
        return false;
    }

    candidate_last_index >= local_last_index
}
