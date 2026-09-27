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

use std::collections::HashSet;

use crate::raft::state::{
    LogIndex, 
    ServerId, 
    Term
};

/// State maintained while a server is participating in an election
///
/// A candidate vote for itself when starting an election and then collect
/// votes from other servers
#[derive(Debug)]
pub struct ElectionState {
    candidate_id: ServerId,
    term: Term,
    votes_received: HashSet<ServerId>,
}

impl ElectionState {
    /// Start a new election
    ///
    /// The candidate immediately records its own vote
    pub fn new(
        candidate_id: ServerId,
        term: Term,
    ) -> Self {
        let mut votes_received = HashSet::new();
        votes_received.insert(candidate_id);
        tracing::info!(
            candidate_id = candidate_id.value(),
            "Started election"
        );

        Self { 
            candidate_id,
            term,
            votes_received,
        }
    }

    /// Records a vote recieved from another server
    ///
    /// tolerant to duplicate votes bcz of hashSet
    pub fn record_vote(&mut self, server_id: ServerId) {
        let inserted = self.votes_received.insert(server_id);

        if inserted {
            tracing::debug!(
                server_id = server_id.value(),
                vote_count = self.votes_received.len(),
                "Recorded vote"
            );
        } else {
            tracing::trace!(
                server_id = server_id.value(),
                "Ignored duplicate vote"
            );
        }
    }

    /// total vote count
    pub fn vote_count(&self) -> usize {
        self.votes_received.len()
    }

    /// Returns true when the candidate has received votes from a
    /// majority of the cluster.
    pub fn has_majority(
        &self, 
        cluster_size: usize
    ) -> bool {
        let majority = cluster_size / 2 + 1;
        let has_majority = self.vote_count() >= majority;

        tracing::debug!(
            vote_count = self.vote_count(),
            cluster_size,
            majority,
            has_majority,
            "Checked election majority"
        );

        has_majority
    }

    /// Returns true if this server has already voted.
    pub fn has_vote_from(
        &self, 
        server_id: ServerId
    ) -> bool {
        self.votes_received.contains(&server_id)
    }

    /// Returns the candidate running in this election.
    pub fn candidate_id(&self) -> ServerId {
        self.candidate_id
    }

    /// Returns the term of this election.
    pub fn term(&self) -> Term {
        self.term
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


/// Determine whether a server should grant its vote to a candidate
/// 
/// A vote can be granted when:
/// 
/// 1. The candidate term's is not older than the receiver's term
/// 2. The receiver has not voted for another candidate for this term
/// 3. The candidate logs are at least up-to-date as the receiver's log
/// 
/// We will only determine here.. the peristence is taken care by the Raft
/// node. 
pub fn should_grant_vote(
    current_term: Term, 
    voted_for: Option<ServerId>,
    candidate_id: ServerId, 
    candidate_term: Term, 
    candidate_last_index: LogIndex,
    candidate_last_term: Term,
    local_last_index: LogIndex,
    local_last_term: Term,
) -> bool {

    // If candiate appears with a lower terms - simply reject
    if candidate_term < current_term {
        tracing::debug!(
            current_term = current_term.value(),
            candidate_term = candidate_term.value(),
            candidate_id = candidate_id.value(),
            "Rejected vote for older term"
        );

        return false;
    }

    // Reject - I've already voted
    if let Some(voted_server) = voted_for {
        if voted_server != candidate_id {
            tracing::debug!(
                candidate_id = candidate_id.value(),
                voted_server = voted_server.value(),
                term = candidate_term.value(),
                "Rejected vote because server already voted"
            );
            return false;
        }
    }

    let log_is_up_to_date = is_log_up_to_date(
        candidate_last_index, 
        candidate_last_term, 
        local_last_index, 
        local_last_term
    );

    if !log_is_up_to_date {
        tracing::info!(
            "Logs are not up-to-date on candidate - reject"
        );
        return false;
    }

    tracing::info!(
        candidate_id = candidate_id.value(),
        candidate_term = candidate_term.value(),
        "Vote granted"
    );

    true
}

