//! Raft commit index
//!
//! the leader can advance commit_index when an entry from its current term
//! has been replicated on a majority of servers
//!
//! Commitment is kept separate from the log replication

use crate::raft::state::{LogIndex, Term};

/// Determine the highest log index that can be commited
///
/// `match_index` contains the highest log index known to be replicated on
/// each server including the leader
///
/// An entry is eligible for commit when:
///  1. It is replicated on majority
///  2. The entry belongs to current term
///
/// The function will return the highest such eligible index greater than
/// current commit
/// 
/// We can commit an entry if it present on the majority
/// 
/// We are passing the match_index of each follower (from the FollowerState)
/// 
pub fn find_commit_index(
    current_commit: LogIndex,
    current_term: Term,
    match_indexes: &[LogIndex],
    term_at: impl Fn(LogIndex) -> Option<Term>,
) -> LogIndex {
    if match_indexes.is_empty() {
        tracing::debug!(
            current_commit = current_commit.value(),
            "No match indexes; commit index remains unchanged"
        );

        return current_commit;
    }

    let mut candidate = current_commit;

    let highest_index = match_indexes
        .iter()
        .copied()
        .max()
        .unwrap_or(LogIndex::ZERO);

    tracing::debug!(
        current_commit = current_commit.value(),
        current_term = current_term.value(),
        highest_index = highest_index.value(),
        server_count = match_indexes.len(),
        "Calculating commit index"
    );

    let mut index = highest_index;

    while index > current_commit {
        let replicated_count = match_indexes
            .iter()
            .filter(|match_index| **match_index >= index)
            .count();

        let majority = match_indexes.len() / 2 + 1;

        tracing::trace!(
            index = index.value(),
            replicated_count,
            majority,
            "Checking log index for commitment"
        );

        // If we have majority + majority on the term which I want to commit
        // because remember a leader only commit logs from its current term
        // the majority might be agreeging on a index which might 
        // be <= leaders current log index so we have to be careful

        let entry_added_by_current_leader_term: bool
            = term_at(index) == Some(current_term);

        let have_majority:bool = replicated_count >= majority;
        if have_majority && entry_added_by_current_leader_term {
            candidate = index;
            tracing::info!(
                old_commit_index = current_commit.value(),
                new_commit_index = candidate.value(),
                term = current_term.value(),
                replicated_count,
                majority,
                "Commit index advanced"
            );

            break;
        }

        index = LogIndex::new(index.value() - 1);
    }

    if candidate == current_commit {
        tracing::debug!(
            commit_index = current_commit.value(),
            "Commit index did not advance"
        );
    }

    candidate
}