use serde::{Deserialize, Serialize};

use crate::raft::state::{LogIndex, ServerId, Term};

/// Request send by a 'candidate' to request vote from other servers
///
/// A candidate includes
///  - current term
///  - last log entry
///  - last log term
///  - it identity
///
/// This will help the reciver determine whether they can cast a vote or not
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    pub term: Term,
    pub candidate_id: ServerId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

/// Response to the Request Vote RPC
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

impl RequestVoteRequest {
    pub fn new(
        term: Term,
        candidate_id: ServerId,
        last_log_index: LogIndex,
        last_log_term: Term,
    ) -> Self {
        Self {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        }
    }
}

impl RequestVoteResponse {
    pub fn granted(term: Term) -> Self {
        Self {
            term,
            vote_granted: true,
        }
    }

    pub fn rejected(term: Term) -> Self {
        Self {
            term,
            vote_granted: false,
        }
    }
}
