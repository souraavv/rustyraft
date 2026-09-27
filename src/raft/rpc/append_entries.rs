use crate::raft::log::LogEntry;
use crate::raft::state::{LogIndex, ServerId, Term};

/// Request sent by a leader to replicate log entries to a follower
///
/// An empty entries vector represents the heartbeat
#[derive(Debug)]
pub struct AppendEntriesRequest<C> {
    pub term: Term,
    pub leader_id: ServerId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry<C>>,
    pub leader_commit: LogIndex,
}

/// Response to append entry RPC
#[derive(Debug)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub success: bool,
}

impl<C> AppendEntriesRequest<C> {
    pub fn new(
        term: Term,
        leader_id: ServerId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<LogEntry<C>>,
        leader_commit: LogIndex,
    ) -> Self {
        Self {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        }
    }

    /// heartbeat
    pub fn heartbeat(
        term: Term,
        leader_id: ServerId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        leader_commit: LogIndex,
    ) -> Self {
        Self {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries: Vec::new(),
            leader_commit,
        }
    }
}

impl AppendEntriesResponse {
    pub fn success(term: Term) -> Self {
        Self {
            term,
            success: true,
        }
    }

    pub fn failure(term: Term) -> Self {
        Self {
            term,
            success: false,
        }
    }
}
