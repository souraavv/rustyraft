use rustyraft::raft::rpc::{AppendEntriesRequest, AppendEntriesResponse};
use rustyraft::raft::{LogEntry, LogIndex, ServerId, Term};

#[test]
fn append_entries_request_contains_replication_state() {
    let entries = vec![LogEntry::new(Term::new(2), "command")];

    let request = AppendEntriesRequest::new(
        Term::new(2),
        ServerId::new(1),
        LogIndex::new(3),
        Term::new(1),
        entries,
        LogIndex::new(2),
    );

    assert_eq!(request.term, Term::new(2));
    assert_eq!(request.leader_id, ServerId::new(1));
    assert_eq!(request.prev_log_index, LogIndex::new(3));
    assert_eq!(request.prev_log_term, Term::new(1));
    assert_eq!(request.entries.len(), 1);
    assert_eq!(request.leader_commit, LogIndex::new(2));
}

#[test]
fn heartbeat_contains_no_entries() {
    let request = AppendEntriesRequest::<String>::heartbeat(
        Term::new(2),
        ServerId::new(1),
        LogIndex::new(3),
        Term::new(1),
        LogIndex::new(3),
    );

    assert!(request.entries.is_empty());
}

#[test]
fn successful_response_sets_success() {
    let response = AppendEntriesResponse::success(Term::new(2));

    assert_eq!(response.term, Term::new(2));
    assert!(response.success);
}

#[test]
fn failed_response_sets_failure() {
    let response = AppendEntriesResponse::failure(Term::new(2));

    assert_eq!(response.term, Term::new(2));
    assert!(!response.success);
}
