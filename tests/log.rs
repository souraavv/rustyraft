use rustyraft::raft::{LogEntry, LogIndex, RaftLog, Term};

#[test]
fn new_log_is_empty() {
    let log = RaftLog::<String>::new();

    assert!(log.is_empty());
    assert_eq!(log.len(), 0);
    assert_eq!(log.last_index(), LogIndex::ZERO);
    assert_eq!(log.last_term(), None);
}

#[test]
fn append_uses_one_based_indexes() {
    let mut log = RaftLog::new();

    let first = log.append(LogEntry::new(Term::new(1), "first"));

    let second = log.append(LogEntry::new(Term::new(1), "second"));

    assert_eq!(first, LogIndex::new(1));
    assert_eq!(second, LogIndex::new(2));
}

#[test]
fn get_returns_entry_at_index() {
    let mut log = RaftLog::new();

    log.append(LogEntry::new(Term::new(1), "first"));

    let entry = log.get(LogIndex::new(1)).expect("entry should exist");

    assert_eq!(entry.term, Term::new(1));
    assert_eq!(entry.command, "first");
}

#[test]
fn zero_index_is_not_a_log_entry() {
    let mut log = RaftLog::new();

    log.append(LogEntry::new(Term::new(1), "first"));

    assert!(log.get(LogIndex::ZERO).is_none());
}

#[test]
fn missing_index_returns_none() {
    let mut log = RaftLog::new();

    log.append(LogEntry::new(Term::new(1), "first"));

    assert!(log.get(LogIndex::new(2)).is_none());
}

#[test]
fn last_term_returns_last_entry_term() {
    let mut log = RaftLog::new();

    log.append(LogEntry::new(Term::new(1), "first"));

    log.append(LogEntry::new(Term::new(3), "second"));

    assert_eq!(log.last_term(), Some(Term::new(3)));
}

#[test]
fn term_at_returns_entry_term() {
    let mut log = RaftLog::new();

    log.append(LogEntry::new(Term::new(2), "command"));

    assert_eq!(log.term_at(LogIndex::new(1)), Some(Term::new(2)));
}

#[test]
fn matches_checks_index_and_term() {
    let mut log = RaftLog::new();

    log.append(LogEntry::new(Term::new(2), "command"));

    assert!(log.matches(LogIndex::new(1), Term::new(2)));

    assert!(!log.matches(LogIndex::new(1), Term::new(3)));
}

#[test]
fn truncate_removes_entry_and_everything_after_it() {
    let mut log = RaftLog::new();

    for index in 1..=5 {
        log.append(LogEntry::new(Term::new(1), index));
    }

    log.truncate_from(LogIndex::new(4));

    assert_eq!(log.len(), 3);
    assert_eq!(log.last_index(), LogIndex::new(3));
}

#[test]
fn truncate_at_zero_clears_log() {
    let mut log = RaftLog::new();

    log.append(LogEntry::new(Term::new(1), "first"));

    log.truncate_from(LogIndex::ZERO);

    assert!(log.is_empty());
}

#[test]
fn iter_returns_all_entries() {
    let mut log = RaftLog::new();

    log.append(LogEntry::new(Term::new(1), "first"));

    log.append(LogEntry::new(Term::new(2), "second"));

    let commands: Vec<&str> = log.iter().map(|entry| entry.command).collect();

    assert_eq!(commands, vec!["first", "second"]);
}
