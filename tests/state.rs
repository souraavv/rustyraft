use rustyraft::raft::state::{LogIndex, Role, ServerId, Term};

#[test]
fn term_starts_at_zero() {
    assert_eq!(Term::ZERO.value(), 0);
}

#[test]
fn term_can_be_created() {
    let term = Term::new(10);

    assert_eq!(term.value(), 10);
}

#[test]
fn term_next_increments() {
    let term = Term::new(10);

    assert_eq!(term.next().value(), 11);
}

#[test]
#[should_panic(expected = "Raft term exhausted")]
fn term_does_not_wrap() {
    Term::new(u64::MAX).next();
}

#[test]
fn log_index_starts_at_zero() {
    assert_eq!(LogIndex::ZERO.value(), 0);
}

#[test]
fn log_index_next_increments() {
    let index = LogIndex::new(10);

    assert_eq!(index.next().value(), 11);
}

#[test]
#[should_panic(expected = "Raft log index exhausted")]
fn log_index_does_not_wrap() {
    LogIndex::new(u64::MAX).next();
}

#[test]
fn server_id_is_stable() {
    let id = ServerId::new(42);

    assert_eq!(id.value(), 42);
}

#[test]
fn follower_is_default_role() {
    assert_eq!(Role::default(), Role::Follower);
}

#[test]
fn terms_are_ordered() {
    assert!(Term::new(1) < Term::new(2));
    assert!(Term::new(2) > Term::new(1));
}

#[test]
fn log_indexes_are_ordered() {
    assert!(LogIndex::new(1) < LogIndex::new(2));
}
