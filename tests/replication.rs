use rustyraft::raft::replication::{
    FollowerProgress,
    ReplicationState,
};

use rustyraft::raft::{
    LogEntry,
    LogIndex,
    RaftLog,
    ServerId,
    Term,
};

#[test]
fn follower_progress_starts_at_given_next_index() {
    let progress =
        FollowerProgress::new(LogIndex::new(5));

    assert_eq!(
        progress.next_index,
        LogIndex::new(5)
    );

    assert_eq!(
        progress.match_index,
        LogIndex::ZERO
    );
}

#[test]
fn successful_replication_advances_progress() {
    let mut progress =
        FollowerProgress::new(LogIndex::new(5));

    progress.record_success(LogIndex::new(5));

    assert_eq!(
        progress.match_index,
        LogIndex::new(5)
    );

    assert_eq!(
        progress.next_index,
        LogIndex::new(6)
    );
}

#[test]
fn stale_success_does_not_move_match_index_backwards() {
    let mut progress =
        FollowerProgress::new(LogIndex::new(5));

    progress.record_success(LogIndex::new(5));
    progress.record_success(LogIndex::new(3));

    assert_eq!(
        progress.match_index,
        LogIndex::new(5)
    );

    assert_eq!(
        progress.next_index,
        LogIndex::new(6)
    );
}

#[test]
fn failed_replication_moves_next_index_back() {
    let mut progress =
        FollowerProgress::new(LogIndex::new(5));

    progress.record_failure();

    assert_eq!(
        progress.next_index,
        LogIndex::new(4)
    );
}

#[test]
fn failed_replication_does_not_go_below_zero() {
    let mut progress =
        FollowerProgress::new(LogIndex::ZERO);

    progress.record_failure();

    assert_eq!(
        progress.next_index,
        LogIndex::ZERO
    );
}

#[test]
fn replication_state_initializes_each_follower() {
    let followers = vec![
        ServerId::new(2),
        ServerId::new(3),
        ServerId::new(4),
    ];

    let state =
        ReplicationState::new(
            &followers,
            LogIndex::new(10),
        );

    for follower in followers {
        assert_eq!(
            state.next_index(follower),
            Some(LogIndex::new(11))
        );

        assert_eq!(
            state.match_index(follower),
            Some(LogIndex::ZERO)
        );
    }
}

#[test]
fn replication_state_updates_correct_follower() {
    let follower1 = ServerId::new(2);
    let follower2 = ServerId::new(3);

    let mut state =
        ReplicationState::new(
            &[follower1, follower2],
            LogIndex::new(10),
        );

    assert!(state.record_success(
        follower1,
        LogIndex::new(7),
    ));

    assert_eq!(
        state.match_index(follower1),
        Some(LogIndex::new(7))
    );

    assert_eq!(
        state.match_index(follower2),
        Some(LogIndex::ZERO)
    );
}

#[test]
fn unknown_follower_returns_false() {
    let mut state =
        ReplicationState::new(
            &[ServerId::new(2)],
            LogIndex::new(10),
        );

    assert!(!state.record_success(
        ServerId::new(99),
        LogIndex::new(5),
    ));

    assert!(!state.record_failure(
        ServerId::new(99),
    ));
}

#[test]
fn unknown_follower_has_no_progress() {
    let state =
        ReplicationState::new(
            &[ServerId::new(2)],
            LogIndex::new(10),
        );

    assert!(
        state.progress(ServerId::new(99)).is_none()
    );
}

fn test_log() -> RaftLog<String> {
    let mut log = RaftLog::new();

    log.append(LogEntry::new(
        Term::new(1),
        "A".to_string(),
    ));

    log.append(LogEntry::new(
        Term::new(1),
        "B".to_string(),
    ));

    log.append(LogEntry::new(
        Term::new(2),
        "C".to_string(),
    ));

    log.append(LogEntry::new(
        Term::new(2),
        "D".to_string(),
    ));

    log
}

#[test]
fn build_append_entries_sends_entries_from_next_index() {
    let follower = ServerId::new(2);
    let leader = ServerId::new(1);

    let log = test_log();

    let replication = ReplicationState::new(
        &[follower],
        log.last_index(),
    );

    let request = replication
        .build_append_entries(
            follower,
            leader,
            Term::new(2),
            &log,
            LogIndex::ZERO,
        )
        .unwrap();

    assert_eq!(
        request.prev_log_index,
        LogIndex::new(4)
    );

    assert_eq!(
        request.prev_log_term,
        Term::new(2)
    );

    assert!(request.entries.is_empty());
}

#[test]
fn build_append_entries_uses_next_index_as_start() {
    let follower = ServerId::new(2);
    let leader = ServerId::new(1);

    // At this point the leader has only entries 1 and 2.
    let mut log = RaftLog::new();

    log.append(LogEntry::new(
        Term::new(1),
        "A".to_string(),
    ));

    log.append(LogEntry::new(
        Term::new(1),
        "B".to_string(),
    ));

    // The follower is initially expected to receive entry 3.
    let replication = ReplicationState::new(
        &[follower],
        log.last_index(),
    );

    // The leader now has two additional entries.
    log.append(LogEntry::new(
        Term::new(2),
        "C".to_string(),
    ));

    log.append(LogEntry::new(
        Term::new(2),
        "D".to_string(),
    ));

    let request = replication
        .build_append_entries(
            follower,
            leader,
            Term::new(2),
            &log,
            LogIndex::new(2),
        )
        .unwrap();

    assert_eq!(
        request.prev_log_index,
        LogIndex::new(2)
    );

    assert_eq!(
        request.prev_log_term,
        Term::new(1)
    );

    assert_eq!(
        request.entries.len(),
        2
    );

    assert_eq!(
        request.entries[0].term,
        Term::new(2)
    );

    assert_eq!(
        request.entries[1].term,
        Term::new(2)
    );
}

#[test]
fn build_append_entries_returns_none_for_unknown_follower() {
    let follower = ServerId::new(2);
    let unknown = ServerId::new(3);
    let leader = ServerId::new(1);

    let log = test_log();

    let replication = ReplicationState::new(
        &[follower],
        log.last_index(),
    );

    let request = replication.build_append_entries(
        unknown,
        leader,
        Term::new(2),
        &log,
        LogIndex::ZERO,
    );

    assert!(request.is_none());
}