use rustyraft::raft::replication::{
    FollowerProgress,
    ReplicationState,
};
use rustyraft::raft::{
    LogIndex,
    ServerId,
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