use rustyraft::raft::election::{
    ElectionState,
    is_log_up_to_date,
};

use rustyraft::raft::{
    ElectionTimer, LogIndex, ServerId, Term, HeartbeatTimer,
};

#[test]
fn candidate_votes_for_itself() {
    let candidate = ServerId::new(1);
    let election = ElectionState::new(
        candidate,
        Term::new(1),
    );

    assert_eq!(election.vote_count(), 1);
    assert!(election.has_vote_from(candidate));
}

#[test]
fn election_records_candidate_and_term() {
    let candidate = ServerId::new(1);
    let term = Term::new(3);

    let election = ElectionState::new(
        candidate,
        term,
    );

    assert_eq!(election.candidate_id(), candidate);
    assert_eq!(election.term(), term);
}

#[test]
fn duplicate_vote_is_not_counted_twice() {
    let candidate = ServerId::new(1);
    let voter = ServerId::new(2);

    let mut election = ElectionState::new(
        candidate,
        Term::new(1),
    );

    election.record_vote(voter);
    election.record_vote(voter);

    assert_eq!(election.vote_count(), 2);
}

#[test]
fn three_votes_are_majority_of_five() {
    let mut election = ElectionState::new(
        ServerId::new(1),
        Term::new(1),
    );

    election.record_vote(ServerId::new(2));
    election.record_vote(ServerId::new(3));

    assert_eq!(election.vote_count(), 3);
    assert!(election.has_majority(5));
}

#[test]
fn two_votes_are_not_majority_of_five() {
    let mut election = ElectionState::new(
        ServerId::new(1),
        Term::new(1),
    );

    election.record_vote(ServerId::new(2));

    assert_eq!(election.vote_count(), 2);
    assert!(!election.has_majority(5));
}

#[test]
fn higher_last_log_term_is_more_up_to_date() {
    assert!(is_log_up_to_date(
        LogIndex::new(5),
        Term::new(3),
        LogIndex::new(100),
        Term::new(2),
    ));
}

#[test]
fn lower_last_log_term_is_not_more_up_to_date() {
    assert!(!is_log_up_to_date(
        LogIndex::new(100),
        Term::new(2),
        LogIndex::new(5),
        Term::new(3),
    ));
}

#[test]
fn same_term_longer_log_is_more_up_to_date() {
    assert!(is_log_up_to_date(
        LogIndex::new(10),
        Term::new(3),
        LogIndex::new(5),
        Term::new(3),
    ));
}

#[test]
fn same_term_shorter_log_is_not_more_up_to_date() {
    assert!(!is_log_up_to_date(
        LogIndex::new(5),
        Term::new(3),
        LogIndex::new(10),
        Term::new(3),
    ));
}

#[test]
fn identical_logs_are_equally_up_to_date() {
    assert!(is_log_up_to_date(
        LogIndex::new(10),
        Term::new(3),
        LogIndex::new(10),
        Term::new(3),
    ));
}

#[test]
fn election_timer_does_not_expire_before_timeout() {
    let mut timer = ElectionTimer::new(3);

    timer.tick();
    timer.tick();

    assert!(!timer.expired());
    assert_eq!(timer.elapsed_ticks(), 2);
}

#[test]
fn election_timer_expires_at_timeout() {
    let mut timer = ElectionTimer::new(3);

    timer.tick();
    timer.tick();
    timer.tick();

    assert!(timer.expired());
    assert_eq!(timer.elapsed_ticks(), 3);
}

#[test]
fn election_timer_can_be_reset() {
    let mut timer = ElectionTimer::new(3);

    timer.tick();
    timer.tick();

    timer.reset();

    assert!(!timer.expired());
    assert_eq!(timer.elapsed_ticks(), 0);
}

#[test]
fn election_timer_remains_expired_after_timeout() {
    let mut timer = ElectionTimer::new(2);

    timer.tick();
    timer.tick();
    timer.tick();

    assert!(timer.expired());
    assert_eq!(timer.elapsed_ticks(), 3);
}

#[test]
#[should_panic(expected = "Election timeout must be greater than zero")]
fn election_timer_rejects_zero_timeout() {
    ElectionTimer::new(0);
}


#[test]
fn heartbeat_timer_does_not_expire_before_interval() {
    let mut timer = HeartbeatTimer::new(3);

    timer.tick();
    timer.tick();

    assert!(!timer.expired());
    assert_eq!(timer.elapsed_ticks(), 2);
}

#[test]
fn heartbeat_timer_expires_at_interval() {
    let mut timer = HeartbeatTimer::new(3);

    timer.tick();
    timer.tick();
    timer.tick();

    assert!(timer.expired());
    assert_eq!(timer.elapsed_ticks(), 3);
}

#[test]
fn heartbeat_timer_can_be_reset() {
    let mut timer = HeartbeatTimer::new(3);

    timer.tick();
    timer.tick();

    timer.reset();

    assert!(!timer.expired());
    assert_eq!(timer.elapsed_ticks(), 0);
}

#[test]
fn heartbeat_timer_remains_expired_after_interval() {
    let mut timer = HeartbeatTimer::new(2);

    timer.tick();
    timer.tick();
    timer.tick();

    assert!(timer.expired());
    assert_eq!(timer.elapsed_ticks(), 3);
}

#[test]
#[should_panic(
    expected = "Heartbeat interval must be greater than zero"
)]
fn heartbeat_timer_rejects_zero_interval() {
    HeartbeatTimer::new(0);
}