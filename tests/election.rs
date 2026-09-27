use rustyraft::raft::election::{ElectionState, is_log_up_to_date};
use rustyraft::raft::{LogIndex, ServerId, Term};

#[test]
fn candidate_votes_for_itself() {
    let candidate = ServerId::new(1);

    let election = ElectionState::new(candidate);

    assert_eq!(election.vote_count(), 1);
    assert!(election.has_vote_from(candidate));
}

#[test]
fn duplicate_vote_is_not_counted_twice() {
    let candidate = ServerId::new(1);
    let voter = ServerId::new(2);

    let mut election = ElectionState::new(candidate);

    election.record_vote(voter);
    election.record_vote(voter);

    assert_eq!(election.vote_count(), 2);
}

#[test]
fn three_votes_are_majority_of_five() {
    let mut election = ElectionState::new(ServerId::new(1));

    election.record_vote(ServerId::new(2));
    election.record_vote(ServerId::new(3));

    assert_eq!(election.vote_count(), 3);
    assert!(election.has_majority(5));
}

#[test]
fn two_votes_are_not_majority_of_five() {
    let mut election = ElectionState::new(ServerId::new(1));

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
