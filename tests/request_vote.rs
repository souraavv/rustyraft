use rustyraft::raft::rpc::{RequestVoteRequest, RequestVoteResponse};
use rustyraft::raft::{LogIndex, ServerId, Term};

#[test]
fn request_vote_request_contains_candidate_state() {
    let request = RequestVoteRequest::new(
        Term::new(3),
        ServerId::new(2),
        LogIndex::new(10),
        Term::new(3),
    );

    assert_eq!(request.term, Term::new(3));
    assert_eq!(request.candidate_id, ServerId::new(2));
    assert_eq!(request.last_log_index, LogIndex::new(10));
    assert_eq!(request.last_log_term, Term::new(3));
}

#[test]
fn granted_response_grants_vote() {
    let response = RequestVoteResponse::granted(Term::new(5));

    assert_eq!(response.term, Term::new(5));
    assert!(response.vote_granted);
}

#[test]
fn rejected_response_rejects_vote() {
    let response = RequestVoteResponse::rejected(Term::new(5));

    assert_eq!(response.term, Term::new(5));
    assert!(!response.vote_granted);
}
