use rustyraft::raft::{
    RaftNode,
    Role,
    ServerId,
    LogIndex,
    Term,
};
use rustyraft::raft::rpc::{
    RequestVoteRequest,
    RequestVoteResponse,
};

#[test]
fn request_vote_rejects_older_term() {
    let mut node = RaftNode::<String>::new(
        ServerId::new(1),
    );

    // Move the node to term 2.
    let request = RequestVoteRequest::new(
        Term::new(2),
        ServerId::new(2),
        LogIndex::ZERO,
        Term::ZERO,
    );

    let response = node.handle_request_vote(request);

    assert!(response.vote_granted);
    assert_eq!(node.current_term(), Term::new(2));

    // Term 1 is now older than the node's current term 2.
    let request = RequestVoteRequest::new(
        Term::new(1),
        ServerId::new(3),
        LogIndex::ZERO,
        Term::ZERO,
    );

    let response = node.handle_request_vote(request);

    assert!(!response.vote_granted);
    assert_eq!(response.term, Term::new(2));
    assert_eq!(node.current_term(), Term::new(2));
}


#[test]
fn request_vote_updates_to_higher_term() {
    let mut node = RaftNode::<String>::new(ServerId::new(1));

    let request = RequestVoteRequest::new(
        Term::new(3),
        ServerId::new(2),
        LogIndex::ZERO,
        Term::ZERO,
    );

    let response = node.handle_request_vote(request);

    assert_eq!(node.current_term(), Term::new(3));
    assert_eq!(node.role(), Role::Follower);
    assert!(response.vote_granted);
}

#[test]
fn request_vote_records_granted_vote() {
    let mut node = RaftNode::<String>::new(ServerId::new(1));

    let candidate = ServerId::new(2);

    let request = RequestVoteRequest::new(
        Term::new(1),
        candidate,
        LogIndex::ZERO,
        Term::ZERO,
    );

    let response = node.handle_request_vote(request);

    assert!(response.vote_granted);
    assert_eq!(node.voted_for(), Some(candidate));
}

#[test]
fn request_vote_rejects_different_candidate_after_voting() {
    let mut node = RaftNode::<String>::new(ServerId::new(1));

    let first_candidate = ServerId::new(2);
    let second_candidate = ServerId::new(3);

    let first_request = RequestVoteRequest::new(
        Term::new(1),
        first_candidate,
        LogIndex::ZERO,
        Term::ZERO,
    );

    let second_request = RequestVoteRequest::new(
        Term::new(1),
        second_candidate,
        LogIndex::ZERO,
        Term::ZERO,
    );

    assert!(
        node.handle_request_vote(first_request)
            .vote_granted
    );

    assert!(
        !node
            .handle_request_vote(second_request)
            .vote_granted
    );

    assert_eq!(
        node.voted_for(),
        Some(first_candidate)
    );
}

#[test]
fn request_vote_allows_same_candidate_again() {
    let mut node = RaftNode::<String>::new(ServerId::new(1));

    let candidate = ServerId::new(2);

    let request = RequestVoteRequest::new(
        Term::new(1),
        candidate,
        LogIndex::ZERO,
        Term::ZERO,
    );

    assert!(
        node.handle_request_vote(request).vote_granted
    );

    let request = RequestVoteRequest::new(
        Term::new(1),
        candidate,
        LogIndex::ZERO,
        Term::ZERO,
    );

    assert!(
        node.handle_request_vote(request).vote_granted
    );
}

#[test]
fn request_vote_rejects_candidate_with_older_log() {
    let mut node = RaftNode::<String>::new(ServerId::new(1));

    // TODO: Add local log entries once node log mutation is exposed.

    let request = RequestVoteRequest::new(
        Term::new(1),
        ServerId::new(2),
        LogIndex::ZERO,
        Term::ZERO,
    );

    let response = node.handle_request_vote(request);

    assert!(response.vote_granted);
}

#[test]
fn start_election_increments_term() {
    let mut node = RaftNode::<String>::new(
        ServerId::new(1),
    );

    node.start_election();

    assert_eq!(
        node.current_term(),
        Term::new(1),
    );
}

#[test]
fn start_election_makes_node_candidate() {
    let mut node = RaftNode::<String>::new(
        ServerId::new(1),
    );

    node.start_election();

    assert_eq!(
        node.role(),
        Role::Candidate,
    );
}

#[test]
fn start_election_votes_for_self() {
    let mut node = RaftNode::<String>::new(
        ServerId::new(1),
    );

    node.start_election();

    assert_eq!(
        node.voted_for(),
        Some(ServerId::new(1)),
    );
}

#[test]
fn start_election_creates_election_state() {
    let mut node = RaftNode::<String>::new(
        ServerId::new(1),
    );

    node.start_election();

    let election = node
        .election()
        .expect("Election state should exist");

    assert_eq!(
        election.candidate_id(),
        ServerId::new(1),
    );

    assert_eq!(
        election.term(),
        Term::new(1),
    );

    assert_eq!(
        election.vote_count(),
        1,
    );
}

#[test]
fn starting_another_election_increments_term_again() {
    let mut node = RaftNode::<String>::new(
        ServerId::new(1),
    );

    node.start_election();

    assert_eq!(
        node.current_term(),
        Term::new(1),
    );

    node.start_election();

    assert_eq!(
        node.current_term(),
        Term::new(2),
    );

    assert_eq!(
        node.role(),
        Role::Candidate,
    );
}

#[test]
fn candidate_stays_candidate_without_majority() {
    let mut node = RaftNode::<String>::new(
        ServerId::new(1),
    );

    let servers = vec![
        ServerId::new(1),
        ServerId::new(2),
        ServerId::new(3),
        ServerId::new(4),
        ServerId::new(5),
    ];

    node.start_election();

    let response = RequestVoteResponse::granted(
        Term::new(1),
    );

    node.handle_request_vote_response(
        ServerId::new(2),
        response,
        &servers,
    );

    assert_eq!(node.role(), Role::Candidate);
}

#[test]
fn candidate_becomes_leader_after_majority() {
    let mut node = RaftNode::<String>::new(
        ServerId::new(1),
    );

    let servers = vec![
        ServerId::new(1),
        ServerId::new(2),
        ServerId::new(3),
        ServerId::new(4),
        ServerId::new(5),
    ];

    node.start_election();

    node.handle_request_vote_response(
        ServerId::new(2),
        RequestVoteResponse::granted(Term::new(1)),
        &servers,
    );

    assert_eq!(node.role(), Role::Candidate);

    node.handle_request_vote_response(
        ServerId::new(3),
        RequestVoteResponse::granted(Term::new(1)),
        &servers,
    );

    assert_eq!(node.role(), Role::Leader);
}

#[test]
fn duplicate_vote_does_not_make_candidate_leader() {
    let mut node = RaftNode::<String>::new(
        ServerId::new(1),
    );

    let servers = vec![
        ServerId::new(1),
        ServerId::new(2),
        ServerId::new(3),
        ServerId::new(4),
        ServerId::new(5),
    ];

    node.start_election();

    let response = RequestVoteResponse::granted(
        Term::new(1),
    );

    node.handle_request_vote_response(
        ServerId::new(2),
        response,
        &servers,
    );

    assert_eq!(node.role(), Role::Candidate);
}

#[test]
fn higher_term_vote_response_makes_candidate_follower() {
    let mut node = RaftNode::<String>::new(
        ServerId::new(1),
    );

    let servers = vec![
        ServerId::new(1),
        ServerId::new(2),
        ServerId::new(3),
    ];

    node.start_election();

    node.handle_request_vote_response(
        ServerId::new(2),
        RequestVoteResponse::granted(Term::new(2)),
        &servers,
    );

    assert_eq!(node.role(), Role::Follower);
    assert_eq!(node.current_term(), Term::new(2));
    assert_eq!(node.voted_for(), None);
}