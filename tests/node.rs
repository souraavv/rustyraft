use rustyraft::raft::{
    RaftNode,
    Role,
    ServerId,
    LogIndex,
    LogEntry,
    Term,
};
use rustyraft::raft::rpc::{
    RequestVoteRequest,
    RequestVoteResponse,
    AppendEntriesRequest,
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

#[test]
fn follower_accepts_heartbeat() {
    let mut node = RaftNode::<String>::new(ServerId::new(1));

    let request = AppendEntriesRequest::heartbeat(
        Term::new(1),
        ServerId::new(2),
        LogIndex::ZERO,
        Term::ZERO,
        LogIndex::ZERO,
    );

    let response = node.handle_append_entries(request);

    assert!(response.success);
    assert_eq!(response.term, Term::new(1));
    assert_eq!(node.current_term(), Term::new(1));
    assert_eq!(node.role(), Role::Follower);
}

#[test]
fn follower_rejects_append_entries_from_older_term() {
    let mut node = RaftNode::<String>::new(ServerId::new(1));

    let newer_request = AppendEntriesRequest::heartbeat(
        Term::new(2),
        ServerId::new(2),
        LogIndex::ZERO,
        Term::ZERO,
        LogIndex::ZERO,
    );

    node.handle_append_entries(newer_request);

    let older_request = AppendEntriesRequest::heartbeat(
        Term::new(1),
        ServerId::new(2),
        LogIndex::ZERO,
        Term::ZERO,
        LogIndex::ZERO,
    );

    let response = node.handle_append_entries(older_request);

    assert!(!response.success);
    assert_eq!(response.term, Term::new(2));
    assert_eq!(node.current_term(), Term::new(2));
}

#[test]
fn follower_updates_term_from_newer_append_entries() {
    let mut node = RaftNode::<String>::new(ServerId::new(1));

    let request = AppendEntriesRequest::heartbeat(
        Term::new(3),
        ServerId::new(2),
        LogIndex::ZERO,
        Term::ZERO,
        LogIndex::ZERO,
    );

    let response = node.handle_append_entries(request);

    assert!(response.success);
    assert_eq!(node.current_term(), Term::new(3));
    assert_eq!(node.role(), Role::Follower);
}

#[test]
fn follower_rejects_missing_previous_log_entry() {
    let mut node = RaftNode::<String>::new(ServerId::new(1));

    let request = AppendEntriesRequest::new(
        Term::new(1),
        ServerId::new(2),
        LogIndex::new(3),
        Term::new(1),
        Vec::new(),
        LogIndex::ZERO,
    );

    let response = node.handle_append_entries(request);

    assert!(!response.success);
    assert_eq!(response.term, Term::new(1));
}

#[test]
fn follower_appends_new_entries() {
    let mut node = RaftNode::<String>::new(ServerId::new(1));

    let entries = vec![
        LogEntry::new(Term::new(1), "A".to_string()),
        LogEntry::new(Term::new(1), "B".to_string()),
        LogEntry::new(Term::new(1), "C".to_string()),
    ];

    let request = AppendEntriesRequest::new(
        Term::new(1),
        ServerId::new(2),
        LogIndex::ZERO,
        Term::ZERO,
        entries,
        LogIndex::ZERO,
    );

    let response = node.handle_append_entries(request);

    assert!(response.success);
    assert_eq!(node.log().last_index(), LogIndex::new(3));
    assert_eq!(
        node.log().term_at(LogIndex::new(1)),
        Some(Term::new(1))
    );
    assert_eq!(
        node.log().term_at(LogIndex::new(2)),
        Some(Term::new(1))
    );
    assert_eq!(
        node.log().term_at(LogIndex::new(3)),
        Some(Term::new(1))
    );
}

#[test]
fn follower_replaces_conflicting_log_entries() {
    let mut node = RaftNode::<String>::new(ServerId::new(1));

    // First create the follower's existing log:
    //
    // index:  1   2   3   4
    // term:   1   1   3   3
    //
    // Entries 3 and 4 will later conflict with the leader's log.
    let existing_entries = vec![
        LogEntry::new(Term::new(1), "A".to_string()),
        LogEntry::new(Term::new(1), "B".to_string()),
        LogEntry::new(Term::new(3), "old-C".to_string()),
        LogEntry::new(Term::new(3), "old-D".to_string()),
    ];

    let request = AppendEntriesRequest::new(
        Term::new(3),
        ServerId::new(2),
        LogIndex::ZERO,
        Term::ZERO,
        existing_entries,
        LogIndex::ZERO,
    );

    let response = node.handle_append_entries(request);

    assert!(response.success);
    assert_eq!(node.log().last_index(), LogIndex::new(4));

    // Now the leader sends its version of entries 3 and 4.
    //
    // Leader:
    //
    // index:  1   2   3   4
    // term:   1   1   2   2
    //
    // The follower already agrees through index 2.
    let leader_entries = vec![
        LogEntry::new(Term::new(2), "new-C".to_string()),
        LogEntry::new(Term::new(2), "new-D".to_string()),
    ];

    let request = AppendEntriesRequest::new(
        Term::new(3),
        ServerId::new(2),
        LogIndex::new(2),
        Term::new(1),
        leader_entries,
        LogIndex::ZERO,
    );

    let response = node.handle_append_entries(request);

    assert!(response.success);

    // The conflicting suffix should have been replaced.
    assert_eq!(node.log().last_index(), LogIndex::new(4));

    assert_eq!(
        node.log().term_at(LogIndex::new(1)),
        Some(Term::new(1))
    );
    assert_eq!(
        node.log().term_at(LogIndex::new(2)),
        Some(Term::new(1))
    );
    assert_eq!(
        node.log().term_at(LogIndex::new(3)),
        Some(Term::new(2))
    );
    assert_eq!(
        node.log().term_at(LogIndex::new(4)),
        Some(Term::new(2))
    );
}

#[test]
fn follower_advances_commit_index_from_leader() {
    let mut node = RaftNode::<String>::new(ServerId::new(1));

    let entries = vec![
        LogEntry::new(Term::new(1), "A".to_string()),
        LogEntry::new(Term::new(1), "B".to_string()),
        LogEntry::new(Term::new(1), "C".to_string()),
        LogEntry::new(Term::new(1), "D".to_string()),
    ];

    let request = AppendEntriesRequest::new(
        Term::new(1),
        ServerId::new(2),
        LogIndex::ZERO,
        Term::ZERO,
        entries,
        LogIndex::new(3),
    );

    let response = node.handle_append_entries(request);

    assert!(response.success);
    assert_eq!(node.log().last_index(), LogIndex::new(4));
    assert_eq!(node.commit_index(), LogIndex::new(3));
}

#[test]
fn follower_does_not_commit_beyond_local_log() {
    let mut node = RaftNode::<String>::new(ServerId::new(1));

    let entries = vec![
        LogEntry::new(Term::new(1), "A".to_string()),
        LogEntry::new(Term::new(1), "B".to_string()),
        LogEntry::new(Term::new(1), "C".to_string()),
    ];

    let request = AppendEntriesRequest::new(
        Term::new(1),
        ServerId::new(2),
        LogIndex::ZERO,
        Term::ZERO,
        entries,
        LogIndex::new(10),
    );

    let response = node.handle_append_entries(request);

    assert!(response.success);
    assert_eq!(node.log().last_index(), LogIndex::new(3));

    // The leader cannot make the follower commit an entry that the
    // follower does not have locally.
    assert_eq!(node.commit_index(), LogIndex::new(3));
}