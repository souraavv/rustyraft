use std::cell::RefCell;
use std::rc::Rc;

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
    AppendEntriesResponse,
};

use rustyraft::raft::state_machine::{NoopStateMachine, StateMachine};

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
    let mut node = RaftNode::<String>::new(
        ServerId::new(1)
    );

    // Create a local log with two entries.
    let entries = vec![
        LogEntry::new(
            Term::new(1),
            "A".to_string(),
        ),
        LogEntry::new(
            Term::new(1),
            "B".to_string(),
        ),
    ];

    let append_request = AppendEntriesRequest::new(
        Term::new(1),
        ServerId::new(2),
        LogIndex::ZERO,
        Term::ZERO,
        entries,
        LogIndex::ZERO,
    );

    let append_response =
        node.handle_append_entries(append_request);

    assert!(
        append_response.success
    );

    assert_eq!(
        node.log().last_index(),
        LogIndex::new(2)
    );

    // Candidate is in a newer term but has an empty log.
    let request = RequestVoteRequest::new(
        Term::new(2),
        ServerId::new(3),
        LogIndex::ZERO,
        Term::ZERO,
    );

    let response =
        node.handle_request_vote(request);

    assert!(
        !response.vote_granted
    );

    // The node must still advance to the candidate's term.
    assert_eq!(
        node.current_term(),
        Term::new(2)
    );

    // A rejected candidate must not receive our vote.
    assert_eq!(
        node.voted_for(),
        None
    );
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

#[test]
fn leader_builds_append_entries_for_follower() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let mut leader = RaftNode::<String>::new(leader_id);

    // Start a real election rather than bypassing the election state.
    leader.start_election();

    assert_eq!(leader.role(), Role::Candidate);
    assert_eq!(leader.current_term(), Term::new(1));

    // The leader needs a majority. With two servers, two votes
    // are enough, including the candidate's own vote.
    let response =
        rustyraft::raft::rpc::RequestVoteResponse::granted(
            Term::new(1),
        );

    leader.handle_request_vote_response(
        follower_id,
        response,
        &[leader_id, follower_id],
    );

    assert_eq!(leader.role(), Role::Leader);

    let request = leader
        .build_append_entries(follower_id)
        .expect("leader should have replication state");

    assert_eq!(
        request.term,
        Term::new(1)
    );

    assert_eq!(
        request.leader_id,
        leader_id
    );

    // The leader has an empty log, so the first replication request
    // is effectively a heartbeat.
    assert_eq!(
        request.prev_log_index,
        LogIndex::ZERO
    );

    assert_eq!(
        request.prev_log_term,
        Term::ZERO
    );

    assert!(
        request.entries.is_empty()
    );
}


#[test]
fn leader_builds_append_entries_with_log_entries() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let mut leader = RaftNode::<String>::new(leader_id);

    leader.start_election();

    let response =
        rustyraft::raft::rpc::RequestVoteResponse::granted(
            Term::new(1),
        );

    leader.handle_request_vote_response(
        follower_id,
        response,
        &[leader_id, follower_id],
    );

    assert_eq!(leader.role(), Role::Leader);

    assert_eq!(
        leader.append_entry("A".to_string()),
        Some(LogIndex::new(1))
    );

    assert_eq!(
        leader.append_entry("B".to_string()),
        Some(LogIndex::new(2))
    );

    assert_eq!(
        leader.append_entry("C".to_string()),
        Some(LogIndex::new(3))
    );

    let request = leader
        .build_append_entries(follower_id)
        .expect("leader should build AppendEntries");

    assert_eq!(
        request.prev_log_index,
        LogIndex::ZERO
    );

    assert_eq!(
        request.prev_log_term,
        Term::ZERO
    );

    assert_eq!(request.entries.len(), 3);

    assert_eq!(
        request.entries[0].command,
        "A"
    );

    assert_eq!(
        request.entries[1].command,
        "B"
    );

    assert_eq!(
        request.entries[2].command,
        "C"
    );
}

#[test]
fn leader_and_follower_complete_log_replication_round() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let mut leader = RaftNode::<String>::new(leader_id);
    let mut follower = RaftNode::<String>::new(follower_id);

    // Elect the leader.
    leader.start_election();

    let vote = RequestVoteResponse::granted(
        Term::new(1),
    );

    leader.handle_request_vote_response(
        follower_id,
        vote,
        &[leader_id, follower_id],
    );

    assert_eq!(leader.role(), Role::Leader);

    // Add commands to the leader's log.
    leader.append_entry("A".to_string());
    leader.append_entry("B".to_string());
    leader.append_entry("C".to_string());

    // Build the replication request.
    let request = leader
        .build_append_entries(follower_id)
        .expect("leader should build AppendEntries");

    assert_eq!(request.entries.len(), 3);

    // Send the request directly to the follower.
    let response = follower.handle_append_entries(request);

    assert!(response.success);
    assert_eq!(
        follower.log().last_index(),
        LogIndex::new(3)
    );

    // Tell the leader that entries through index 3 were
    // successfully replicated.
    leader.handle_append_entries_response(
        follower_id,
        response,
    );

    let progress = leader
        .follower_progress(follower_id)
        .expect("leader should track follower");

    assert_eq!(
        progress.match_index,
        LogIndex::new(3)
    );

    assert_eq!(
        progress.next_index,
        LogIndex::new(4)
    );
    // The leader now knows that the follower has entry 3.
    //
    // next_index should therefore point at entry 4.
    //
    // We don't currently expose leader replication state through
    // RaftNode, so the next test will add that observation point.
}

#[test]
fn leader_backs_up_next_index_when_follower_is_missing_index() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let mut leader = RaftNode::<String>::new(leader_id);
    let mut follower = RaftNode::<String>::new(follower_id);

    leader.start_election();

    leader.handle_request_vote_response(
        follower_id,
        RequestVoteResponse::granted(Term::new(1)),
        &[leader_id, follower_id],
    );

    assert_eq!(leader.role(), Role::Leader);

    leader.append_entry("A".to_string());
    leader.append_entry("B".to_string());
    leader.append_entry("C".to_string());

    // Follower has only entries 1 and 2.
    let setup_request = AppendEntriesRequest::new(
        Term::new(1),
        leader_id,
        LogIndex::ZERO,
        Term::ZERO,
        vec![
            LogEntry::new(
                Term::new(1),
                "A".to_string(),
            ),
            LogEntry::new(
                Term::new(1),
                "B".to_string(),
            ),
        ],
        LogIndex::ZERO,
    );

    assert!(
        follower
            .handle_append_entries(setup_request)
            .success
    );

    // Simulate the leader previously learning that entries 1..3
    // were replicated. This puts next_index at 4.
    leader.handle_append_entries_response(
        follower_id,
        AppendEntriesResponse::success(
        Term::new(1),
        LogIndex::new(3),
    ),
    );

    let progress = leader
        .follower_progress(follower_id)
        .expect("leader should track follower");

    assert_eq!(
        progress.next_index,
        LogIndex::new(4)
    );

    assert_eq!(
        progress.match_index,
        LogIndex::new(3)
    );

    // Leader now assumes follower has index 3.
    let request = leader
        .build_append_entries(follower_id)
        .expect("leader should build AppendEntries");

    assert_eq!(
        request.prev_log_index,
        LogIndex::new(3)
    );

    assert_eq!(
        request.prev_log_term,
        Term::new(1)
    );

    // Follower actually only has indexes 1 and 2.
    let response =
        follower.handle_append_entries(request);

    assert!(!response.success);

    leader.handle_append_entries_response(
        follower_id,
        response,
    );

    let progress = leader
        .follower_progress(follower_id)
        .expect("leader should track follower");

    // Failure backs next_index up from 4 to 3.
    assert_eq!(
        progress.next_index,
        LogIndex::new(3)
    );

    // A failed replication must not change match_index.
    assert_eq!(
        progress.match_index,
        LogIndex::new(3)
    );
}


#[test]
fn leader_retries_append_entries_after_failure() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let mut leader = RaftNode::<String>::new(leader_id);
    let mut follower = RaftNode::<String>::new(follower_id);

    // Elect the leader while both logs are empty.
    leader.start_election();

    leader.handle_request_vote_response(
        follower_id,
        RequestVoteResponse::granted(Term::new(1)),
        &[leader_id, follower_id],
    );

    assert_eq!(leader.role(), Role::Leader);

    // Leader log:
    //
    // index:  1   2   3
    // term:   1   1   1
    leader.append_entry("A".to_string());
    leader.append_entry("B".to_string());
    leader.append_entry("C".to_string());

    // Follower has only entries 1 and 2.
    let setup_request = AppendEntriesRequest::new(
        Term::new(1),
        leader_id,
        LogIndex::ZERO,
        Term::ZERO,
        vec![
            LogEntry::new(
                Term::new(1),
                "A".to_string(),
            ),
            LogEntry::new(
                Term::new(1),
                "B".to_string(),
            ),
        ],
        LogIndex::ZERO,
    );

    assert!(
        follower
            .handle_append_entries(setup_request)
            .success
    );

    // Simulate the leader believing that index 3 was replicated.
    //
    // This gives us:
    //
    // match_index = 3
    // next_index  = 4
    leader.handle_append_entries_response(
        follower_id,
        AppendEntriesResponse::success(
        Term::new(1),
        LogIndex::new(3),
    ),
    );

    let progress = leader
        .follower_progress(follower_id)
        .expect("leader should track follower");

    assert_eq!(
        progress.next_index,
        LogIndex::new(4)
    );

    assert_eq!(
        progress.match_index,
        LogIndex::new(3)
    );

    // First attempt:
    //
    // prev_log_index = 3
    // prev_log_term  = 1
    //
    // The follower does not have index 3.
    let request = leader
        .build_append_entries(follower_id)
        .expect("leader should build AppendEntries");

    assert_eq!(
        request.prev_log_index,
        LogIndex::new(3)
    );

    let response =
        follower.handle_append_entries(request);

    assert!(!response.success);

    leader.handle_append_entries_response(
        follower_id,
        response,
    );

    // Failure backs next_index from 4 to 3.
    let progress = leader
        .follower_progress(follower_id)
        .expect("leader should track follower");

    assert_eq!(
        progress.next_index,
        LogIndex::new(3)
    );

    // The leader still remembers its previous match.
    assert_eq!(
        progress.match_index,
        LogIndex::new(3)
    );

    // Retry:
    //
    // next_index = 3
    // prev_log_index = 2
    //
    // The follower DOES have index 2 with term 1.
    let retry_request = leader
        .build_append_entries(follower_id)
        .expect("leader should build retry");

    assert_eq!(
        retry_request.prev_log_index,
        LogIndex::new(2)
    );

    assert_eq!(
        retry_request.prev_log_term,
        Term::new(1)
    );

    let retry_response =
        follower.handle_append_entries(retry_request);

    assert!(retry_response.success);

    // The retry successfully sends entry 3.
    leader.handle_append_entries_response(
        follower_id,
        retry_response,
    );

    let progress = leader
        .follower_progress(follower_id)
        .expect("leader should track follower");

    // Successful retry restores replication progress.
    assert_eq!(
        progress.match_index,
        LogIndex::new(3)
    );

    assert_eq!(
        progress.next_index,
        LogIndex::new(4)
    );

    // Follower should now have the complete leader log.
    assert_eq!(
        follower.log().last_index(),
        LogIndex::new(3)
    );

    assert_eq!(
        follower.log().term_at(LogIndex::new(3)),
        Some(Term::new(1))
    );
}


#[test]
fn leader_advances_commit_index_after_majority_replication() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let mut leader = RaftNode::<String>::new(leader_id);
    let mut follower = RaftNode::<String>::new(follower_id);

    // Elect the leader.
    leader.start_election();

    leader.handle_request_vote_response(
        follower_id,
        RequestVoteResponse::granted(Term::new(1)),
        &[leader_id, follower_id],
    );

    assert_eq!(leader.role(), Role::Leader);

    // The leader creates an entry in its current term.
    let index = leader
        .append_entry("A".to_string())
        .expect("leader should accept command");

    assert_eq!(
        index,
        LogIndex::new(1)
    );

    // Replicate the entry to the follower.
    let request = leader
        .build_append_entries(follower_id)
        .expect("leader should build AppendEntries");

    let response =
        follower.handle_append_entries(request);

    assert!(response.success);

    // Tell the leader that the follower successfully replicated
    // index 1.
    leader.handle_append_entries_response(
        follower_id,
        response,
    );

    // Leader + follower = 2/2, which is a majority.
    leader.update_commit_index();

    assert_eq!(
        leader.commit_index(),
        LogIndex::new(1)
    );
}

#[test]
fn leader_does_not_advance_commit_index_without_majority() {
    let leader_id = ServerId::new(1);
    let follower_a = ServerId::new(2);
    let follower_b = ServerId::new(3);

    let mut leader = RaftNode::<String>::new(leader_id);

    // Elect the leader. Give it one vote from each follower so that
    // the election is complete.
    leader.start_election();

    leader.handle_request_vote_response(
        follower_a,
        RequestVoteResponse::granted(Term::new(1)),
        &[leader_id, follower_a, follower_b],
    );

    assert_eq!(leader.role(), Role::Leader);

    // Append an entry in the leader's current term.
    leader.append_entry("A".to_string());

    // The leader itself has the entry, but neither follower has it.
    leader.update_commit_index();

    // 1/3 is not a majority.
    assert_eq!(
        leader.commit_index(),
        LogIndex::ZERO
    );
}

#[test]
fn leader_commits_entry_after_replicating_to_follower() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let mut leader = RaftNode::<String>::new(leader_id);
    let mut follower = RaftNode::<String>::new(follower_id);

    // Elect the leader.
    leader.start_election();

    leader.handle_request_vote_response(
        follower_id,
        RequestVoteResponse::granted(Term::new(1)),
        &[leader_id, follower_id],
    );

    assert_eq!(leader.role(), Role::Leader);

    // Append a command to the leader.
    leader.append_entry("A".to_string());

    assert_eq!(
        leader.commit_index(),
        LogIndex::ZERO
    );

    // Send the entry to the follower.
    let request = leader
        .build_append_entries(follower_id)
        .expect("leader should build AppendEntries");

    let response =
        follower.handle_append_entries(request);

    assert!(response.success);

    // Tell the leader that index 1 was replicated.
    leader.handle_append_entries_response(
        follower_id,
        response,
    );

    // Replication is now on a majority:
    //
    // leader   -> 1
    // follower -> 1
    //
    // 2/2 is a majority.
    leader.update_commit_index();

    assert_eq!(
        leader.commit_index(),
        LogIndex::new(1)
    );
}

#[test]
fn follower_advances_commit_index_from_leader_commit() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let mut leader = RaftNode::<String>::new(leader_id);
    let mut follower = RaftNode::<String>::new(follower_id);

    // Elect the leader.
    leader.start_election();

    leader.handle_request_vote_response(
        follower_id,
        RequestVoteResponse::granted(Term::new(1)),
        &[leader_id, follower_id],
    );

    assert_eq!(leader.role(), Role::Leader);

    // Leader creates three entries.
    leader.append_entry("A".to_string());
    leader.append_entry("B".to_string());
    leader.append_entry("C".to_string());

    // Send all three entries and tell the follower that index 3
    // is committed.
    let mut request = leader
        .build_append_entries(follower_id)
        .expect("leader should build AppendEntries");

    request.leader_commit = LogIndex::new(3);

    let response =
        follower.handle_append_entries(request);

    assert!(response.success);

    assert_eq!(
        follower.log().last_index(),
        LogIndex::new(3)
    );

    assert_eq!(
        follower.commit_index(),
        LogIndex::new(3)
    );
}

#[test]
fn follower_does_not_commit_past_local_log() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let mut leader = RaftNode::<String>::new(leader_id);
    let mut follower = RaftNode::<String>::new(follower_id);

    leader.start_election();

    leader.handle_request_vote_response(
        follower_id,
        RequestVoteResponse::granted(Term::new(1)),
        &[leader_id, follower_id],
    );

    assert_eq!(leader.role(), Role::Leader);

    leader.append_entry("A".to_string());
    leader.append_entry("B".to_string());
    leader.append_entry("C".to_string());

    let mut request = leader
        .build_append_entries(follower_id)
        .expect("leader should build AppendEntries");

    request.entries.truncate(2);
    request.leader_commit = LogIndex::new(3);

    let response =
        follower.handle_append_entries(request);

    assert!(response.success);

    assert_eq!(
        follower.log().last_index(),
        LogIndex::new(2)
    );

    assert_eq!(
        follower.commit_index(),
        LogIndex::new(2)
    );
}
#[test]
fn follower_commit_index_never_moves_backward() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let mut leader = RaftNode::<String>::new(leader_id);
    let mut follower = RaftNode::<String>::new(follower_id);

    leader.start_election();

    leader.handle_request_vote_response(
        follower_id,
        RequestVoteResponse::granted(Term::new(1)),
        &[leader_id, follower_id],
    );

    leader.append_entry("A".to_string());
    leader.append_entry("B".to_string());
    leader.append_entry("C".to_string());

    let mut request = leader
        .build_append_entries(follower_id)
        .expect("leader should build AppendEntries");

    request.leader_commit = LogIndex::new(3);

    let response =
        follower.handle_append_entries(request);

    assert!(response.success);
    assert_eq!(
        follower.commit_index(),
        LogIndex::new(3)
    );

    // A later RPC must not move commit_index backwards.
    let mut request = leader
        .build_append_entries(follower_id)
        .expect("leader should build AppendEntries");

    request.leader_commit = LogIndex::new(1);

    let response =
        follower.handle_append_entries(request);

    assert!(response.success);

    assert_eq!(
        follower.commit_index(),
        LogIndex::new(3)
    );
}

#[test]
fn leader_commit_index_never_moves_backward() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let mut leader = RaftNode::<String>::new(leader_id);

    leader.start_election();

    leader.handle_request_vote_response(
        follower_id,
        RequestVoteResponse::granted(Term::new(1)),
        &[leader_id, follower_id],
    );

    assert_eq!(leader.role(), Role::Leader);

    leader.append_entry("A".to_string());

    leader.handle_append_entries_response(
        follower_id,
        AppendEntriesResponse::success(
        Term::new(1),
        LogIndex::new(1),
    ),
    );

    leader.update_commit_index();

    assert_eq!(
        leader.commit_index(),
        LogIndex::new(1)
    );

    // Recalculating cannot reduce the commit index.
    leader.update_commit_index();

    assert_eq!(
        leader.commit_index(),
        LogIndex::new(1)
    );
}

#[test]
fn leader_does_not_commit_older_term_entry_directly() {
    let leader_id = ServerId::new(1);
    let follower_a = ServerId::new(2);
    let follower_b = ServerId::new(3);

    let mut leader = RaftNode::<String>::new(leader_id);

    // Term 1.
    leader.start_election();

    leader.handle_request_vote_response(
        follower_a,
        RequestVoteResponse::granted(Term::new(1)),
        &[leader_id, follower_a, follower_b],
    );

    assert_eq!(leader.role(), Role::Leader);

    leader.append_entry("A".to_string());

    // Do not replicate it yet.
    //
    // The entry is from the current term, so this setup alone
    // does not prove the older-term rule. We need a new term.
    //
    // Step down because of a newer term.
    leader.handle_append_entries(
        AppendEntriesRequest::heartbeat(
            Term::new(2),
            follower_a,
            LogIndex::new(0),
            Term::ZERO,
            LogIndex::ZERO,
        )
    );

    assert_eq!(leader.role(), Role::Follower);

    // Start a new election in term 3.
    leader.start_election();

    assert_eq!(
        leader.current_term(),
        Term::new(3)
    );

    // The old entry is still at index 1 and belongs to term 1.
    //
    // Become leader again.
    leader.handle_request_vote_response(
        follower_a,
        RequestVoteResponse::granted(Term::new(3)),
        &[leader_id, follower_a, follower_b],
    );

    assert_eq!(leader.role(), Role::Leader);

    leader.update_commit_index();

    assert_eq!(
        leader.commit_index(),
        LogIndex::ZERO
    );
}

struct RecordingStateMachine {
    applied: std::rc::Rc<
        std::cell::RefCell<Vec<String>>
    >,
}

impl RecordingStateMachine {
    fn new(
        applied: std::rc::Rc<
            std::cell::RefCell<Vec<String>>
        >,
    ) -> Self {
        Self {
            applied,
        }
    }
}

impl StateMachine<String> for RecordingStateMachine {
    type Error = ();

    fn apply(
        &mut self,
        command: &String,
    ) -> Result<(), Self::Error> {
        self
            .applied
            .borrow_mut()
            .push(command.clone());

        Ok(())
    }
}

struct FailingStateMachine {
    applied: Vec<String>,
}

impl FailingStateMachine {
    fn new() -> Self {
        Self {
            applied: Vec::new(),
        }
    }
}

impl StateMachine<String> for FailingStateMachine {
    type Error = &'static str;

    fn apply(
        &mut self,
        command: &String,
    ) -> Result<(), Self::Error> {
        self.applied.push(command.clone());

        if command == "B" {
            return Err("failed to apply B");
        }

        Ok(())
    }
}


#[test]
fn last_applied_does_not_advance_when_application_fails() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let state_machine = FailingStateMachine::new();

    let mut leader = RaftNode::with_state_machine(
        leader_id,
        state_machine,
    );

    let mut follower = RaftNode::<String>::new(follower_id);

    // Elect the leader.
    leader.start_election();

    leader.handle_request_vote_response(
        follower_id,
        RequestVoteResponse::granted(
            Term::new(1),
        ),
        &[leader_id, follower_id],
    );

    assert_eq!(
        leader.role(),
        Role::Leader
    );

    // Add two commands to the leader's log.
    leader.append_entry("A".to_string());
    leader.append_entry("B".to_string());

    // Replicate entry A.
    let request = leader
        .build_append_entries(follower_id)
        .expect(
            "leader should build AppendEntries",
        );

    let response =
        follower.handle_append_entries(request);

    assert!(response.success);

    leader.handle_append_entries_response(
        follower_id,
        response,
    );

    // Replicate entry B.
    let request = leader
        .build_append_entries(follower_id)
        .expect(
            "leader should build AppendEntries",
        );

    let response =
        follower.handle_append_entries(request);

    assert!(response.success);

    leader.handle_append_entries_response(
        follower_id,
        response,
    );

    // Both entries are now replicated on a majority.
    //
    // The leader automatically updates the commit index and
    // applies committed entries when processing the response.
    //
    // Entry A succeeds.
    // Entry B fails.
    //
    // Therefore:
    //
    // commit_index = 2
    // last_applied = 1

    assert_eq!(
        leader.commit_index(),
        LogIndex::new(2)
    );

    assert_eq!(
        leader.last_applied(),
        LogIndex::new(1)
    );

    // Entry B failed during the automatic application above.
    //
    // Applying again should retry B and fail again.
    let result = leader.apply_committed_entries();

    assert_eq!(
        result,
        Err("failed to apply B")
    );

    // last_applied must not advance past A.
    assert_eq!(
        leader.last_applied(),
        LogIndex::new(1)
    );
}

#[test]
fn follower_starts_election_when_election_timer_expires() {
    let server_id = ServerId::new(1);

    let mut node = RaftNode::<String>::new(server_id);

    assert_eq!(
        node.role(),
        Role::Follower
    );

    for _ in 0..5 {
        node.tick();
    }

    assert_eq!(
        node.role(),
        Role::Candidate
    );

    assert_eq!(
        node.current_term(),
        Term::new(1)
    );
}

#[test]
fn follower_does_not_start_election_before_timeout() {
    let server_id = ServerId::new(1);

    let mut node = RaftNode::<String>::new(server_id);

    for _ in 0..4 {
        node.tick();
    }

    assert_eq!(
        node.role(),
        Role::Follower
    );

    assert_eq!(
        node.current_term(),
        Term::ZERO
    );
}

#[test]
fn append_entries_resets_follower_election_timer() {
    let server_id = ServerId::new(1);
    let leader_id = ServerId::new(2);

    let mut node = RaftNode::<String>::new(server_id);

    for _ in 0..4 {
        node.tick();
    }

    let request = AppendEntriesRequest::heartbeat(
        Term::ZERO,
        leader_id,
        LogIndex::ZERO,
        Term::ZERO,
        LogIndex::ZERO,
    );

    let response = node.handle_append_entries(request);

    assert!(response.success);

    for _ in 0..4 {
        node.tick();
    }

    assert_eq!(
        node.role(),
        Role::Follower
    );
}

#[test]
fn candidate_starts_another_election_after_timeout() {
    let server_id = ServerId::new(1);

    let mut node = RaftNode::<String>::new(server_id);

    // First timeout starts the first election.
    for _ in 0..5 {
        node.tick();
    }

    assert_eq!(
        node.role(),
        Role::Candidate
    );

    assert_eq!(
        node.current_term(),
        Term::new(1)
    );

    // The candidate does not receive enough votes.
    // Its election timer eventually expires again.

    for _ in 0..5 {
        node.tick();
    }

    assert_eq!(
        node.role(),
        Role::Candidate
    );

    assert_eq!(
        node.current_term(),
        Term::new(2)
    );
}

#[test]
fn starting_another_election_increments_term_again() {
    let server_id = ServerId::new(1);

    let mut node = RaftNode::<String>::new(server_id);

    for expected_term in 1..=3 {
        for _ in 0..5 {
            node.tick();
        }

        assert_eq!(
            node.role(),
            Role::Candidate
        );

        assert_eq!(
            node.current_term(),
            Term::new(expected_term)
        );
    }
}

#[test]
fn candidate_does_not_start_another_election_before_timeout() {
    let server_id = ServerId::new(1);

    let mut node = RaftNode::<String>::new(server_id);

    // First election.
    for _ in 0..5 {
        node.tick();
    }

    assert_eq!(
        node.role(),
        Role::Candidate
    );

    assert_eq!(
        node.current_term(),
        Term::new(1)
    );

    // Only four ticks into the next election.
    for _ in 0..4 {
        node.tick();
    }

    assert_eq!(
        node.role(),
        Role::Candidate
    );

    assert_eq!(
        node.current_term(),
        Term::new(1)
    );
}


#[test]
fn candidate_steps_down_when_valid_append_entries_arrives() {
    let server_id = ServerId::new(1);
    let leader_id = ServerId::new(2);

    let mut node = RaftNode::<String>::new(server_id);

    // Become candidate.
    for _ in 0..5 {
        node.tick();
    }

    assert_eq!(
        node.role(),
        Role::Candidate
    );

    assert_eq!(
        node.current_term(),
        Term::new(1)
    );

    let request = AppendEntriesRequest::heartbeat(
        Term::new(1),
        leader_id,
        LogIndex::ZERO,
        Term::ZERO,
        LogIndex::ZERO,
    );

    let response =
        node.handle_append_entries(request);

    assert!(response.success);

    assert_eq!(
        node.role(),
        Role::Follower
    );

    assert_eq!(
        node.current_term(),
        Term::new(1)
    );
}

/// Verifies that a heartbeat is not generated
/// again before the normal interval expires.
#[test]
fn leader_does_not_generate_second_heartbeat_before_interval() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let mut leader =
        RaftNode::<String>::new(leader_id);

    leader.start_election();

    leader.handle_request_vote_response(
        follower_id,
        RequestVoteResponse::granted(
            Term::new(1),
        ),
        &[leader_id, follower_id],
    );

    assert_eq!(
        leader.role(),
        Role::Leader
    );

    // The newly elected leader sends its initial
    // heartbeat immediately.
    let initial_requests =
        leader.heartbeat_requests();

    assert_eq!(
        initial_requests.len(),
        1
    );

    // Four ticks are not enough for the next
    // heartbeat interval.
    for _ in 0..4 {
        leader.tick();
    }

    let requests =
        leader.heartbeat_requests();

    assert!(
        requests.is_empty()
    );

    // The fifth tick completes the normal interval.
    leader.tick();

    let requests =
        leader.heartbeat_requests();

    assert_eq!(
        requests.len(),
        1
    );
}

#[test]
fn leader_generates_heartbeat_for_each_follower() {
    let leader_id = ServerId::new(1);

    let followers = [
        ServerId::new(2),
        ServerId::new(3),
        ServerId::new(4),
    ];

    let cluster = [
        leader_id,
        followers[0],
        followers[1],
        followers[2],
    ];

    let mut leader =
        RaftNode::<String>::new(leader_id);

    leader.start_election();

    // The leader already voted for itself.
    //
    // Two additional votes give the candidate a
    // majority in a four-node cluster.
    leader.handle_request_vote_response(
        followers[0],
        RequestVoteResponse::granted(
            Term::new(1),
        ),
        &cluster,
    );

    leader.handle_request_vote_response(
        followers[1],
        RequestVoteResponse::granted(
            Term::new(1),
        ),
        &cluster,
    );

    assert_eq!(
        leader.role(),
        Role::Leader
    );

    // The heartbeat interval is five ticks.
    for _ in 0..5 {
        leader.tick();
    }

    let requests =
        leader.heartbeat_requests();

    assert_eq!(
        requests.len(),
        3
    );

    assert!(
        requests
            .iter()
            .all(|(_, request)| request.entries.is_empty())
    );
}

#[test]
fn heartbeat_reaches_follower_and_resets_election_timer() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let cluster = [
        leader_id,
        follower_id,
    ];

    let mut leader =
        RaftNode::<String>::new(leader_id);

    let mut follower =
        RaftNode::<String>::new(follower_id);

    // Elect the leader.
    leader.start_election();

    leader.handle_request_vote_response(
        follower_id,
        RequestVoteResponse::granted(
            Term::new(1),
        ),
        &cluster,
    );

    assert_eq!(
        leader.role(),
        Role::Leader
    );

    // Let the leader heartbeat timer expire.
    for _ in 0..5 {
        leader.tick();
    }

    let requests =
        leader.heartbeat_requests();

    assert_eq!(
        requests.len(),
        1
    );

    let (_, request) =
        requests.into_iter().next().expect(
            "leader should generate one heartbeat"
        );

    assert!(
        request.entries.is_empty()
    );

    // Let the follower get close to its election timeout.
    for _ in 0..4 {
        follower.tick();
    }

    assert_eq!(
        follower.role(),
        Role::Follower
    );

    // Deliver the heartbeat.
    let response =
        follower.handle_append_entries(request);

    assert!(response.success);

    // The heartbeat should have reset the follower's
    // election timer. Therefore another four ticks should
    // still not start an election.
    for _ in 0..4 {
        follower.tick();
    }

    assert_eq!(
        follower.role(),
        Role::Follower
    );
}

#[test]
fn successful_heartbeat_does_not_advance_match_index() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let cluster = [
        leader_id,
        follower_id,
    ];

    let mut leader =
        RaftNode::<String>::new(leader_id);

    let mut follower =
        RaftNode::<String>::new(follower_id);

    // Elect the leader.
    leader.start_election();

    leader.handle_request_vote_response(
        follower_id,
        RequestVoteResponse::granted(
            Term::new(1),
        ),
        &cluster,
    );

    assert_eq!(
        leader.role(),
        Role::Leader
    );

    let progress_before =
        leader
            .follower_progress(follower_id)
            .expect(
                "follower progress should exist"
            );

    assert_eq!(
        progress_before.match_index,
        LogIndex::ZERO
    );

    assert_eq!(
        progress_before.next_index,
        LogIndex::new(1)
    );

    // Wait for the heartbeat interval.
    for _ in 0..5 {
        leader.tick();
    }

    let requests =
        leader.heartbeat_requests();

    assert_eq!(
        requests.len(),
        1
    );

    let (_, request) =
        requests.into_iter().next().expect(
            "leader should generate heartbeat"
        );

    assert!(
        request.entries.is_empty()
    );

    // Deliver the heartbeat.
    let response =
        follower.handle_append_entries(request);

    assert!(
        response.success
    );

    // A heartbeat contains no new log entry.
    leader.handle_append_entries_response(
        follower_id,
        response,
    );

    let progress_after =
        leader
            .follower_progress(follower_id)
            .expect(
                "follower progress should exist"
            );

    // Nothing was replicated.
    assert_eq!(
        progress_after.match_index,
        LogIndex::ZERO
    );

    // No new entry was replicated, so next_index
    // must also remain unchanged.
    assert_eq!(
        progress_after.next_index,
        LogIndex::new(1)
    );
}

#[test]
fn leader_generates_heartbeat_again_after_next_interval() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let cluster = [
        leader_id,
        follower_id,
    ];

    let mut leader =
        RaftNode::<String>::new(leader_id);

    leader.start_election();

    leader.handle_request_vote_response(
        follower_id,
        RequestVoteResponse::granted(
            Term::new(1),
        ),
        &cluster,
    );

    assert_eq!(
        leader.role(),
        Role::Leader
    );

    // First heartbeat round.
    for _ in 0..5 {
        leader.tick();
    }

    let first_round =
        leader.heartbeat_requests();

    assert_eq!(
        first_round.len(),
        1
    );

    // The timer must have been reset after the
    // first heartbeat round.
    let no_second_round =
        leader.heartbeat_requests();

    assert!(
        no_second_round.is_empty()
    );

    // Four ticks are not enough for another heartbeat.
    for _ in 0..4 {
        leader.tick();
    }

    let still_no_round =
        leader.heartbeat_requests();

    assert!(
        still_no_round.is_empty()
    );

    // The fifth tick completes the next interval.
    leader.tick();

    let second_round =
        leader.heartbeat_requests();

    assert_eq!(
        second_round.len(),
        1
    );
}

#[test]
fn node_restart_preserves_persistent_state() {
    let server_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let mut node = RaftNode::<String>::new(server_id);

    // Start a real election.
    node.start_election();

    assert_eq!(node.role(), Role::Candidate);
    assert_eq!(node.current_term(), Term::new(1));
    assert_eq!(node.voted_for(), Some(server_id));

    // Give the candidate the second vote so it becomes leader.
    let response =
        rustyraft::raft::rpc::RequestVoteResponse::granted(
            Term::new(1),
        );

    node.handle_request_vote_response(
        follower_id,
        response,
        &[server_id, follower_id],
    );

    assert_eq!(node.role(), Role::Leader);

    // Now the node is allowed to append client commands.
    assert_eq!(
        node.append_entry("A".to_string()),
        Some(LogIndex::new(1))
    );

    assert_eq!(
        node.append_entry("B".to_string()),
        Some(LogIndex::new(2))
    );

    // Move the persistent state out of the failed node.
    let persistent = node.into_persistent_state();

    assert_eq!(
        persistent.current_term,
        Term::new(1)
    );

    assert_eq!(
        persistent.voted_for,
        Some(server_id)
    );

    assert_eq!(
        persistent.log.last_index(),
        LogIndex::new(2)
    );

    // Recreate the node from persisted state.
    let restarted =
        RaftNode::from_persistent_state(
            server_id,
            persistent,
            NoopStateMachine,
        );

    assert_eq!(
        restarted.current_term(),
        Term::new(1)
    );

    assert_eq!(
        restarted.voted_for(),
        Some(server_id)
    );

    assert_eq!(
        restarted.log().last_index(),
        LogIndex::new(2)
    );

    // Volatile/leader state should not survive the restart.
    assert_eq!(
        restarted.role(),
        Role::Follower
    );

    assert_eq!(
        restarted.commit_index(),
        LogIndex::ZERO
    );

    assert_eq!(
        restarted.last_applied(),
        LogIndex::ZERO
    );
}

#[test]
fn follower_restart_continues_log_replication() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let mut leader =
        RaftNode::<String>::new(leader_id);

    let mut follower =
        RaftNode::<String>::new(follower_id);

    // Elect the leader.
    leader.start_election();

    leader.handle_request_vote_response(
        follower_id,
        RequestVoteResponse::granted(
            Term::new(1),
        ),
        &[leader_id, follower_id],
    );

    assert_eq!(
        leader.role(),
        Role::Leader
    );

    // Add three commands to the leader's log.
    leader.append_entry(
        "A".to_string()
    );

    leader.append_entry(
        "B".to_string()
    );

    leader.append_entry(
        "C".to_string()
    );

    // Replicate only the first two entries
    // before the follower crashes.
    let request = AppendEntriesRequest::new(
        Term::new(1),
        leader_id,
        LogIndex::ZERO,
        Term::ZERO,
        vec![
            LogEntry::new(
                Term::new(1),
                "A".to_string(),
            ),
            LogEntry::new(
                Term::new(1),
                "B".to_string(),
            ),
        ],
        LogIndex::ZERO,
    );

    let response =
        follower.handle_append_entries(request);

    assert!(response.success);

    assert_eq!(
        follower.log().last_index(),
        LogIndex::new(2)
    );

    // Tell the leader that entries through index 2
    // were successfully replicated.
    leader.handle_append_entries_response(
        follower_id,
        response,
    );

    let progress = leader
        .follower_progress(follower_id)
        .expect(
            "leader should track follower"
        );

    assert_eq!(
        progress.match_index,
        LogIndex::new(2)
    );

    assert_eq!(
        progress.next_index,
        LogIndex::new(3)
    );

    // The follower crashes. Only persistent state survives.
    let persistent =
        follower.into_persistent_state();

    assert_eq!(
        persistent.log.last_index(),
        LogIndex::new(2)
    );

    // Restart the follower from persisted state.
    let mut follower =
        RaftNode::from_persistent_state(
            follower_id,
            persistent,
            NoopStateMachine,
        );

    assert_eq!(
        follower.role(),
        Role::Follower
    );

    assert_eq!(
        follower.log().last_index(),
        LogIndex::new(2)
    );

    // The leader should continue from the follower's
    // recovered log.
    let request = leader
        .build_append_entries(follower_id)
        .expect(
            "leader should build AppendEntries"
        );

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
        1
    );

    assert_eq!(
        request.entries[0].command,
        "C"
    );

    // Deliver the remaining entry to the
    // restarted follower.
    let response =
        follower.handle_append_entries(request);

    assert!(response.success);

    assert_eq!(
        response.replicated_index,
        Some(LogIndex::new(3))
    );

    assert_eq!(
        follower.log().last_index(),
        LogIndex::new(3)
    );
}

#[test]
fn request_vote_grants_candidate_with_newer_log_term() {
    let mut node =
        RaftNode::<String>::new(
            ServerId::new(1)
        );

    // Local log:
    //
    // index:  1   2
    // term:   1   1
    //
    // Candidate log:
    //
    // index:  1
    // term:   2
    //
    // Candidate has fewer entries, but its last log term
    // is newer, so its log is considered more up-to-date.
    let entries = vec![
        LogEntry::new(
            Term::new(1),
            "A".to_string(),
        ),
        LogEntry::new(
            Term::new(1),
            "B".to_string(),
        ),
    ];

    let append_request =
        AppendEntriesRequest::new(
            Term::new(1),
            ServerId::new(2),
            LogIndex::ZERO,
            Term::ZERO,
            entries,
            LogIndex::ZERO,
        );

    let response =
        node.handle_append_entries(
            append_request
        );

    assert!(
        response.success
    );

    let request =
        RequestVoteRequest::new(
            Term::new(2),
            ServerId::new(3),
            LogIndex::new(1),
            Term::new(2),
        );

    let response =
        node.handle_request_vote(request);

    assert!(
        response.vote_granted
    );

    assert_eq!(
        node.current_term(),
        Term::new(2)
    );

    assert_eq!(
        node.voted_for(),
        Some(ServerId::new(3))
    );
}

#[test]
fn request_vote_rejects_candidate_with_shorter_log_same_term() {
    let mut node =
        RaftNode::<String>::new(
            ServerId::new(1)
        );

    // Local log:
    //
    // index:  1   2
    // term:   1   2
    //
    // Candidate log:
    //
    // index:  1
    // term:   2
    //
    // The last terms are equal, so log index decides.
    // Candidate's log is shorter and must be rejected.
    let entries = vec![
        LogEntry::new(
            Term::new(1),
            "A".to_string(),
        ),
        LogEntry::new(
            Term::new(2),
            "B".to_string(),
        ),
    ];

    let append_request =
        AppendEntriesRequest::new(
            Term::new(2),
            ServerId::new(2),
            LogIndex::ZERO,
            Term::ZERO,
            entries,
            LogIndex::ZERO,
        );

    let response =
        node.handle_append_entries(
            append_request
        );

    assert!(
        response.success
    );

    let request =
        RequestVoteRequest::new(
            Term::new(3),
            ServerId::new(3),
            LogIndex::new(1),
            Term::new(2),
        );

    let response =
        node.handle_request_vote(request);

    assert!(
        !response.vote_granted
    );

    assert_eq!(
        node.current_term(),
        Term::new(3)
    );

    assert_eq!(
        node.voted_for(),
        None
    );
}

#[test]
fn leader_retries_and_reconciles_follower_after_failure() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let mut leader =
        RaftNode::<String>::new(leader_id);

    let mut follower =
        RaftNode::<String>::new(follower_id);

    // Elect the leader.
    leader.start_election();

    leader.handle_request_vote_response(
        follower_id,
        RequestVoteResponse::granted(
            Term::new(1),
        ),
        &[leader_id, follower_id],
    );

    assert_eq!(
        leader.role(),
        Role::Leader
    );

    // Leader log:
    //
    // index:  1   2   3
    // term:   1   1   1
    leader.append_entry(
        "A".to_string()
    );

    leader.append_entry(
        "B".to_string()
    );

    leader.append_entry(
        "C".to_string()
    );

    // Follower has only entries 1 and 2.
    let setup_request =
        AppendEntriesRequest::new(
            Term::new(1),
            leader_id,
            LogIndex::ZERO,
            Term::ZERO,
            vec![
                LogEntry::new(
                    Term::new(1),
                    "A".to_string(),
                ),
                LogEntry::new(
                    Term::new(1),
                    "B".to_string(),
                ),
            ],
            LogIndex::ZERO,
        );

    let response =
        follower.handle_append_entries(
            setup_request
        );

    assert!(response.success);

    assert_eq!(
        follower.log().last_index(),
        LogIndex::new(2)
    );

    // Simulate stale leader knowledge that index 3
    // had already been replicated.
    leader.handle_append_entries_response(
        follower_id,
        AppendEntriesResponse::success(
            Term::new(1),
            LogIndex::new(3),
        ),
    );

    let progress = leader
        .follower_progress(follower_id)
        .expect(
            "leader should track follower"
        );

    assert_eq!(
        progress.match_index,
        LogIndex::new(3)
    );

    assert_eq!(
        progress.next_index,
        LogIndex::new(4)
    );

    // First attempt starts at index 3.
    let request = leader
        .build_append_entries(
            follower_id
        )
        .expect(
            "leader should build AppendEntries"
        );

    assert_eq!(
        request.prev_log_index,
        LogIndex::new(3)
    );

    assert_eq!(
        request.entries.len(),
        0
    );

    // The follower does not have index 3.
    let response =
        follower.handle_append_entries(
            request
        );

    assert!(
        !response.success
    );

    // The leader backs next_index up.
    leader.handle_append_entries_response(
        follower_id,
        response,
    );

    let progress = leader
        .follower_progress(follower_id)
        .expect(
            "leader should track follower"
        );

    assert_eq!(
        progress.next_index,
        LogIndex::new(3)
    );

    // Retry from index 3.
    let request = leader
        .build_append_entries(
            follower_id
        )
        .expect(
            "leader should build retry"
        );

    assert_eq!(
        request.prev_log_index,
        LogIndex::new(2)
    );

    assert_eq!(
        request.entries.len(),
        1
    );

    assert_eq!(
        request.entries[0].command,
        "C"
    );

    let response =
        follower.handle_append_entries(
            request
        );

    assert!(
        response.success
    );

    assert_eq!(
        response.replicated_index,
        Some(LogIndex::new(3))
    );

    // The follower has now converged with the leader.
    assert_eq!(
        follower.log().last_index(),
        LogIndex::new(3)
    );

    assert_eq!(
        follower.log().term_at(
            LogIndex::new(3)
        ),
        Some(Term::new(1))
    );

    // Tell the leader that the retry succeeded.
    leader.handle_append_entries_response(
        follower_id,
        response,
    );

    let progress = leader
        .follower_progress(follower_id)
        .expect(
            "leader should track follower"
        );

    assert_eq!(
        progress.match_index,
        LogIndex::new(3)
    );

    assert_eq!(
        progress.next_index,
        LogIndex::new(4)
    );
}


/// Verifies that committed commands are applied
/// to the state machine in log order.
#[test]
fn apply_committed_entries_applies_commands_in_order() {
    let applied =
        std::rc::Rc::new(
            std::cell::RefCell::new(
                Vec::new()
            )
        );

    let state_machine =
        RecordingStateMachine::new(
            std::rc::Rc::clone(&applied)
        );

    let mut node =
        RaftNode::with_state_machine(
            ServerId::new(1),
            state_machine,
        );

    // Replicate three entries and mark all three
    // as committed.
    let request =
        AppendEntriesRequest::new(
            Term::new(1),
            ServerId::new(2),
            LogIndex::ZERO,
            Term::ZERO,
            vec![
                LogEntry::new(
                    Term::new(1),
                    "A".to_string(),
                ),
                LogEntry::new(
                    Term::new(1),
                    "B".to_string(),
                ),
                LogEntry::new(
                    Term::new(1),
                    "C".to_string(),
                ),
            ],
            LogIndex::new(3),
        );

    let response =
        node.handle_append_entries(
            request
        );

    assert!(
        response.success
    );

    assert_eq!(
        node.commit_index(),
        LogIndex::new(3)
    );

    assert_eq!(
        node.last_applied(),
        LogIndex::new(3)
    );

    assert_eq!(
        *applied.borrow(),
        vec![
            "A".to_string(),
            "B".to_string(),
            "C".to_string(),
        ]
    );
}

/// Verifies that a follower applies newly committed entries in order.
#[test]
fn follower_applies_committed_entries_in_order() {
    let applied = Rc::new(RefCell::new(Vec::new()));

    let state_machine =
        RecordingStateMachine::new(Rc::clone(&applied));

    let follower_id = ServerId::new(1);
    let leader_id = ServerId::new(2);

    let mut follower = RaftNode::with_state_machine(
        follower_id,
        state_machine,
    );

    let entries = vec![
        LogEntry::new(
            Term::new(1),
            "A".to_string(),
        ),
        LogEntry::new(
            Term::new(1),
            "B".to_string(),
        ),
        LogEntry::new(
            Term::new(1),
            "C".to_string(),
        ),
    ];

    let request = AppendEntriesRequest::new(
        Term::new(1),
        leader_id,
        LogIndex::ZERO,
        Term::ZERO,
        entries,
        LogIndex::new(2),
    );

    let response = follower.handle_append_entries(request);

    assert!(response.success);

    assert_eq!(
        follower.commit_index(),
        LogIndex::new(2)
    );

    assert_eq!(
        follower.last_applied(),
        LogIndex::new(2)
    );

    assert_eq!(
        *applied.borrow(),
        vec![
            "A".to_string(),
            "B".to_string(),
        ]
    );

    // A later heartbeat with the same commit index
    // must not apply the entries again.
    let request = AppendEntriesRequest::heartbeat(
        Term::new(1),
        leader_id,
        LogIndex::new(3),
        Term::new(1),
        LogIndex::new(2),
    );

    let response = follower.handle_append_entries(request);

    assert!(response.success);

    assert_eq!(
        follower.last_applied(),
        LogIndex::new(2)
    );

    assert_eq!(
        *applied.borrow(),
        vec![
            "A".to_string(),
            "B".to_string(),
        ]
    );
}


/// Verifies that stale AppendEntries does not reset election timeout.
#[test]
fn stale_append_entries_does_not_reset_election_timer() {
    let server_id = ServerId::new(1);
    let old_leader_id = ServerId::new(2);
    let current_leader_id = ServerId::new(3);

    let mut node = RaftNode::<String>::new(
        server_id
    );

    // Move the follower to term 2.
    let request = AppendEntriesRequest::heartbeat(
        Term::new(2),
        current_leader_id,
        LogIndex::ZERO,
        Term::ZERO,
        LogIndex::ZERO,
    );

    let response = node.handle_append_entries(request);

    assert!(response.success);
    assert_eq!(
        node.current_term(),
        Term::new(2)
    );

    // Get close to the election timeout.
    for _ in 0..4 {
        node.tick();
    }

    assert_eq!(
        node.role(),
        Role::Follower
    );

    // A delayed heartbeat from the old term must be ignored.
    let stale_request =
        AppendEntriesRequest::heartbeat(
            Term::new(1),
            old_leader_id,
            LogIndex::ZERO,
            Term::ZERO,
            LogIndex::ZERO,
        );

    let response =
        node.handle_append_entries(stale_request);

    assert!(!response.success);

    assert_eq!(
        node.current_term(),
        Term::new(2)
    );

    // The stale message must not have reset the timer.
    // The next tick should therefore trigger an election.
    node.tick();

    assert_eq!(
        node.role(),
        Role::Candidate
    );

    assert_eq!(
        node.current_term(),
        Term::new(3)
    );
}

/// Verifies that granting a vote resets the election timeout.
#[test]
fn granting_vote_resets_election_timer() {
    let server_id = ServerId::new(1);
    let candidate_id = ServerId::new(2);

    let mut node =
        RaftNode::<String>::new(server_id);

    // Move close to the election timeout.
    for _ in 0..4 {
        node.tick();
    }

    assert_eq!(
        node.role(),
        Role::Follower
    );

    // Grant a valid vote in the current term.
    let request = RequestVoteRequest::new(
        Term::ZERO,
        candidate_id,
        LogIndex::ZERO,
        Term::ZERO,
    );

    let response =
        node.handle_request_vote(request);

    assert!(response.vote_granted);
    assert_eq!(
        node.voted_for(),
        Some(candidate_id)
    );

    // The vote should have reset the election timer.
    // Four more ticks must still leave the node
    // as a follower.
    for _ in 0..4 {
        node.tick();
    }

    assert_eq!(
        node.role(),
        Role::Follower
    );

    // The fifth tick after the reset should
    // eventually cause a new election.
    node.tick();

    assert_eq!(
        node.role(),
        Role::Candidate
    );

    assert_eq!(
        node.current_term(),
        Term::new(1)
    );
}

/// Verifies that a candidate steps down when a valid leader appears.
#[test]
fn candidate_steps_down_on_current_term_append_entries() {
    let candidate_id = ServerId::new(1);
    let leader_id = ServerId::new(2);

    let mut node =
        RaftNode::<String>::new(candidate_id);

    // Start an election.
    node.start_election();

    assert_eq!(
        node.role(),
        Role::Candidate
    );

    assert_eq!(
        node.current_term(),
        Term::new(1)
    );

    assert!(
        node.election().is_some()
    );

    // A leader in the same term establishes itself
    // with an AppendEntries heartbeat.
    let request =
        AppendEntriesRequest::heartbeat(
            Term::new(1),
            leader_id,
            LogIndex::ZERO,
            Term::ZERO,
            LogIndex::ZERO,
        );

    let response =
        node.handle_append_entries(request);

    assert!(response.success);

    // The candidate must step down.
    assert_eq!(
        node.role(),
        Role::Follower
    );

    // The old election must no longer exist.
    assert!(
        node.election().is_none()
    );

    // The term remains unchanged because the
    // AppendEntries is from the current term.
    assert_eq!(
        node.current_term(),
        Term::new(1)
    );

    // The heartbeat reset the election timer.
    // Four ticks must still leave us as a follower.
    for _ in 0..4 {
        node.tick();
    }

    assert_eq!(
        node.role(),
        Role::Follower
    );
}

/// Verifies that the initial heartbeat is one-shot.
#[test]
fn initial_heartbeat_is_followed_by_normal_heartbeat_interval() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let cluster = [
        leader_id,
        follower_id,
    ];

    let mut leader =
        RaftNode::<String>::new(leader_id);

    leader.start_election();

    leader.handle_request_vote_response(
        follower_id,
        RequestVoteResponse::granted(
            Term::new(1),
        ),
        &cluster,
    );

    assert_eq!(
        leader.role(),
        Role::Leader
    );

    // Initial heartbeat is available immediately.
    let requests =
        leader.heartbeat_requests();

    assert_eq!(
        requests.len(),
        1
    );

    // It must not be generated again immediately.
    let requests =
        leader.heartbeat_requests();

    assert!(
        requests.is_empty()
    );

    // Normal heartbeat interval now applies.
    for _ in 0..4 {
        leader.tick();
    }

    assert!(
        leader.heartbeat_requests().is_empty()
    );

    leader.tick();

    let requests =
        leader.heartbeat_requests();

    assert_eq!(
        requests.len(),
        1
    );
}

/// Verifies that a leader commits and applies a command
/// after replication to a majority.
#[test]
fn leader_commits_command_after_majority_replication() {
    let leader_id = ServerId::new(1);
    let follower_a = ServerId::new(2);
    let follower_b = ServerId::new(3);

    let mut leader =
        RaftNode::<String>::new(leader_id);

    let mut follower =
        RaftNode::<String>::new(follower_a);

    // Elect node 1.
    leader.start_election();

    leader.handle_request_vote_response(
        follower_a,
        RequestVoteResponse::granted(
            Term::new(1),
        ),
        &[
            leader_id,
            follower_a,
            follower_b,
        ],
    );

    assert_eq!(
        leader.role(),
        Role::Leader
    );

    // The client command is appended only
    // to the leader initially.
    assert_eq!(
        leader.append_entry(
            "SET A".to_string()
        ),
        Some(LogIndex::new(1))
    );

    assert_eq!(
        leader.commit_index(),
        LogIndex::ZERO
    );

    assert_eq!(
        leader.last_applied(),
        LogIndex::ZERO
    );

    // Replicate the command to one follower.
    let request = leader
        .build_append_entries(follower_a)
        .expect(
            "leader should build AppendEntries",
        );

    assert_eq!(
        request.entries.len(),
        1
    );

    assert_eq!(
        request.entries[0].command,
        "SET A"
    );

    let response =
        follower.handle_append_entries(request);

    assert!(response.success);

    assert_eq!(
        follower.log().last_index(),
        LogIndex::new(1)
    );

    // Leader + follower_a = 2/3,
    // which is a majority.
    leader.handle_append_entries_response(
        follower_a,
        response,
    );

    assert_eq!(
        leader.commit_index(),
        LogIndex::new(1)
    );

    assert_eq!(
        leader.last_applied(),
        LogIndex::new(1)
    );

    // Applying again must not apply the
    // already-applied entry again.
    assert!(
        leader.apply_committed_entries().is_ok()
    );

    assert_eq!(
        leader.last_applied(),
        LogIndex::new(1)
    );

    // follower_b was never replicated.
    // The leader still committed because
    // leader + follower_a formed the majority.
    assert_eq!(
        leader.log().last_index(),
        LogIndex::new(1)
    );
}

/// Verifies that a leader does not commit a command
/// without replication to a majority.
#[test]
fn leader_does_not_commit_without_majority() {
    let leader_id = ServerId::new(1);
    let follower_a = ServerId::new(2);
    let follower_b = ServerId::new(3);

    let mut leader =
        RaftNode::<String>::new(leader_id);

    // Elect node 1.
    leader.start_election();

    leader.handle_request_vote_response(
        follower_a,
        RequestVoteResponse::granted(
            Term::new(1),
        ),
        &[
            leader_id,
            follower_a,
            follower_b,
        ],
    );

    assert_eq!(
        leader.role(),
        Role::Leader
    );

    // Append a command locally.
    assert_eq!(
        leader.append_entry(
            "SET B".to_string()
        ),
        Some(LogIndex::new(1))
    );

    // Nobody else has the command.
    leader.update_commit_index();

    // Only the leader has the entry: 1/3.
    assert_eq!(
        leader.commit_index(),
        LogIndex::ZERO
    );

    assert_eq!(
        leader.last_applied(),
        LogIndex::ZERO
    );
}
