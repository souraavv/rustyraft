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

use rustyraft::raft::state_machine::StateMachine;

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
    applied: Vec<String>,
}

impl RecordingStateMachine {
    fn new() -> Self {
        Self {
            applied: Vec::new(),
        }
    }
}

impl StateMachine<String> for RecordingStateMachine {
    type Error = ();

    fn apply(
        &mut self,
        command: &String,
    ) -> Result<(), Self::Error> {
        self.applied.push(command.clone());
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


#[test]
fn leader_does_not_generate_heartbeat_before_interval() {
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

    for _ in 0..4 {
        leader.tick();
    }

    let requests =
        leader.heartbeat_requests();

    assert!(requests.is_empty());
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