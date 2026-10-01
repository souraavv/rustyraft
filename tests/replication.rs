mod supports;

use supports::node::{
    new_node,
    new_node_with_log,
};

use rustyraft::raft::replication::{
    FollowerProgress,
    ReplicationState,
};

use rustyraft::raft::{
    LogEntry, LogIndex, RaftLog, RaftNode, Role, ServerId, Term,
};
use rustyraft::raft::rpc::{AppendEntriesResponse, RequestVoteResponse};

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

    progress.record_success(
        LogIndex::new(5),
    );

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

    progress.record_success(
        LogIndex::new(5),
    );

    progress.record_success(
        LogIndex::new(3),
    );

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
        state
            .progress(ServerId::new(99))
            .is_none()
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

    let request = replication
        .build_append_entries(
            unknown,
            leader,
            Term::new(2),
            &log,
            LogIndex::ZERO,
        );

    assert!(request.is_none());
}

#[test]
fn successful_append_entries_response_advances_progress() {
    let follower = ServerId::new(2);

    let mut state =
        ReplicationState::new(
            &[follower],
            LogIndex::new(5),
        );

    let response =
        rustyraft::raft::rpc::AppendEntriesResponse::success(
            Term::new(1),
            LogIndex::new(5),
        );

    assert!(state.handle_response(
        follower,
        &response,
    ));

    assert_eq!(
        state.match_index(follower),
        Some(LogIndex::new(5))
    );

    assert_eq!(
        state.next_index(follower),
        Some(LogIndex::new(6))
    );
}

#[test]
fn failed_append_entries_response_backs_up_next_index() {
    let follower = ServerId::new(2);

    let mut state =
        ReplicationState::new(
            &[follower],
            LogIndex::new(5),
        );

    let response =
        rustyraft::raft::rpc::AppendEntriesResponse::failure(
            Term::new(1),
        );

    assert!(state.handle_response(
        follower,
        &response,
    ));

    assert_eq!(
        state.next_index(follower),
        Some(LogIndex::new(5))
    );

    assert_eq!(
        state.match_index(follower),
        Some(LogIndex::ZERO)
    );
}

#[test]
fn replication_returns_match_indexes_for_all_followers() {
    let followers = vec![
        ServerId::new(2),
        ServerId::new(3),
        ServerId::new(4),
    ];

    let mut replication =
        ReplicationState::new(
            &followers,
            LogIndex::new(5),
        );

    replication.record_success(
        ServerId::new(2),
        LogIndex::new(3),
    );

    replication.record_success(
        ServerId::new(3),
        LogIndex::new(5),
    );

    replication.record_success(
        ServerId::new(4),
        LogIndex::new(2),
    );

    let match_indexes =
        replication.match_indexes();

    assert_eq!(
        match_indexes,
        vec![
            LogIndex::new(3),
            LogIndex::new(5),
            LogIndex::new(2),
        ]
    );
}

#[test]
fn successful_replication_is_monotonic() {
    let mut progress =
        FollowerProgress::new(
            LogIndex::new(6),
        );

    progress.record_success(
        LogIndex::new(5),
    );

    assert_eq!(
        progress.match_index,
        LogIndex::new(5),
    );

    assert_eq!(
        progress.next_index,
        LogIndex::new(6),
    );

    progress.record_success(
        LogIndex::new(3),
    );

    assert_eq!(
        progress.match_index,
        LogIndex::new(5),
    );

    assert_eq!(
        progress.next_index,
        LogIndex::new(6),
    );
}

#[test]
fn failed_replication_does_not_move_before_match_index() {
    let mut progress =
        FollowerProgress::new(
            LogIndex::new(6),
        );

    progress.record_success(
        LogIndex::new(5),
    );

    progress.record_failure();

    assert_eq!(
        progress.match_index,
        LogIndex::new(5),
    );

    assert_eq!(
        progress.next_index,
        progress.match_index,
    );
}

#[test]
fn failed_replication_can_backoff_to_match_boundary() {
    let mut progress =
        FollowerProgress::new(
            LogIndex::new(6),
        );

    progress.record_success(
        LogIndex::new(5),
    );

    progress.record_failure();
    progress.record_failure();

    assert_eq!(
        progress.match_index,
        LogIndex::new(5),
    );

    assert_eq!(
        progress.next_index,
        progress.match_index,
    );
}

/// Verifies a conflicting follower log is repaired after retrying replication.
#[test]
fn conflicting_follower_log_is_repaired_after_retry() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let mut leader = new_node(leader_id);
    let mut follower = new_node(follower_id);

    // Elect the leader in term 1.
    leader.start_election();

    leader.handle_request_vote_response(
        follower_id,
        RequestVoteResponse::granted(Term::new(1)),
        &[leader_id, follower_id],
    );

    assert_eq!(leader.role(), Role::Leader);
    assert_eq!(leader.current_term(), Term::new(1));

    // Leader creates A and B in term 1.
    assert_eq!(
        leader.append_entry("A".to_string()),
        Some(LogIndex::new(1)),
    );

    assert_eq!(
        leader.append_entry("B".to_string()),
        Some(LogIndex::new(2)),
    );

    // Replicate A and B to the follower first.
    let request = leader
        .build_append_entries(follower_id)
        .expect("leader should build AppendEntries");

    assert_eq!(
        request.prev_log_index,
        LogIndex::ZERO,
    );

    assert_eq!(request.entries.len(), 2);

    let response = follower.handle_append_entries(request);

    assert!(
        response.success,
        "initial common-prefix replication should succeed",
    );

    leader.handle_append_entries_response(
        follower_id,
        response,
    );

    // The leader should now know that entries 1 and 2
    // are replicated.
    let progress = leader
        .follower_progress(follower_id)
        .expect("leader should track follower");

    assert_eq!(
        progress.match_index,
        LogIndex::new(2),
    );

    assert_eq!(
        progress.next_index,
        LogIndex::new(3),
    );

    // Start a new election.
    //
    // The leader still has A and B when it becomes leader,
    // so next_index remains 3.
    leader.start_election();

    leader.handle_request_vote_response(
        follower_id,
        RequestVoteResponse::granted(Term::new(2)),
        &[leader_id, follower_id],
    );

    assert_eq!(leader.role(), Role::Leader);
    assert_eq!(leader.current_term(), Term::new(2));

    // C is created in term 2.
    assert_eq!(
        leader.append_entry("C".to_string()),
        Some(LogIndex::new(3)),
    );

    // Replicate C to the follower successfully.
    let request = leader
        .build_append_entries(follower_id)
        .expect("leader should build AppendEntries");

    assert_eq!(
        request.prev_log_index,
        LogIndex::new(2),
    );

    assert_eq!(
        request.prev_log_term,
        Term::new(1),
    );

    assert_eq!(request.entries.len(), 1);

    assert_eq!(
        request.entries[0].term,
        Term::new(2),
    );

    assert_eq!(
        request.entries[0].command,
        "C",
    );

    let response = follower.handle_append_entries(request);

    assert!(
        response.success,
        "C should replicate successfully",
    );

    leader.handle_append_entries_response(
        follower_id,
        response,
    );

    // The leader now believes that the follower has
    // the complete log.
    let progress = leader
        .follower_progress(follower_id)
        .expect("leader should track follower");

    assert_eq!(
        progress.match_index,
        LogIndex::new(3),
    );

    assert_eq!(
        progress.next_index,
        LogIndex::new(4),
    );

    assert_eq!(
        follower.log().last_index(),
        LogIndex::new(3),
    );

    // Now reconstruct the follower with a conflicting entry
    // at index 3.
    //
    // Leader:
    //
    //   index:  1   2   3
    //   term:   1   1   2
    //   command A   B   C
    //
    // Follower:
    //
    //   index:  1   2   3
    //   term:   1   1   1
    //   command A   B   X
    follower = new_node_with_log(
        follower_id,
        Term::new(2),
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
                "X".to_string(),
            ),
        ],
    );

    // Verify the follower starts with the conflicting entry.
    assert_eq!(
        follower.log().last_index(),
        LogIndex::new(3),
    );

    assert_eq!(
        follower
            .log()
            .term_at(LogIndex::new(3)),
        Some(Term::new(1)),
    );

    assert_eq!(
        follower
            .log()
            .get(LogIndex::new(3))
            .expect("entry 3 should exist")
            .command,
        "X",
    );

    // The leader still believes the follower has index 3.
    //
    // It therefore sends a consistency check using:
    //
    // prev_log_index = 3
    // prev_log_term  = 2
    //
    // The follower has term 1 at index 3, so it rejects.
    let request = leader
        .build_append_entries(follower_id)
        .expect("leader should build consistency request");

    assert_eq!(
        request.prev_log_index,
        LogIndex::new(3),
    );

    assert_eq!(
        request.prev_log_term,
        Term::new(2),
    );

    assert!(
        request.entries.is_empty(),
        "first request should check log consistency",
    );

    let response = follower.handle_append_entries(request);

    assert!(
        !response.success,
        "follower should reject the conflicting previous entry",
    );

    leader.handle_append_entries_response(
        follower_id,
        response,
    );

    // Failure backs next_index from 4 to 3.
    //
    // match_index stays at 3 because that replication was
    // previously confirmed.
    let progress = leader
        .follower_progress(follower_id)
        .expect("leader should track follower");

    assert_eq!(
        progress.next_index,
        LogIndex::new(3),
    );

    assert_eq!(
        progress.match_index,
        LogIndex::new(3),
    );

    // Retry from index 2.
    //
    // The leader now sends C after the matching prefix:
    //
    // prev_log_index = 2
    // prev_log_term  = 1
    // entry 3        = C(term 2)
    let retry = leader
        .build_append_entries(follower_id)
        .expect("leader should build retry request");

    assert_eq!(
        retry.prev_log_index,
        LogIndex::new(2),
    );

    assert_eq!(
        retry.prev_log_term,
        Term::new(1),
    );

    assert_eq!(retry.entries.len(), 1);

    assert_eq!(
        retry.entries[0].term,
        Term::new(2),
    );

    assert_eq!(
        retry.entries[0].command,
        "C",
    );

    let response = follower.handle_append_entries(retry);

    assert!(
        response.success,
        "follower should accept the corrected entry",
    );

    leader.handle_append_entries_response(
        follower_id,
        response,
    );

    // The conflicting X should have been replaced by C.
    assert_eq!(
        follower.log().last_index(),
        LogIndex::new(3),
    );

    assert_eq!(
        follower
            .log()
            .term_at(LogIndex::new(3)),
        Some(Term::new(2)),
    );

    assert_eq!(
        follower
            .log()
            .get(LogIndex::new(3))
            .expect("entry 3 should exist")
            .command,
        "C",
    );

    // Successful retry restores the leader's replication progress.
    let progress = leader
        .follower_progress(follower_id)
        .expect("leader should track follower");

    assert_eq!(
        progress.match_index,
        LogIndex::new(3),
    );

    assert_eq!(
        progress.next_index,
        LogIndex::new(4),
    );
}

/// Verifies a leader steps down after receiving a newer-term response.
#[test]
fn leader_steps_down_on_newer_term_append_entries_response() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let mut leader = new_node(leader_id);

    // Elect the leader in term 1.
    leader.start_election();

    leader.handle_request_vote_response(
        follower_id,
        RequestVoteResponse::granted(Term::new(1)),
        &[leader_id, follower_id],
    );

    assert_eq!(
        leader.role(),
        Role::Leader,
    );

    assert_eq!(
        leader.current_term(),
        Term::new(1),
    );

    // Establish that the leader has replication state for the follower.
    let progress = leader
        .follower_progress(follower_id)
        .expect("leader should track follower");

    assert_eq!(
        progress.match_index,
        LogIndex::ZERO,
    );

    assert_eq!(
        progress.next_index,
        LogIndex::new(1),
    );

    // A response from a newer term means this leader's term is stale.
    let response = AppendEntriesResponse::failure(
        Term::new(2),
    );

    leader.handle_append_entries_response(
        follower_id,
        response,
    );

    // The leader must immediately become a follower.
    assert_eq!(
        leader.role(),
        Role::Follower,
    );

    // The newer term becomes the node's current term.
    assert_eq!(
        leader.current_term(),
        Term::new(2),
    );

    // Leader-specific replication state must no longer exist.
    assert!(
        leader
            .follower_progress(follower_id)
            .is_none()
    );
}

/// Verifies a leader ignores a stale AppendEntries response.
#[test]
fn leader_ignores_older_term_append_entries_response() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let mut leader = new_node(leader_id);

    // Elect the leader in term 1.
    leader.start_election();

    leader.handle_request_vote_response(
        follower_id,
        RequestVoteResponse::granted(Term::new(1)),
        &[leader_id, follower_id],
    );

    assert_eq!(
        leader.role(),
        Role::Leader,
    );

    assert_eq!(
        leader.current_term(),
        Term::new(1),
    );

    // Create a log entry and move to a newer term.
    leader.append_entry("A".to_string());

    leader.start_election();

    leader.handle_request_vote_response(
        follower_id,
        RequestVoteResponse::granted(Term::new(2)),
        &[leader_id, follower_id],
    );

    assert_eq!(
        leader.role(),
        Role::Leader,
    );

    assert_eq!(
        leader.current_term(),
        Term::new(2),
    );

    // Establish some replication progress in the current term.
    //
    // This is deliberately a successful response so we have
    // observable state that a stale response could incorrectly
    // change.
    let current_response = AppendEntriesResponse::success(
        Term::new(2),
        LogIndex::new(1),
    );

    leader.handle_append_entries_response(
        follower_id,
        current_response,
    );

    let progress = leader
        .follower_progress(follower_id)
        .expect("leader should track follower");

    assert_eq!(
        progress.match_index,
        LogIndex::new(1),
    );

    assert_eq!(
        progress.next_index,
        LogIndex::new(2),
    );

    // A delayed response from the previous term arrives.
    //
    // Even though this response says success, it must not
    // modify the current term's replication state.
    let stale_response = AppendEntriesResponse::success(
        Term::new(1),
        LogIndex::ZERO,
    );

    leader.handle_append_entries_response(
        follower_id,
        stale_response,
    );

    // The leader must remain in the current term.
    assert_eq!(
        leader.current_term(),
        Term::new(2),
    );

    assert_eq!(
        leader.role(),
        Role::Leader,
    );

    // The stale response must not change replication progress.
    let progress = leader
        .follower_progress(follower_id)
        .expect("leader should still track follower");

    assert_eq!(
        progress.match_index,
        LogIndex::new(1),
    );

    assert_eq!(
        progress.next_index,
        LogIndex::new(2),
    );
}

/// Verifies an older successful response cannot reduce replication progress.
#[test]
fn stale_success_response_does_not_move_progress_backward() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let mut leader = new_node(leader_id);

    // Elect the leader in term 1.
    leader.start_election();

    leader.handle_request_vote_response(
        follower_id,
        RequestVoteResponse::granted(Term::new(1)),
        &[leader_id, follower_id],
    );

    assert_eq!(
        leader.role(),
        Role::Leader,
    );

    assert_eq!(
        leader.current_term(),
        Term::new(1),
    );

    // Create three entries so replication progress can move
    // through multiple indexes.
    assert_eq!(
        leader.append_entry("A".to_string()),
        Some(LogIndex::new(1)),
    );

    assert_eq!(
        leader.append_entry("B".to_string()),
        Some(LogIndex::new(2)),
    );

    assert_eq!(
        leader.append_entry("C".to_string()),
        Some(LogIndex::new(3)),
    );

    // Simulate a successful response for index 2.
    //
    // This represents an earlier AppendEntries RPC that replicated
    // entries through index 2.
    let response = AppendEntriesResponse::success(
        Term::new(1),
        LogIndex::new(2),
    );

    leader.handle_append_entries_response(
        follower_id,
        response,
    );

    let progress = leader
        .follower_progress(follower_id)
        .expect("leader should track follower");

    assert_eq!(
        progress.match_index,
        LogIndex::new(2),
    );

    assert_eq!(
        progress.next_index,
        LogIndex::new(3),
    );

    // A newer RPC succeeds through index 3.
    let response = AppendEntriesResponse::success(
        Term::new(1),
        LogIndex::new(3),
    );

    leader.handle_append_entries_response(
        follower_id,
        response,
    );

    let progress = leader
        .follower_progress(follower_id)
        .expect("leader should track follower");

    assert_eq!(
        progress.match_index,
        LogIndex::new(3),
    );

    assert_eq!(
        progress.next_index,
        LogIndex::new(4),
    );

    // Now an older response arrives late.
    //
    // It is still from the current term and still reports success,
    // but it only confirms replication through index 2.
    let stale_response = AppendEntriesResponse::success(
        Term::new(1),
        LogIndex::new(2),
    );

    leader.handle_append_entries_response(
        follower_id,
        stale_response,
    );

    // The stale success must not move either value backward.
    let progress = leader
        .follower_progress(follower_id)
        .expect("leader should track follower");

    assert_eq!(
        progress.match_index,
        LogIndex::new(3),
    );

    assert_eq!(
        progress.next_index,
        LogIndex::new(4),
    );
}

/// Verifies a successful response without an index does not advance progress.
#[test]
fn success_response_without_replicated_index_does_not_advance_progress() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let mut leader = new_node(leader_id);

    // Elect the leader in term 1.
    leader.start_election();

    leader.handle_request_vote_response(
        follower_id,
        RequestVoteResponse::granted(Term::new(1)),
        &[leader_id, follower_id],
    );

    assert_eq!(
        leader.role(),
        Role::Leader,
    );

    // Record some legitimate replication progress first.
    let valid_response = AppendEntriesResponse::success(
        Term::new(1),
        LogIndex::new(2),
    );

    leader.handle_append_entries_response(
        follower_id,
        valid_response,
    );

    let progress = leader
        .follower_progress(follower_id)
        .expect("leader should track follower");

    assert_eq!(
        progress.match_index,
        LogIndex::new(2),
    );

    assert_eq!(
        progress.next_index,
        LogIndex::new(3),
    );

    // A malformed success response does not say which log index
    // was actually replicated.
    let malformed_response = AppendEntriesResponse {
        term: Term::new(1),
        success: true,
        replicated_index: None,
    };

    leader.handle_append_entries_response(
        follower_id,
        malformed_response,
    );

    // The leader must not invent replication progress.
    let progress = leader
        .follower_progress(follower_id)
        .expect("leader should track follower");

    assert_eq!(
        progress.match_index,
        LogIndex::new(2),
    );

    assert_eq!(
        progress.next_index,
        LogIndex::new(3),
    );
}