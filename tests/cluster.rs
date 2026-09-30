mod support;

use rustyraft::raft::{
    LogIndex, Role, ServerId, Term,
};

use support::cluster::TestCluster;

#[test]
fn cluster_creates_all_nodes() {
    let server_ids = [
        ServerId::new(1),
        ServerId::new(2),
        ServerId::new(3),
    ];

    let cluster =
        TestCluster::<String>::new(
            &server_ids,
        );

    for server_id in server_ids {
        let node =
            cluster
                .node(server_id)
                .expect("node should exist");

        assert_eq!(
            node.id(),
            server_id
        );

        assert_eq!(
            node.role(),
            Role::Follower
        );
    }
}

#[test]
fn starting_election_queues_request_vote_messages() {
    let server_ids = [
        ServerId::new(1),
        ServerId::new(2),
        ServerId::new(3),
    ];

    let candidate_id =
        ServerId::new(1);

    let mut cluster =
        TestCluster::<String>::new(
            &server_ids,
        );

    cluster.start_election(
        candidate_id,
    );

    assert_eq!(
        cluster.transport().pending_count(),
        2
    );

    assert_eq!(
        cluster
            .node(candidate_id)
            .unwrap()
            .role(),
        Role::Candidate
    );
}

#[test]
fn cluster_delivers_request_vote() {
    let server_ids = [
        ServerId::new(1),
        ServerId::new(2),
        ServerId::new(3),
    ];

    let candidate_id =
        ServerId::new(1);

    let follower_id =
        ServerId::new(2);

    let mut cluster =
        TestCluster::<String>::new(
            &server_ids,
        );

    cluster.start_election(
        candidate_id,
    );

    let delivery = cluster
        .deliver_to(follower_id)
        .expect(
            "RequestVote should be delivered"
        );

    assert_eq!(
        delivery.from,
        candidate_id
    );

    assert_eq!(
        delivery.to,
        follower_id
    );

    assert_eq!(
        cluster
            .node(follower_id)
            .unwrap()
            .current_term(),
        Term::new(1)
    );
}

#[test]
fn request_vote_round_trip_reaches_candidate() {
    let server_ids = [
        ServerId::new(1),
        ServerId::new(2),
        ServerId::new(3),
    ];

    let candidate_id =
        ServerId::new(1);

    let follower_id =
        ServerId::new(2);

    let mut cluster =
        TestCluster::<String>::new(
            &server_ids,
        );

    cluster.start_election(
        candidate_id,
    );

    let delivery =
        cluster
            .deliver_to(follower_id)
            .expect(
                "RequestVote should be delivered"
            );

    assert_eq!(
        delivery.from,
        candidate_id
    );

    assert_eq!(
        delivery.to,
        follower_id
    );

    assert_eq!(
        cluster
            .transport()
            .pending_count(),
        2
    );

    assert!(
        cluster
            .deliver_to(candidate_id)
            .is_some()
    );

    assert_eq!(
        cluster
            .node(candidate_id)
            .unwrap()
            .role(),
        Role::Leader
    );
}

#[test]
fn append_entries_round_trip_uses_transport() {
    let leader_id =
        ServerId::new(1);

    let follower_id =
        ServerId::new(2);

    let server_ids = [
        leader_id,
        follower_id,
    ];

    let mut cluster =
        TestCluster::<String>::new(
            &server_ids,
        );

    {
        let leader =
            cluster
                .node_mut(leader_id)
                .unwrap();

        leader.start_election();

        leader.handle_request_vote_response(
            follower_id,
            rustyraft::raft::rpc::RequestVoteResponse::granted(
                Term::new(1),
            ),
            &server_ids,
        );

        assert_eq!(
            leader.role(),
            Role::Leader,
        );

        leader.append_entry(
            "A".to_string(),
        );

        leader.append_entry(
            "B".to_string(),
        );

        leader.append_entry(
            "C".to_string(),
        );
    }

    // Build the AppendEntries request from the leader
    // and queue it in the transport.
    assert!(
        cluster.send_append_entries(
            leader_id,
            follower_id,
        )
    );

    assert_eq!(
        cluster
            .transport()
            .pending_count(),
        1,
    );

    // Deliver AppendEntries to the follower.
    let delivery =
        cluster
            .deliver_to(follower_id)
            .expect(
                "AppendEntries should be delivered",
            );

    assert_eq!(
        delivery.from,
        leader_id,
    );

    assert_eq!(
        delivery.to,
        follower_id,
    );

    // The request was consumed and the follower
    // queued exactly one response.
    assert_eq!(
        cluster
            .transport()
            .pending_count(),
        1,
    );

    // Deliver the AppendEntriesResponse back
    // to the leader.
    let response_delivery =
        cluster
            .deliver_to(leader_id)
            .expect(
                "AppendEntriesResponse should be delivered",
            );

    assert_eq!(
        response_delivery.from,
        follower_id,
    );

    assert_eq!(
        response_delivery.to,
        leader_id,
    );

    // The response has now been consumed.
    assert_eq!(
        cluster
            .transport()
            .pending_count(),
        0,
    );

    let progress =
        cluster
            .node(leader_id)
            .unwrap()
            .follower_progress(
                follower_id,
            )
            .expect(
                "leader should track follower",
            );

    assert_eq!(
        progress.match_index,
        LogIndex::new(3),
    );

    assert_eq!(
        progress.next_index,
        LogIndex::new(4),
    );

    assert_eq!(
        cluster
            .node(follower_id)
            .unwrap()
            .last_log_index(),
        LogIndex::new(3),
    );
}

#[test]
fn append_entries_can_be_retried_after_drop() {
    let leader_id =
        ServerId::new(1);

    let follower_id =
        ServerId::new(2);

    let server_ids = [
        leader_id,
        follower_id,
    ];

    let mut cluster =
        TestCluster::<String>::new(
            &server_ids,
        );

    {
        let leader =
            cluster
                .node_mut(leader_id)
                .unwrap();

        leader.start_election();

        leader.handle_request_vote_response(
            follower_id,
            rustyraft::raft::rpc::RequestVoteResponse::granted(
                Term::new(1),
            ),
            &server_ids,
        );

        assert_eq!(
            leader.role(),
            Role::Leader,
        );

        leader.append_entry(
            "A".to_string(),
        );

        leader.append_entry(
            "B".to_string(),
        );

        leader.append_entry(
            "C".to_string(),
        );
    }

    // First replication attempt.
    assert!(
        cluster.send_append_entries(
            leader_id,
            follower_id,
        )
    );

    assert_eq!(
        cluster
            .transport()
            .pending_count(),
        1,
    );

    // Drop the AppendEntries message.
    assert!(
        cluster.drop_to(
            follower_id,
        )
    );

    assert_eq!(
        cluster
            .transport()
            .pending_count(),
        0,
    );

    // The follower never received the dropped message.
    assert_eq!(
        cluster
            .node(follower_id)
            .unwrap()
            .last_log_index(),
        LogIndex::ZERO,
    );

    // Retry the replication.
    assert!(
        cluster.send_append_entries(
            leader_id,
            follower_id,
        )
    );

    assert_eq!(
        cluster
            .transport()
            .pending_count(),
        1,
    );

    // Deliver the retry to the follower.
    assert!(
        cluster
            .deliver_to(follower_id)
            .is_some()
    );

    // The follower generated a response.
    assert_eq!(
        cluster
            .transport()
            .pending_count(),
        1,
    );

    assert_eq!(
        cluster
            .node(follower_id)
            .unwrap()
            .last_log_index(),
        LogIndex::new(3),
    );

    // Deliver the response back to the leader.
    assert!(
        cluster
            .deliver_to(leader_id)
            .is_some()
    );

    assert_eq!(
        cluster
            .transport()
            .pending_count(),
        0,
    );

    let progress =
        cluster
            .node(leader_id)
            .unwrap()
            .follower_progress(
                follower_id,
            )
            .expect(
                "leader should track follower",
            );

    assert_eq!(
        progress.match_index,
        LogIndex::new(3),
    );

    assert_eq!(
        progress.next_index,
        LogIndex::new(4),
    );
}


#[test]
fn reordered_append_entries_does_not_move_progress_backward() {
    let leader_id =
        ServerId::new(1);

    let follower_id =
        ServerId::new(2);

    let server_ids = [
        leader_id,
        follower_id,
    ];

    let mut cluster =
        TestCluster::<String>::new(
            &server_ids,
        );

    {
        let leader =
            cluster
                .node_mut(leader_id)
                .unwrap();

        leader.start_election();

        leader.handle_request_vote_response(
            follower_id,
            rustyraft::raft::rpc::RequestVoteResponse::granted(
                Term::new(1),
            ),
            &server_ids,
        );

        assert_eq!(
            leader.role(),
            Role::Leader,
        );

        // First entry.
        leader.append_entry(
            "A".to_string(),
        );
    }

    // First AppendEntries:
    //
    // entries = [A]
    assert!(
        cluster.send_append_entries(
            leader_id,
            follower_id,
        )
    );

    {
        let leader =
            cluster
                .node_mut(leader_id)
                .unwrap();

        // Add another entry before the first
        // replication response is processed.
        leader.append_entry(
            "B".to_string(),
        );
    }

    // Second AppendEntries:
    //
    // entries = [A, B]
    assert!(
        cluster.send_append_entries(
            leader_id,
            follower_id,
        )
    );

    assert_eq!(
        cluster
            .transport()
            .pending_count(),
        2,
    );

    // Deliver the newer AppendEntries first.
    let delivery =
        cluster
            .deliver_at(1)
            .expect(
                "second AppendEntries should exist",
            );

    assert_eq!(
        delivery.from,
        leader_id,
    );

    assert_eq!(
        delivery.to,
        follower_id,
    );

    // The follower now has A and B.
    assert_eq!(
        cluster
            .node(follower_id)
            .unwrap()
            .last_log_index(),
        LogIndex::new(2),
    );

    // Queue now contains:
    //
    // [old AppendEntries, newer response]
    assert_eq!(
        cluster
            .transport()
            .pending_count(),
        2,
    );

    // Deliver the newer response first.
    let response_delivery =
        cluster
            .deliver_at(1)
            .expect(
                "newer response should exist",
            );

    assert_eq!(
        response_delivery.from,
        follower_id,
    );

    assert_eq!(
        response_delivery.to,
        leader_id,
    );

    let progress =
        cluster
            .node(leader_id)
            .unwrap()
            .follower_progress(
                follower_id,
            )
            .expect(
                "leader should track follower",
            );

    assert_eq!(
        progress.match_index,
        LogIndex::new(2),
    );

    assert_eq!(
        progress.next_index,
        LogIndex::new(3),
    );

    // Now deliver the older AppendEntries.
    assert!(
        cluster
            .deliver_to(follower_id)
            .is_some()
    );

    // The follower must still have A and B.
    assert_eq!(
        cluster
            .node(follower_id)
            .unwrap()
            .last_log_index(),
        LogIndex::new(2),
    );

    // The older request generated an older response.
    assert_eq!(
        cluster
            .transport()
            .pending_count(),
        1,
    );

    // Deliver the older response last.
    assert!(
        cluster
            .deliver_to(leader_id)
            .is_some()
    );

    // Older success must not move replication
    // progress backward.
    let progress =
        cluster
            .node(leader_id)
            .unwrap()
            .follower_progress(
                follower_id,
            )
            .expect(
                "leader should track follower",
            );

    assert_eq!(
        progress.match_index,
        LogIndex::new(2),
    );

    assert_eq!(
        progress.next_index,
        LogIndex::new(3),
    );

    assert_eq!(
        cluster
            .transport()
            .pending_count(),
        0,
    );
}

#[test]
fn duplicate_append_entries_does_not_duplicate_log_entries() {
    let leader_id =
        ServerId::new(1);

    let follower_id =
        ServerId::new(2);

    let server_ids = [
        leader_id,
        follower_id,
    ];

    let mut cluster =
        TestCluster::<String>::new(
            &server_ids,
        );

    {
        let leader =
            cluster
                .node_mut(leader_id)
                .unwrap();

        leader.start_election();

        leader.handle_request_vote_response(
            follower_id,
            rustyraft::raft::rpc::RequestVoteResponse::granted(
                Term::new(1),
            ),
            &server_ids,
        );

        assert_eq!(
            leader.role(),
            Role::Leader,
        );

        leader.append_entry(
            "A".to_string(),
        );

        leader.append_entry(
            "B".to_string(),
        );
    }

    // Send the same logical AppendEntries twice.
    assert!(
        cluster.send_append_entries(
            leader_id,
            follower_id,
        )
    );

    assert!(
        cluster.send_append_entries(
            leader_id,
            follower_id,
        )
    );

    assert_eq!(
        cluster
            .transport()
            .pending_count(),
        2,
    );

    // Deliver the first copy.
    assert!(
        cluster
            .deliver_to(follower_id)
            .is_some()
    );

    assert_eq!(
        cluster
            .node(follower_id)
            .unwrap()
            .last_log_index(),
        LogIndex::new(2),
    );

    // Deliver the duplicate copy.
    assert!(
        cluster
            .deliver_to(follower_id)
            .is_some()
    );

    // The duplicate must not append A and B again.
    assert_eq!(
        cluster
            .node(follower_id)
            .unwrap()
            .last_log_index(),
        LogIndex::new(2),
    );

    // Two responses are now queued.
    assert_eq!(
        cluster
            .transport()
            .pending_count(),
        2,
    );

    // Process both responses.
    assert!(
        cluster
            .deliver_to(leader_id)
            .is_some()
    );

    assert!(
        cluster
            .deliver_to(leader_id)
            .is_some()
    );

    let progress =
        cluster
            .node(leader_id)
            .unwrap()
            .follower_progress(
                follower_id,
            )
            .expect(
                "leader should track follower",
            );

    assert_eq!(
        progress.match_index,
        LogIndex::new(2),
    );

    assert_eq!(
        progress.next_index,
        LogIndex::new(3),
    );

    assert_eq!(
        cluster
            .transport()
            .pending_count(),
        0,
    );
}