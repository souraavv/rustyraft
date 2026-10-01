mod supports;

use rustyraft::raft::{
    LogIndex, Role, ServerId, Term,
};

use supports::cluster::TestCluster;

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

#[test]
fn three_node_cluster_commits_command_after_majority_replication() {
    let leader_id = ServerId::new(1);
    let follower_a = ServerId::new(2);
    let follower_b = ServerId::new(3);

    let server_ids = [
        leader_id,
        follower_a,
        follower_b,
    ];

    let mut cluster =
        TestCluster::<String>::new(&server_ids);

    // Start an election on node 1.
    cluster.start_election(leader_id);

    // Deliver RequestVote to both followers.
    cluster.deliver_to(follower_a);
    cluster.deliver_to(follower_b);

    // Deliver the vote responses back to the candidate.
    cluster.deliver_to(leader_id);
    cluster.deliver_to(leader_id);

    assert_eq!(
        cluster
            .node(leader_id)
            .expect("leader should exist")
            .role(),
        Role::Leader
    );

    // Append a client command to the leader.
    let index = cluster
        .node_mut(leader_id)
        .expect("leader should exist")
        .append_entry(
            "SET A".to_string(),
        )
        .expect("leader should accept command");

    assert_eq!(
        index,
        LogIndex::new(1)
    );

    // The command is only on the leader at this point.
    assert_eq!(
        cluster
            .node(leader_id)
            .expect("leader should exist")
            .commit_index(),
        LogIndex::ZERO
    );

    // Replicate the command to one follower.
    //
    // Leader + follower_a = 2/3, which is a majority.
    assert!(
        cluster.send_append_entries(
            leader_id,
            follower_a,
        )
    );

    // Deliver AppendEntries to follower_a.
    cluster.deliver_to(follower_a);

    // Deliver the successful response back to the leader.
    cluster.deliver_to(leader_id);

    assert_eq!(
        cluster
            .node(leader_id)
            .expect("leader should exist")
            .commit_index(),
        LogIndex::new(1)
    );

    assert_eq!(
        cluster
            .node(leader_id)
            .expect("leader should exist")
            .last_applied(),
        LogIndex::new(1)
    );

    // follower_b was never given the command.
    assert_eq!(
        cluster
            .node(follower_b)
            .expect("follower should exist")
            .log()
            .last_index(),
        LogIndex::ZERO
    );

    // follower_a received the command.
    assert_eq!(
        cluster
            .node(follower_a)
            .expect("follower should exist")
            .log()
            .last_index(),
        LogIndex::new(1)
    );
}

#[test]
fn three_node_cluster_does_not_commit_without_majority() {
    let leader_id = ServerId::new(1);
    let follower_a = ServerId::new(2);
    let follower_b = ServerId::new(3);

    let server_ids = [
        leader_id,
        follower_a,
        follower_b,
    ];

    let mut cluster =
        TestCluster::<String>::new(&server_ids);

    cluster.start_election(leader_id);

    cluster.deliver_to(follower_a);
    cluster.deliver_to(follower_b);

    cluster.deliver_to(leader_id);
    cluster.deliver_to(leader_id);

    assert_eq!(
        cluster
            .node(leader_id)
            .expect("leader should exist")
            .role(),
        Role::Leader
    );

    cluster
        .node_mut(leader_id)
        .expect("leader should exist")
        .append_entry(
            "SET B".to_string(),
        )
        .expect("leader should accept command");

    // Do not deliver the command to either follower.
    //
    // Only the leader has the entry: 1/3.
    cluster
        .node(leader_id)
        .expect("leader should exist");

    assert_eq!(
        cluster
            .node(leader_id)
            .expect("leader should exist")
            .commit_index(),
        LogIndex::ZERO
    );

    assert_eq!(
        cluster
            .node(leader_id)
            .expect("leader should exist")
            .last_applied(),
        LogIndex::ZERO
    );
}

/// Verifies a dropped replication is retried and eventually commits.
#[test]
fn cluster_retries_dropped_append_entries() {
    let leader_id = ServerId::new(1);
    let follower_a = ServerId::new(2);
    let follower_b = ServerId::new(3);

    let server_ids = [
        leader_id,
        follower_a,
        follower_b,
    ];

    let mut cluster =
        TestCluster::<String>::new(&server_ids);

    // Elect the leader.
    cluster.start_election(leader_id);

    cluster
        .deliver_to(follower_a)
        .expect("follower A should receive RequestVote");

    cluster
        .deliver_to(follower_b)
        .expect("follower B should receive RequestVote");

    cluster
        .deliver_to(leader_id)
        .expect("leader should receive vote");

    cluster
        .deliver_to(leader_id)
        .expect("leader should receive vote");

    assert_eq!(
        cluster
            .node(leader_id)
            .expect("leader should exist")
            .role(),
        rustyraft::raft::Role::Leader
    );

    // Append a command to the leader.
    let index = cluster
        .node_mut(leader_id)
        .expect("leader should exist")
        .append_entry("A".to_string())
        .expect("leader should accept command");

    assert_eq!(
        index,
        LogIndex::new(1)
    );

    // Send the first replication attempt.
    assert!(
        cluster.send_append_entries(
            leader_id,
            follower_a
        )
    );

    // Drop the first attempt.
    assert!(
        cluster.drop_to(follower_a)
    );

    assert_eq!(
        cluster
            .node(follower_a)
            .expect("follower A should exist")
            .log()
            .last_index(),
        LogIndex::ZERO
    );

    // Send another AppendEntries request. The follower has not
    // received the previous request, so the leader must retry.
    assert!(
        cluster.send_append_entries(
            leader_id,
            follower_a
        )
    );

    // Deliver the retry.
    cluster
        .deliver_to(follower_a)
        .expect("follower A should receive retry");

    assert_eq!(
        cluster
            .node(follower_a)
            .expect("follower A should exist")
            .log()
            .last_index(),
        LogIndex::new(1)
    );

    // Deliver the successful response to the leader.
    cluster
        .deliver_to(leader_id)
        .expect("leader should receive replication response");

    assert_eq!(
        cluster
            .node(leader_id)
            .expect("leader should exist")
            .commit_index(),
        LogIndex::new(1)
    );

    assert_eq!(
        cluster
            .node(leader_id)
            .expect("leader should exist")
            .last_applied(),
        LogIndex::new(1)
    );

    let progress = cluster
        .node(leader_id)
        .expect("leader should exist")
        .follower_progress(follower_a)
        .expect("leader should track follower A");

    assert_eq!(
        progress.match_index,
        LogIndex::new(1)
    );
}

/// Verifies an election can be driven entirely by logical time.
#[test]
fn cluster_elects_leader_after_election_timeout() {
    let candidate_id = ServerId::new(1);
    let follower_a = ServerId::new(2);
    let follower_b = ServerId::new(3);

    let mut cluster = TestCluster::<String>::new(&[
        candidate_id,
        follower_a,
        follower_b,
    ]);

    // Tick only the candidate so the other nodes do not
    // start competing elections.
    for _ in 0..4 {
        cluster.tick(candidate_id);

        assert_eq!(
            cluster.transport().pending_count(),
            0,
        );
    }

    assert_eq!(
        cluster
            .node(candidate_id)
            .expect("candidate should exist")
            .role(),
        Role::Follower,
    );

    // The fifth tick expires the election timer.
    cluster.tick(candidate_id);

    assert_eq!(
        cluster
            .node(candidate_id)
            .expect("candidate should exist")
            .role(),
        Role::Candidate,
    );

    assert_eq!(
        cluster
            .node(candidate_id)
            .expect("candidate should exist")
            .current_term()
            .value(),
        1,
    );

    // The candidate sends one RequestVote RPC to each follower.
    assert_eq!(
        cluster.transport().pending_count(),
        2,
    );

    // Deliver both RequestVote RPCs.
    cluster
        .deliver_next()
        .expect("first RequestVote should exist");

    cluster
        .deliver_next()
        .expect("second RequestVote should exist");

    // Both followers have now sent their vote responses.
    assert_eq!(
        cluster.transport().pending_count(),
        2,
    );

    // The candidate already has its own vote.
    // One additional granted vote gives it a majority of 2/3.
    cluster
        .deliver_next()
        .expect("first vote response should exist");

    assert_eq!(
        cluster
            .node(candidate_id)
            .expect("leader should exist")
            .role(),
        Role::Leader,
    );

    assert_eq!(
        cluster
            .node(candidate_id)
            .expect("leader should exist")
            .current_term()
            .value(),
        1,
    );

    // The remaining vote response may still arrive after the node
    // has become leader. It must not change the leader's state.
    cluster
        .deliver_next()
        .expect("second vote response should exist");

    assert_eq!(
        cluster
            .node(candidate_id)
            .expect("leader should exist")
            .role(),
        Role::Leader,
    );

    // The new leader's initial heartbeat is generated on its next
    // logical tick.
    cluster.tick(candidate_id);

    assert_eq!(
        cluster.transport().pending_count(),
        2,
    );
}

/// Verifies step drives a leader heartbeat through the transport.
#[test]
fn cluster_step_delivers_leader_heartbeat() {
    let leader_id = ServerId::new(1);
    let follower_a = ServerId::new(2);
    let follower_b = ServerId::new(3);

    let mut cluster = TestCluster::<String>::new(&[
        leader_id,
        follower_a,
        follower_b,
    ]);

    // Start a real election through the transport.
    cluster.start_election(leader_id);

    // Deliver both RequestVote RPCs.
    cluster
        .deliver_next()
        .expect("first RequestVote should exist");

    cluster
        .deliver_next()
        .expect("second RequestVote should exist");

    // Deliver both vote responses.
    //
    // The first response gives the candidate a majority because
    // it already voted for itself.
    cluster
        .deliver_next()
        .expect("first vote response should exist");

    assert_eq!(
        cluster
            .node(leader_id)
            .expect("leader should exist")
            .role(),
        Role::Leader,
    );

    cluster
        .deliver_next()
        .expect("second vote response should exist");

    assert_eq!(
        cluster
            .node(leader_id)
            .expect("leader should exist")
            .role(),
        Role::Leader,
    );

    // The transport should now be idle.
    assert_eq!(
        cluster.transport().pending_count(),
        0,
    );

    // step() advances the leader and delivers one generated
    // heartbeat through the transport.
    let delivery = cluster
        .step(leader_id)
        .expect("leader should send a heartbeat");

    assert_eq!(
        delivery.from,
        leader_id,
    );

    assert_ne!(
        delivery.to,
        leader_id,
    );

    // Delivering the heartbeat immediately generated a response
    // from the follower. The other heartbeat is still pending.
    assert_eq!(
        cluster.transport().pending_count(),
        2,
    );
}

/// Verifies a delivered leader heartbeat prevents a follower election.
#[test]
fn cluster_heartbeat_prevents_follower_election() {
    let leader_id = ServerId::new(1);
    let follower_a = ServerId::new(2);
    let follower_b = ServerId::new(3);

    let mut cluster = TestCluster::<String>::new(&[
        leader_id,
        follower_a,
        follower_b,
    ]);

    // Start a real election through the cluster transport.
    cluster.start_election(leader_id);

    // Deliver both RequestVote RPCs.
    cluster
        .deliver_next()
        .expect("first RequestVote should exist");

    cluster
        .deliver_next()
        .expect("second RequestVote should exist");

    // Deliver the first vote response. The candidate already
    // voted for itself, so this gives it a majority.
    cluster
        .deliver_next()
        .expect("first vote response should exist");

    assert_eq!(
        cluster
            .node(leader_id)
            .expect("leader should exist")
            .role(),
        Role::Leader,
    );

    // Deliver the remaining vote response.
    cluster
        .deliver_next()
        .expect("second vote response should exist");

    assert_eq!(
        cluster
            .node(leader_id)
            .expect("leader should exist")
            .role(),
        Role::Leader,
    );

    // Drive the leader once. This generates and delivers one
    // initial heartbeat to a follower.
    let delivery = cluster
        .step(leader_id)
        .expect("leader should send a heartbeat");

    assert_eq!(
        delivery.from,
        leader_id,
    );

    let follower_id = delivery.to;

    assert_ne!(
        follower_id,
        leader_id,
    );

    assert_eq!(
        cluster
            .node(follower_id)
            .expect("follower should exist")
            .role(),
        Role::Follower,
    );

    // The heartbeat reset the follower's election timer.
    // Four more ticks must not start an election.
    for _ in 0..4 {
        cluster.tick(follower_id);
    }

    assert_eq!(
        cluster
            .node(follower_id)
            .expect("follower should exist")
            .role(),
        Role::Follower,
    );

    assert_eq!(
        cluster
            .node(follower_id)
            .expect("follower should exist")
            .current_term()
            .value(),
        1,
    );
}


/// Verifies a client command is replicated and committed on a majority.
#[test]
fn cluster_replicates_command_and_commits_to_majority() {
    let leader_id = ServerId::new(1);
    let follower_a = ServerId::new(2);
    let follower_b = ServerId::new(3);

    let mut cluster = TestCluster::<String>::new(&[
        leader_id,
        follower_a,
        follower_b,
    ]);

    // Elect the leader through real RequestVote RPCs.
    cluster.start_election(leader_id);

    for _ in 0..4 {
        cluster
            .deliver_next()
            .expect("election message should exist");
    }

    assert_eq!(
        cluster
            .node(leader_id)
            .expect("leader should exist")
            .role(),
        Role::Leader,
    );

    // Append a client command to the elected leader.
    let index = cluster
        .node_mut(leader_id)
        .expect("leader should exist")
        .append_entry("A".to_string())
        .expect("leader should accept command");

    assert_eq!(
        index,
        LogIndex::new(1),
    );

    assert_eq!(
        cluster
            .node(leader_id)
            .expect("leader should exist")
            .commit_index(),
        LogIndex::ZERO,
    );

    // step() advances the leader and delivers one AppendEntries RPC.
    cluster
        .step(leader_id)
        .expect("leader should send AppendEntries");

    // Process the remaining replication and response messages.
    for _ in 0..4 {
        if cluster
            .node(leader_id)
            .expect("leader should exist")
            .commit_index()
            == LogIndex::new(1)
        {
            break;
        }

        cluster
            .deliver_next()
            .expect("replication message should exist");
    }

    assert_eq!(
        cluster
            .node(leader_id)
            .expect("leader should exist")
            .commit_index(),
        LogIndex::new(1),
    );

    assert_eq!(
        cluster
            .node(leader_id)
            .expect("leader should exist")
            .last_applied(),
        LogIndex::new(1),
    );

    // Drain the remaining replication messages.
    while cluster.transport().has_pending() {
        cluster
            .deliver_next()
            .expect("pending replication message should exist");
    }

    // Both followers should eventually receive the command.
    assert_eq!(
        cluster
            .node(follower_a)
            .expect("follower should exist")
            .log()
            .last_index(),
        LogIndex::new(1),
    );

    assert_eq!(
        cluster
            .node(follower_b)
            .expect("follower should exist")
            .log()
            .last_index(),
        LogIndex::new(1),
    );
}

/// Verifies a leader commits when one follower is unreachable.
#[test]
fn cluster_commits_command_with_one_follower_unavailable() {
    let leader_id = ServerId::new(1);
    let follower_a = ServerId::new(2);
    let follower_b = ServerId::new(3);

    let mut cluster = TestCluster::<String>::new(&[
        leader_id,
        follower_a,
        follower_b,
    ]);

    // Elect the leader.
    cluster.start_election(leader_id);

    for _ in 0..4 {
        cluster
            .deliver_next()
            .expect("election message should exist");
    }

    assert_eq!(
        cluster
            .node(leader_id)
            .expect("leader should exist")
            .role(),
        Role::Leader,
    );

    // Append a command to the leader.
    cluster
        .node_mut(leader_id)
        .expect("leader should exist")
        .append_entry("A".to_string())
        .expect("leader should accept command");

    // Generate AppendEntries for both followers.
    cluster.tick(leader_id);

    assert_eq!(
        cluster.transport().pending_count(),
        2,
    );

    // Drop every message currently destined for follower B.
    assert!(
        cluster.drop_to(follower_b),
        "follower B should have a pending message"
    );

    // The remaining AppendEntries goes to follower A.
    cluster
        .deliver_next()
        .expect("replication should reach follower A");

    // Follower A sends the successful response back to the leader.
    cluster
        .deliver_next()
        .expect("leader should receive replication response");

    // Leader + follower A = 2/3, which is a majority.
    assert_eq!(
        cluster
            .node(leader_id)
            .expect("leader should exist")
            .commit_index(),
        LogIndex::new(1),
    );

    assert_eq!(
        cluster
            .node(follower_a)
            .expect("follower A should exist")
            .log()
            .last_index(),
        LogIndex::new(1),
    );

    // Follower B never received the command.
    assert_eq!(
        cluster
            .node(follower_b)
            .expect("follower B should exist")
            .log()
            .last_index(),
        LogIndex::ZERO,
    );
}

/// Verifies a follower starts a new election when heartbeats stop.
#[test]
fn cluster_follower_starts_election_without_heartbeat() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);
    let other_follower = ServerId::new(3);

    let mut cluster = TestCluster::<String>::new(&[
        leader_id,
        follower_id,
        other_follower,
    ]);

    // Elect the leader.
    cluster.start_election(leader_id);

    for _ in 0..4 {
        cluster
            .deliver_next()
            .expect("election message should exist");
    }

    assert_eq!(
        cluster
            .node(leader_id)
            .expect("leader should exist")
            .role(),
        Role::Leader,
    );

    assert_eq!(
        cluster
            .node(follower_id)
            .expect("follower should exist")
            .role(),
        Role::Follower,
    );

    // The follower has not received a heartbeat.
    //
    // Four ticks are still below the election timeout.
    for _ in 0..4 {
        cluster.tick(follower_id);

        assert_eq!(
            cluster
                .node(follower_id)
                .expect("follower should exist")
                .role(),
            Role::Follower,
        );
    }

    assert_eq!(
        cluster
            .node(follower_id)
            .expect("follower should exist")
            .current_term()
            .value(),
        1,
    );

    // The fifth tick expires the election timer.
    cluster.tick(follower_id);

    assert_eq!(
        cluster
            .node(follower_id)
            .expect("candidate should exist")
            .role(),
        Role::Candidate,
    );

    assert_eq!(
        cluster
            .node(follower_id)
            .expect("candidate should exist")
            .current_term()
            .value(),
        2,
    );

    // The new candidate sends RequestVote to the other two servers.
    assert_eq!(
        cluster.transport().pending_count(),
        2,
    );
}