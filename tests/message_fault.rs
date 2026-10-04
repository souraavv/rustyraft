use rustyraft::raft::{
    LogIndex,
    Role,
    ServerId,
    Term,
};

mod supports;

use supports::cluster::TestCluster;

#[test]
fn dropped_request_vote_does_not_complete_election() {
    let server1 = ServerId::new(1);
    let server2 = ServerId::new(2);
    let server3 = ServerId::new(3);

    let mut cluster = TestCluster::new(&[
        server1,
        server2,
        server3,
    ]);

    // Prevent server1 from reaching server2.
    cluster.partition(
        server1,
        server2,
    );

    // Start an election on server1.
    cluster.start_election(server1);

    // The RequestVote message to server2 is blocked.
    assert!(
        cluster
            .deliver_matching(|message| {
                message.from == server1
                    && message.to == server2
            })
            .is_none(),
        "RequestVote to server2 should be dropped",
    );

    // server3 is still reachable and should receive
    // the RequestVote request.
    cluster
        .deliver_matching(|message| {
            message.from == server1
                && message.to == server3
        })
        .expect(
            "server3 should receive RequestVote",
        );

    // server1 cannot become leader with only its own
    // vote because server2 is unreachable and the
    // vote from server3 has not been delivered yet.
    assert_ne!(
        cluster
            .node(server1)
            .unwrap()
            .role(),
        Role::Leader,
    );

    // Deliver server3's vote.
    cluster
        .deliver_matching(|message| {
            message.from == server3
                && message.to == server1
        })
        .expect(
            "server1 should receive server3's vote",
        );

    // In a three-node cluster, server1 actually has
    // a majority with its own vote plus server3's vote.
    //
    // Therefore the election should complete even though
    // the RequestVote message to server2 was lost.
    assert_eq!(
        cluster
            .node(server1)
            .unwrap()
            .role(),
        Role::Leader,
    );
}

#[test]
fn dropped_append_entries_does_not_reach_follower() {
    let server1 = ServerId::new(1);
    let server2 = ServerId::new(2);
    let server3 = ServerId::new(3);

    let mut cluster = TestCluster::new(&[
        server1,
        server2,
        server3,
    ]);

    // Elect server1 as leader.
    cluster.start_election(server1);

    cluster
        .deliver_next()
        .expect(
            "server2 should receive RequestVote",
        );

    cluster
        .deliver_next()
        .expect(
            "server3 should receive RequestVote",
        );

    cluster
        .deliver_matching(|message| {
            message.from == server2
                && message.to == server1
        })
        .expect(
            "server1 should receive vote from server2",
        );

    assert_eq!(
        cluster
            .node(server1)
            .unwrap()
            .role(),
        Role::Leader,
    );

    // Append an entry only to the leader.
    assert_eq!(
        cluster
            .node_mut(server1)
            .unwrap()
            .append_entry("A".to_string()),
        Some(LogIndex::new(1)),
    );

    // Block the leader -> follower direction.
    cluster.partition(
        server1,
        server2,
    );

    // The leader can still build the AppendEntries
    // request, but the transport must drop it.
    assert!(
        cluster.send_append_entries(
            server1,
            server2,
        ),
        "leader should be able to build AppendEntries",
    );

    // The message must not reach server2.
    assert!(
        cluster
            .deliver_matching(|message| {
                message.from == server1
                    && message.to == server2
            })
            .is_none(),
        "partitioned AppendEntries should not reach server2",
    );

    // server2 must still have an empty log.
    assert_eq!(
        cluster
            .node(server2)
            .unwrap()
            .last_log_index(),
        LogIndex::ZERO,
    );
}

#[test]
fn delayed_append_entries_is_processed_after_other_messages() {
    let server1 = ServerId::new(1);
    let server2 = ServerId::new(2);
    let server3 = ServerId::new(3);

    let mut cluster = TestCluster::new(&[
        server1,
        server2,
        server3,
    ]);

    // Elect server1 as leader.
    cluster.start_election(server1);

    cluster
        .deliver_next()
        .expect(
            "server2 should receive RequestVote",
        );

    cluster
        .deliver_next()
        .expect(
            "server3 should receive RequestVote",
        );

    cluster
        .deliver_matching(|message| {
            message.from == server2
                && message.to == server1
        })
        .expect(
            "server1 should receive vote from server2",
        );

    assert_eq!(
        cluster
            .node(server1)
            .unwrap()
            .role(),
        Role::Leader,
    );

    assert_eq!(
        cluster
            .node(server1)
            .unwrap()
            .current_term(),
        Term::new(1),
    );

    // Append entry A to the leader.
    assert_eq!(
        cluster
            .node_mut(server1)
            .unwrap()
            .append_entry("A".to_string()),
        Some(LogIndex::new(1)),
    );

    // Build an AppendEntries message for server2.
    //
    // This message will remain queued and intentionally
    // will not be delivered yet.
    assert!(
        cluster.send_append_entries(
            server1,
            server2,
        ),
        "leader should build AppendEntries for server2",
    );

    // Build another independent AppendEntries message
    // for server3.
    //
    // Both messages are now in the transport queue.
    assert!(
        cluster.send_append_entries(
            server1,
            server3,
        ),
        "leader should build AppendEntries for server3",
    );

    // Deliver the message to server3 first.
    //
    // This deliberately reorders messages without changing
    // the current Raft term.
    cluster
        .deliver_matching(|message| {
            message.from == server1
                && message.to == server3
        })
        .expect(
            "server3 should receive its AppendEntries first",
        );

    assert_eq!(
        cluster
            .node(server3)
            .unwrap()
            .last_log_index(),
        LogIndex::new(1),
    );

    assert_eq!(
        cluster
            .node(server3)
            .unwrap()
            .log_at(
                LogIndex::new(1),
            )
            .unwrap()
            .command,
        "A",
    );

    // The AppendEntries for server2 was delayed.
    //
    // It should still be pending in the transport and must
    // now be processed successfully.
    cluster
        .deliver_matching(|message| {
            message.from == server1
                && message.to == server2
        })
        .expect(
            "server2 should eventually receive delayed AppendEntries",
        );

    assert_eq!(
        cluster
            .node(server2)
            .unwrap()
            .last_log_index(),
        LogIndex::new(1),
    );

    assert_eq!(
        cluster
            .node(server2)
            .unwrap()
            .log_at(
                LogIndex::new(1),
            )
            .unwrap()
            .command,
        "A",
    );

    // The leader and both followers should still be
    // operating in the same term.
    assert_eq!(
        cluster
            .node(server1)
            .unwrap()
            .current_term(),
        Term::new(1),
    );

    assert_eq!(
        cluster
            .node(server2)
            .unwrap()
            .current_term(),
        Term::new(1),
    );

    assert_eq!(
        cluster
            .node(server3)
            .unwrap()
            .current_term(),
        Term::new(1),
    );
}

#[test]
fn lost_append_entries_can_be_sent_again_after_recovery() {
    let server1 = ServerId::new(1);
    let server2 = ServerId::new(2);
    let server3 = ServerId::new(3);

    let mut cluster = TestCluster::new(&[
        server1,
        server2,
        server3,
    ]);

    // Elect server1 as leader.
    cluster.start_election(server1);

    cluster
        .deliver_next()
        .expect(
            "server2 should receive RequestVote",
        );

    cluster
        .deliver_next()
        .expect(
            "server3 should receive RequestVote",
        );

    cluster
        .deliver_matching(|message| {
            message.from == server2
                && message.to == server1
        })
        .expect(
            "server1 should receive vote from server2",
        );

    assert_eq!(
        cluster
            .node(server1)
            .unwrap()
            .role(),
        Role::Leader,
    );

    // Append a command to the leader.
    assert_eq!(
        cluster
            .node_mut(server1)
            .unwrap()
            .append_entry("A".to_string()),
        Some(LogIndex::new(1)),
    );

    // Partition server1 -> server2.
    cluster.partition(
        server1,
        server2,
    );

    // The request is successfully built but dropped
    // by the transport.
    assert!(
        cluster.send_append_entries(
            server1,
            server2,
        ),
        "leader should be able to build AppendEntries",
    );

    assert!(
        cluster
            .deliver_matching(|message| {
                message.from == server1
                    && message.to == server2
            })
            .is_none(),
        "lost AppendEntries must not reach server2",
    );

    assert_eq!(
        cluster
            .node(server2)
            .unwrap()
            .last_log_index(),
        LogIndex::ZERO,
    );

    // Restore communication.
    cluster.heal(
        server1,
        server2,
    );

    // The leader can produce the replication request again.
    assert!(
        cluster.send_append_entries(
            server1,
            server2,
        ),
        "leader should be able to build AppendEntries again",
    );

    cluster
        .deliver_matching(|message| {
            message.from == server1
                && message.to == server2
        })
        .expect(
            "server2 should receive the retransmitted AppendEntries",
        );

    assert_eq!(
        cluster
            .node(server2)
            .unwrap()
            .last_log_index(),
        LogIndex::new(1),
    );

    assert_eq!(
        cluster
            .node(server2)
            .unwrap()
            .log_at(
                LogIndex::new(1),
            )
            .unwrap()
            .command,
        "A",
    );
}

#[test]
fn duplicate_append_entries_response_is_safe() {
    let server1 = ServerId::new(1);
    let server2 = ServerId::new(2);
    let server3 = ServerId::new(3);

    let mut cluster = TestCluster::new(&[
        server1,
        server2,
        server3,
    ]);

    // Elect server1 as leader.
    cluster.start_election(server1);

    cluster
        .deliver_next()
        .expect(
            "server2 should receive RequestVote",
        );

    cluster
        .deliver_next()
        .expect(
            "server3 should receive RequestVote",
        );

    cluster
        .deliver_matching(|message| {
            message.from == server2
                && message.to == server1
        })
        .expect(
            "server1 should receive vote from server2",
        );

    assert_eq!(
        cluster
            .node(server1)
            .unwrap()
            .role(),
        Role::Leader,
    );

    // Append an entry to the leader.
    assert_eq!(
        cluster
            .node_mut(server1)
            .unwrap()
            .append_entry("A".to_string()),
        Some(LogIndex::new(1)),
    );

    // Send AppendEntries to server2.
    assert!(
        cluster.send_append_entries(
            server1,
            server2,
        ),
        "leader should be able to build AppendEntries",
    );

    // Deliver the request to server2.
    //
    // This causes server2 to generate the real
    // AppendEntriesResponse.
    cluster
        .deliver_matching(|message| {
            message.from == server1
                && message.to == server2
        })
        .expect(
            "server2 should receive AppendEntries",
        );

    assert_eq!(
        cluster
            .node(server2)
            .unwrap()
            .last_log_index(),
        LogIndex::new(1),
    );

    // Duplicate the response generated by server2.
    //
    // The original response remains queued and the
    // duplicate is added as another pending message.
    assert!(
        cluster.duplicate_matching(|message| {
            message.from == server2
                && message.to == server1
        }),
        "AppendEntriesResponse should be pending",
    );

    // Deliver the original response.
    cluster
        .deliver_matching(|message| {
            message.from == server2
                && message.to == server1
        })
        .expect(
            "server1 should receive the original response",
        );

    assert_eq!(
        cluster
            .node(server1)
            .unwrap()
            .role(),
        Role::Leader,
    );

    // Deliver the duplicate response.
    //
    // Processing the same successful response twice must
    // be harmless.
    cluster
        .deliver_matching(|message| {
            message.from == server2
                && message.to == server1
        })
        .expect(
            "server1 should receive the duplicate response",
        );

    assert_eq!(
        cluster
            .node(server1)
            .unwrap()
            .role(),
        Role::Leader,
    );

    // The follower's replicated log must remain unchanged.
    assert_eq!(
        cluster
            .node(server2)
            .unwrap()
            .last_log_index(),
        LogIndex::new(1),
    );

    assert_eq!(
        cluster
            .node(server2)
            .unwrap()
            .log_at(
                LogIndex::new(1),
            )
            .unwrap()
            .command,
        "A",
    );
}