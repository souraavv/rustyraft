use rustyraft::raft::{Role, ServerId};

mod supports;

use supports::cluster::TestCluster;

#[test]
fn bidirectional_partition_blocks_messages_between_servers() {
    let server1 = ServerId::new(1);
    let server2 = ServerId::new(2);
    let server3 = ServerId::new(3);

    let mut cluster =
        TestCluster::new(&[
            server1,
            server2,
            server3,
        ]);

    // Partition server1 from server2 in both directions.
    cluster.partition_bidirectional(
        server1,
        server2,
    );

    // Start an election on server1.
    //
    // server1 will try to send RequestVote messages
    // to server2 and server3.
    cluster.start_election(
        server1,
    );

    // The message to server2 should be blocked by the
    // partition. The message to server3 should still
    // be queued.
    assert_eq!(
        cluster.transport().pending_count(),
        1,
    );

    // The remaining message should be destined for
    // server3.
    let message =
        cluster.transport().peek().expect(
            "RequestVote message to server3 should be pending",
        );

    assert_eq!(
        message.from,
        server1,
    );

    assert_eq!(
        message.to,
        server3,
    );
}

#[test]
fn isolated_leader_loses_majority_and_new_leader_is_elected() {
    let server1 = ServerId::new(1);
    let server2 = ServerId::new(2);
    let server3 = ServerId::new(3);

    let mut cluster =
        TestCluster::new(&[
            server1,
            server2,
            server3,
        ]);

    // Elect server1 as the initial leader.
    cluster.start_election(
        server1,
    );

    // Deliver server1's RequestVote messages and the
    // resulting RequestVote responses until server1
    // becomes leader.
    cluster.deliver_next().expect(
        "server2 should receive RequestVote",
    );

    cluster.deliver_next().expect(
        "server3 should receive RequestVote",
    );

    cluster.deliver_next().expect(
        "server1 should receive vote from server2",
    );

    assert_eq!(
        cluster
            .node(server1)
            .expect("server1 should exist")
            .role(),
        Role::Leader,
    );

    // Isolate the leader from both followers.
    //
    // server2 and server3 can still communicate with
    // each other and therefore still form a majority.
    cluster.partition_bidirectional(
        server1,
        server2,
    );

    cluster.partition_bidirectional(
        server1,
        server3,
    );

    // Start an election on server2.
    //
    // The RequestVote message to server1 is blocked.
    // The RequestVote message to server3 is allowed.
    cluster.start_election(
        server2,
    );

    // Deliver the RequestVote from server2 to server3.
    //
    // There may still be an older message from server3
    // to server1 in the transport queue. We intentionally
    // leave unrelated messages untouched.
    cluster
        .deliver_matching(|message| {
            message.from == server2
                && message.to == server3
        })
        .expect(
            "server3 should receive RequestVote from server2",
        );

    // Deliver server3's vote back to server2.
    //
    // Select the specific response instead of relying
    // on global transport queue ordering.
    cluster
        .deliver_matching(|message| {
            message.from == server3
                && message.to == server2
        })
        .expect(
            "server2 should receive vote from server3",
        );

    // server2 has its own vote plus server3's vote,
    // which is a majority of the three-node cluster.
    assert_eq!(
        cluster
            .node(server2)
            .expect("server2 should exist")
            .role(),
        Role::Leader,
    );

    // The isolated old leader must still be a leader
    // only from its own local point of view.
    //
    // It has not received the newer term yet because
    // communication with server2 and server3 is blocked.
    assert_eq!(
        cluster
            .node(server1)
            .expect("server1 should exist")
            .role(),
        Role::Leader,
    );

    // Heal the network.
    cluster.heal_bidirectional(
        server1,
        server2,
    );

    cluster.heal_bidirectional(
        server1,
        server3,
    );

    // Send AppendEntries from the new leader to the
    // old leader. The request carries server2's newer term.
    assert!(
        cluster.send_append_entries(
            server2,
            server1,
        ),
        "server2 should be able to build AppendEntries"
    );

    // Deliver specifically the AppendEntries from the
    // new leader to the old leader.
    //
    // Other messages for server1 may still be pending.
    // We intentionally leave those messages untouched.
    cluster
        .deliver_matching(|message| {
            message.from == server2
                && message.to == server1
        })
        .expect(
            "server1 should receive AppendEntries from server2",
        );

    // server1 observed the newer term and stepped down.
    assert_eq!(
        cluster
            .node(server1)
            .expect("server1 should exist")
            .role(),
        Role::Follower,
    );
}