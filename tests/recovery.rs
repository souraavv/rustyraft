use rustyraft::raft::{
    LogIndex,
    Role,
    ServerId,
    Term,
};

mod supports;

use supports::cluster::TestCluster;

#[test]
fn uncommitted_entry_is_replaced_by_new_leader() {
    let server1 = ServerId::new(1);
    let server2 = ServerId::new(2);
    let server3 = ServerId::new(3);

    let mut cluster = TestCluster::new(&[
        server1,
        server2,
        server3,
    ]);

    // Elect server1 as the initial leader.
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

    // Append an entry only to the old leader.
    //
    // The entry is intentionally not replicated to a
    // majority and therefore must remain uncommitted.
    assert_eq!(
        cluster
            .node_mut(server1)
            .unwrap()
            .append_entry("B".to_string()),
        Some(LogIndex::new(1)),
    );

    assert_eq!(
        cluster
            .node(server1)
            .unwrap()
            .commit_index(),
        LogIndex::ZERO,
    );

    // Isolate the old leader from the other servers.
    cluster.partition_bidirectional(
        server1,
        server2,
    );

    cluster.partition_bidirectional(
        server1,
        server3,
    );

    // The majority partition elects server2.
    cluster.start_election(server2);

    cluster
        .deliver_matching(|message| {
            message.from == server2
                && message.to == server3
        })
        .expect(
            "server3 should receive RequestVote",
        );

    cluster
        .deliver_matching(|message| {
            message.from == server3
                && message.to == server2
        })
        .expect(
            "server2 should receive vote from server3",
        );

    assert_eq!(
        cluster
            .node(server2)
            .unwrap()
            .role(),
        Role::Leader,
    );

    assert_eq!(
        cluster
            .node(server2)
            .unwrap()
            .current_term(),
        Term::new(2),
    );

    // The new leader has an empty log at this point.
    assert_eq!(
        cluster
            .node(server2)
            .unwrap()
            .last_log_index(),
        LogIndex::ZERO,
    );

    // Append a new entry to the new leader.
    //
    // This entry is intentionally at the same index as
    // the uncommitted entry that exists only on server1.
    assert_eq!(
        cluster
            .node_mut(server2)
            .unwrap()
            .append_entry("C".to_string()),
        Some(LogIndex::new(1)),
    );

    assert_eq!(
        cluster
            .node(server2)
            .unwrap()
            .last_log_index(),
        LogIndex::new(1),
    );

    // The old leader still has its uncommitted entry B.
    assert_eq!(
        cluster
            .node(server1)
            .unwrap()
            .last_log_index(),
        LogIndex::new(1),
    );

    assert_eq!(
        cluster
            .node(server1)
            .unwrap()
            .log_at(
                LogIndex::new(1),
            )
            .unwrap()
            .command,
        "B",
    );

    // Heal communication between the new leader and
    // the old leader.
    cluster.heal_bidirectional(
        server1,
        server2,
    );

    // The new leader must overwrite the old leader's
    // uncommitted entry.
    assert!(
        cluster.send_append_entries(
            server2,
            server1,
        ),
        "server2 should be able to build AppendEntries",
    );

    cluster
        .deliver_matching(|message| {
            message.from == server2
                && message.to == server1
        })
        .expect(
            "server1 should receive AppendEntries",
        );

    assert_eq!(
        cluster
            .node(server1)
            .unwrap()
            .role(),
        Role::Follower,
    );

    assert_eq!(
        cluster
            .node(server1)
            .unwrap()
            .current_term(),
        Term::new(2),
    );

    // The old uncommitted entry B must have been replaced
    // by the new leader's entry C.
    assert_eq!(
        cluster
            .node(server1)
            .unwrap()
            .last_log_index(),
        LogIndex::new(1),
    );

    assert_eq!(
        cluster
            .node(server1)
            .unwrap()
            .log_at(
                LogIndex::new(1),
            )
            .unwrap()
            .command,
        "C",
    );
}

#[test]
fn committed_entry_is_never_replaced() {
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

    // Append entry A to the leader.
    assert_eq!(
        cluster
            .node_mut(server1)
            .unwrap()
            .append_entry("A".to_string()),
        Some(LogIndex::new(1)),
    );

    // Replicate A to server2.
    assert!(
        cluster.send_append_entries(
            server1,
            server2,
        ),
        "server1 should be able to build AppendEntries",
    );

    cluster
        .deliver_matching(|message| {
            message.from == server1
                && message.to == server2
        })
        .expect(
            "server2 should receive entry A",
        );

    // Replicate A to server3.
    assert!(
        cluster.send_append_entries(
            server1,
            server3,
        ),
        "server1 should be able to build AppendEntries",
    );

    cluster
        .deliver_matching(|message| {
            message.from == server1
                && message.to == server3
        })
        .expect(
            "server3 should receive entry A",
        );

    // A is now replicated to a majority.
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

    // Isolate server1 from the majority.
    cluster.partition_bidirectional(
        server1,
        server2,
    );

    cluster.partition_bidirectional(
        server1,
        server3,
    );

    // Elect server2 in the majority partition.
    cluster.start_election(server2);

    cluster
        .deliver_matching(|message| {
            message.from == server2
                && message.to == server3
        })
        .expect(
            "server3 should receive RequestVote",
        );

    cluster
        .deliver_matching(|message| {
            message.from == server3
                && message.to == server2
        })
        .expect(
            "server2 should receive vote from server3",
        );

    assert_eq!(
        cluster
            .node(server2)
            .unwrap()
            .role(),
        Role::Leader,
    );

    // The new leader must retain the committed entry.
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
}

#[test]
fn follower_catches_up_after_downtime() {
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

    // Temporarily isolate server3.
    cluster.partition_bidirectional(
        server1,
        server3,
    );

    // Append entries while server3 is unavailable.
    assert_eq!(
        cluster
            .node_mut(server1)
            .unwrap()
            .append_entry("A".to_string()),
        Some(LogIndex::new(1)),
    );

    assert_eq!(
        cluster
            .node_mut(server1)
            .unwrap()
            .append_entry("B".to_string()),
        Some(LogIndex::new(2)),
    );

    // Server2 remains reachable and receives the entries.
    assert!(
        cluster.send_append_entries(
            server1,
            server2,
        ),
        "leader should be able to build AppendEntries",
    );

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
        LogIndex::new(2),
    );

    // Heal the connection to server3.
    cluster.heal_bidirectional(
        server1,
        server3,
    );

    // The leader should now bring server3 up to date.
    assert!(
        cluster.send_append_entries(
            server1,
            server3,
        ),
        "leader should be able to build AppendEntries",
    );

    cluster
        .deliver_matching(|message| {
            message.from == server1
                && message.to == server3
        })
        .expect(
            "server3 should receive missing entries",
        );

    assert_eq!(
        cluster
            .node(server3)
            .unwrap()
            .last_log_index(),
        LogIndex::new(2),
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

    assert_eq!(
        cluster
            .node(server3)
            .unwrap()
            .log_at(
                LogIndex::new(2),
            )
            .unwrap()
            .command,
        "B",
    );
}

#[test]
fn leader_retries_replication_after_message_loss() {
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

    // Partition the leader from server2.
    cluster.partition(
        server1,
        server2,
    );

    // The leader can still build the request.
    //
    // The transport drops it because the link is partitioned.
    assert!(
        cluster.send_append_entries(
            server1,
            server2,
        ),
        "leader should be able to build AppendEntries",
    );

    // The dropped message must not reach server2.
    assert!(
        cluster
            .deliver_matching(|message| {
                message.from == server1
                    && message.to == server2
            })
            .is_none(),
        "partitioned AppendEntries should not reach server2",
    );

    assert_eq!(
        cluster
            .node(server2)
            .unwrap()
            .last_log_index(),
        LogIndex::ZERO,
    );

    // Heal the connection.
    cluster.heal(
        server1,
        server2,
    );

    // The leader builds the replication request again.
    assert!(
        cluster.send_append_entries(
            server1,
            server2,
        ),
        "leader should be able to build the retry",
    );

    cluster
        .deliver_matching(|message| {
            message.from == server1
                && message.to == server2
        })
        .expect(
            "server2 should receive the retried AppendEntries",
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