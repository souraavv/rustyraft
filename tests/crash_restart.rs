use rustyraft::raft::{
    LogIndex,
    Role,
    ServerId,
    Term,
};

mod supports;

use supports::cluster::TestCluster;

#[test]
fn crashed_leader_restarts_with_persistent_state() {
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

    // Append an entry only to the leader.
    //
    // It is intentionally not replicated or committed.
    assert_eq!(
        cluster
            .node_mut(server1)
            .unwrap()
            .append_entry("A".to_string()),
        Some(LogIndex::new(1)),
    );

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
            .commit_index(),
        LogIndex::ZERO,
    );

    // Crash server1.
    //
    // Persistent storage is retained.
    let storage = cluster
        .crash_node(server1)
        .expect(
            "server1 should exist before crash",
        );

    assert!(
        cluster.node(server1).is_none()
    );

    // Elect server2 while server1 is down.
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

    // Restart server1 using its persistent storage.
    cluster.restart_node(
        server1,
        storage,
    );

    assert_eq!(
        cluster
            .node(server1)
            .unwrap()
            .role(),
        Role::Follower,
    );

    // The term persisted before the crash.
    assert_eq!(
        cluster
            .node(server1)
            .unwrap()
            .current_term(),
        Term::new(1),
    );

    // The uncommitted log entry also survived the crash.
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
        "A",
    );

    // The entry was never committed before the crash.
    assert_eq!(
        cluster
            .node(server1)
            .unwrap()
            .commit_index(),
        LogIndex::ZERO,
    );

    // The new leader should be able to communicate
    // with the restarted server.
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

    // server1 must update to the newer term after
    // receiving AppendEntries from server2.
    assert_eq!(
        cluster
            .node(server1)
            .unwrap()
            .current_term(),
        Term::new(2),
    );
}

#[test]
fn crashed_follower_restarts_with_persistent_state() {
    let server1 = ServerId::new(1);
    let server2 = ServerId::new(2);
    let server3 = ServerId::new(3);

    let mut cluster = TestCluster::new(&[
        server1,
        server2,
        server3,
    ]);

    // Elect server1 as the leader.
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

    // Replicate an entry to server2.
    assert_eq!(
        cluster
            .node_mut(server1)
            .unwrap()
            .append_entry("A".to_string()),
        Some(LogIndex::new(1)),
    );

    assert!(
        cluster.send_append_entries(
            server1,
            server2,
        ),
        "leader should build AppendEntries for server2",
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
        LogIndex::new(1),
    );

    // Crash the follower.
    //
    // Its persistent state must survive the crash.
    let storage = cluster
        .crash_node(server2)
        .expect(
            "server2 should exist before crash",
        );

    assert!(
        cluster.node(server2).is_none()
    );

    // Restart server2 from its persistent storage.
    cluster.restart_node(
        server2,
        storage,
    );

    assert_eq!(
        cluster
            .node(server2)
            .unwrap()
            .role(),
        Role::Follower,
    );

    assert_eq!(
        cluster
            .node(server2)
            .unwrap()
            .current_term(),
        Term::new(1),
    );

    // The replicated entry must survive the crash.
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
fn restarted_node_updates_to_newer_term() {
    let server1 = ServerId::new(1);
    let server2 = ServerId::new(2);
    let server3 = ServerId::new(3);

    let mut cluster = TestCluster::new(&[
        server1,
        server2,
        server3,
    ]);

    // Elect server1 in term 1.
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

    // Crash server1 while it is still in term 1.
    let storage = cluster
        .crash_node(server1)
        .expect(
            "server1 should exist before crash",
        );

    // Elect server2 in term 2.
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

    // Restart server1 with its old persistent term.
    cluster.restart_node(
        server1,
        storage,
    );

    assert_eq!(
        cluster
            .node(server1)
            .unwrap()
            .current_term(),
        Term::new(1),
    );

    // A message from the current leader must move
    // server1 to the newer term.
    assert!(
        cluster.send_append_entries(
            server2,
            server1,
        ),
        "server2 should build AppendEntries for server1",
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
            .current_term(),
        Term::new(2),
    );

    assert_eq!(
        cluster
            .node(server1)
            .unwrap()
            .role(),
        Role::Follower,
    );
}

#[test]
fn restarted_follower_rejoins_replication() {
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

    // Append and replicate the first entry to server2.
    assert_eq!(
        cluster
            .node_mut(server1)
            .unwrap()
            .append_entry("A".to_string()),
        Some(LogIndex::new(1)),
    );

    assert!(
        cluster.send_append_entries(
            server1,
            server2,
        ),
        "server1 should build AppendEntries",
    );

    cluster
        .deliver_matching(|message| {
            message.from == server1
                && message.to == server2
        })
        .expect(
            "server2 should receive AppendEntries",
        );

    // Crash server2 after it has replicated entry A.
    let storage = cluster
        .crash_node(server2)
        .expect(
            "server2 should exist before crash",
        );

    // Leader continues operating while server2 is down.
    assert_eq!(
        cluster
            .node_mut(server1)
            .unwrap()
            .append_entry("B".to_string()),
        Some(LogIndex::new(2)),
    );

    assert_eq!(
        cluster
            .node(server1)
            .unwrap()
            .last_log_index(),
        LogIndex::new(2),
    );

    // Restart server2 from its persistent state.
    cluster.restart_node(
        server2,
        storage,
    );

    assert_eq!(
        cluster
            .node(server2)
            .unwrap()
            .last_log_index(),
        LogIndex::new(1),
    );

    // The restarted follower should be reachable by the leader.
    assert!(
        cluster.send_append_entries(
            server1,
            server2,
        ),
        "leader should build AppendEntries for restarted follower",
    );

    cluster
        .deliver_matching(|message| {
            message.from == server1
                && message.to == server2
        })
        .expect(
            "restarted follower should receive AppendEntries",
        );

    // The follower should now contain the missing entry.
    assert_eq!(
        cluster
            .node(server2)
            .unwrap()
            .last_log_index(),
        LogIndex::new(2),
    );

    assert_eq!(
        cluster
            .node(server2)
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
fn restarted_follower_recovers_missing_entries() {
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

    // Replicate the first entry before the follower crashes.
    assert_eq!(
        cluster
            .node_mut(server1)
            .unwrap()
            .append_entry("A".to_string()),
        Some(LogIndex::new(1)),
    );

    assert!(
        cluster.send_append_entries(
            server1,
            server2,
        ),
        "leader should build AppendEntries",
    );

    cluster
        .deliver_matching(|message| {
            message.from == server1
                && message.to == server2
        })
        .expect(
            "server2 should receive first entry",
        );

    // Crash server2.
    let storage = cluster
        .crash_node(server2)
        .expect(
            "server2 should exist before crash",
        );

    // Leader appends more entries while server2 is down.
    assert_eq!(
        cluster
            .node_mut(server1)
            .unwrap()
            .append_entry("B".to_string()),
        Some(LogIndex::new(2)),
    );

    assert_eq!(
        cluster
            .node_mut(server1)
            .unwrap()
            .append_entry("C".to_string()),
        Some(LogIndex::new(3)),
    );

    // Restart server2.
    cluster.restart_node(
        server2,
        storage,
    );

    assert_eq!(
        cluster
            .node(server2)
            .unwrap()
            .last_log_index(),
        LogIndex::new(1),
    );

    // The leader should send the missing entries.
    assert!(
        cluster.send_append_entries(
            server1,
            server2,
        ),
        "leader should build AppendEntries for restarted follower",
    );

    cluster
        .deliver_matching(|message| {
            message.from == server1
                && message.to == server2
        })
        .expect(
            "server2 should receive missing entries",
        );

    assert_eq!(
        cluster
            .node(server2)
            .unwrap()
            .last_log_index(),
        LogIndex::new(3),
    );

    assert_eq!(
        cluster
            .node(server2)
            .unwrap()
            .log_at(
                LogIndex::new(2),
            )
            .unwrap()
            .command,
        "B",
    );

    assert_eq!(
        cluster
            .node(server2)
            .unwrap()
            .log_at(
                LogIndex::new(3),
            )
            .unwrap()
            .command,
        "C",
    );
}