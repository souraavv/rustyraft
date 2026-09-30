mod support;

use rustyraft::raft::{
    Role, ServerId, Term,
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
            .deliver_request_vote_response(
                candidate_id,
            )
    );

    assert_eq!(
        cluster
            .node(candidate_id)
            .unwrap()
            .role(),
        Role::Leader
    );
}