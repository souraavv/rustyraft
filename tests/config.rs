use rustyraft::cluster::{ClusterConfig, ClusterConfigError};
use rustyraft::raft::ServerId;

#[test]
fn empty_cluster_is_rejected() {
    let result = ClusterConfig::new(Vec::new());

    assert!(matches!(result, Err(ClusterConfigError::EmptyCluster)));
}

#[test]
fn duplicate_server_is_rejected() {
    let result = ClusterConfig::new(vec![ServerId::new(1), ServerId::new(2), ServerId::new(1)]);

    assert!(matches!(result, Err(ClusterConfigError::DuplicateServer)));
}

#[test]
fn cluster_size_is_correct() {
    let config =
        ClusterConfig::new(vec![ServerId::new(1), ServerId::new(2), ServerId::new(3)]).unwrap();

    assert_eq!(config.size(), 3);
}

#[test]
fn majority_of_three_is_two() {
    let config =
        ClusterConfig::new(vec![ServerId::new(1), ServerId::new(2), ServerId::new(3)]).unwrap();

    assert_eq!(config.majority(), 2);
}

#[test]
fn majority_of_five_is_three() {
    let config = ClusterConfig::new(vec![
        ServerId::new(1),
        ServerId::new(2),
        ServerId::new(3),
        ServerId::new(4),
        ServerId::new(5),
    ])
    .unwrap();

    assert_eq!(config.majority(), 3);
}

#[test]
fn contains_checks_membership() {
    let config =
        ClusterConfig::new(vec![ServerId::new(1), ServerId::new(2), ServerId::new(3)]).unwrap();

    assert!(config.contains(ServerId::new(2)));
    assert!(!config.contains(ServerId::new(10)));
}

#[test]
fn has_majority_checks_quorum() {
    let config = ClusterConfig::new(vec![
        ServerId::new(1),
        ServerId::new(2),
        ServerId::new(3),
        ServerId::new(4),
        ServerId::new(5),
    ])
    .unwrap();

    assert!(!config.has_majority(2));
    assert!(config.has_majority(3));
    assert!(config.has_majority(4));
    assert!(config.has_majority(5));
}

#[test]
fn servers_returns_members() {
    let servers = vec![ServerId::new(1), ServerId::new(2), ServerId::new(3)];

    let config = ClusterConfig::new(servers.clone()).unwrap();

    assert_eq!(config.servers(), servers.as_slice());
}
