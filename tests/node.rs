use rustyraft::raft::{LogIndex, RaftNode, Role, ServerId, Term};

#[test]
fn new_node_is_follower() {
    let node = RaftNode::<String>::new(ServerId::new(1));

    assert_eq!(node.id(), ServerId::new(1));
    assert_eq!(node.role(), Role::Follower);
}

#[test]
fn new_node_starts_at_term_zero() {
    let node = RaftNode::<String>::new(ServerId::new(1));

    assert_eq!(node.current_term(), Term::ZERO);
}

#[test]
fn new_node_has_not_voted() {
    let node = RaftNode::<String>::new(ServerId::new(1));

    assert_eq!(node.voted_for(), None);
}

#[test]
fn new_node_has_empty_log() {
    let node = RaftNode::<String>::new(ServerId::new(1));

    assert!(node.log().is_empty());
    assert_eq!(node.log().last_index(), LogIndex::ZERO);
}

#[test]
fn new_node_has_zero_commit_index() {
    let node = RaftNode::<String>::new(ServerId::new(1));

    assert_eq!(node.commit_index(), LogIndex::ZERO);
}

#[test]
fn new_node_has_zero_last_applied() {
    let node = RaftNode::<String>::new(ServerId::new(1));

    assert_eq!(node.last_applied(), LogIndex::ZERO);
}
