use crate::raft::{Role, ServerId};

/// A read only snapshot of the Raft node's externally visible state
/// 
/// The runtime can publish this snapshot to consumers such as the
/// REST API wihtout exposing the live RaftNode
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeStatus {
    pub node_id: ServerId, 
    pub role: Role,
    pub leader_id: Option<ServerId>,
}

impl NodeStatus {
    pub fn new(
        node_id: ServerId,
        role: Role,
        leader_id: Option<ServerId>,
    ) -> Self {
        Self {
            node_id,
            role, 
            leader_id,
        }
    }
}