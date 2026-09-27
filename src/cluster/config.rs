use std::collections::HashSet;

use crate::raft::state::ServerId;

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    servers: Vec<ServerId>,
}

impl ClusterConfig {
    /// Create a cluster configuration
    ///
    /// Every server ID must be unique and cluster must contains at least
    /// one server
    pub fn new(servers: Vec<ServerId>) -> Result<Self, ClusterConfigError> {
        if servers.is_empty() {
            return Err(ClusterConfigError::EmptyCluster);
        }

        let unique_servers: HashSet<ServerId> = servers.iter().copied().collect();

        if unique_servers.len() != servers.len() {
            return Err(ClusterConfigError::DuplicateServer);
        }

        Ok(Self { servers })
    }

    /// Returns the number of servers in the cluster.
    pub fn size(&self) -> usize {
        self.servers.len()
    }

    /// Returns the number of servers required for a majority.
    pub fn majority(&self) -> usize {
        self.size() / 2 + 1
    }

    /// Returns true if the cluster contains the given server.
    pub fn contains(&self, server_id: ServerId) -> bool {
        self.servers.contains(&server_id)
    }

    /// Returns all server IDs (borrowed view of a serversId)
    /// reference to the slice &[T]
    pub fn servers(&self) -> &[ServerId] {
        &self.servers
    }

    /// Returns true when the supplied vote count forms majority
    pub fn has_majority(&self, vote_count: usize) -> bool {
        vote_count >= self.majority()
    }
}

/// Errors that can occur while creating a cluster configuration.
#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum ClusterConfigError {
    EmptyCluster,
    DuplicateServer,
}
