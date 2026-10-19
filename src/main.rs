mod config;

use std::error::Error;

use rustyraft::raft::node::RaftNode;
use rustyraft::raft::runtime::RaftRuntime;
use rustyraft::raft::state_machine::NoopStateMachine;
use rustyraft::raft::storage::{
    CachedStorage, FileStorage, InMemoryCache,
};
use rustyraft::raft::transport::TcpTransport;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let config = config::NodeConfig::from_args()?;

    let storage = CachedStorage::new(
        FileStorage::<String>::open(&config.storage_dir)?,
        InMemoryCache::<String>::new(),
    )?;

    let transport = TcpTransport::<String>::bind(
        config.node_id,
        config.listen_addr,
        config.peers,
    )
    .await?;

    let mut node = RaftNode::with_storage(
        config.node_id,
        storage,
        NoopStateMachine,
    );

    node.randomize_election_timeout(10, 20);

    let mut runtime = RaftRuntime::new(
        node,
        transport,
        config.cluster_servers,
    );

    runtime.run().await;

    Ok(())
}