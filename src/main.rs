
mod api;
mod config;
mod http_server;

use std::error::Error;
use std::io;

use rustyraft::raft::ServerId;
use rustyraft::raft::node::RaftNode;
use rustyraft::raft::runtime::RaftRuntime;
use rustyraft::raft::state_machine::NoopStateMachine;
use rustyraft::raft::storage::{
    CachedStorage, FileStorage, InMemoryCache,
};
use rustyraft::raft::transport::TcpTransport;
use tokio::net::TcpListener;
use tokio::sync::watch;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // Read the config provided in the CLI
    let config = config::NodeConfig::from_args()?;

    // Fail startup if the HTTP port cannot be bound initially.
    let listener =
        TcpListener::bind(config.http_listen_addr).await?;

    tracing::info!(
        node_id = config.node_id.value(),
        raft_address = %config.listen_addr,
        http_address = %config.http_listen_addr,
        "Starting RustyRaft node"
    );

    // Create the storage for the RAFT Node
    let storage = CachedStorage::new(
        FileStorage::<String>::open(&config.storage_dir)?,
        InMemoryCache::<String>::new(),
    )?;

    // Transport protocol for the Raft Messages
    let transport = TcpTransport::<String>::bind(
        config.node_id,
        config.listen_addr,
        config.peers,
    )
    .await?;

    // Get the raft node handle - consumes storage and state machine
    let mut node = RaftNode::with_storage(
        config.node_id,
        storage,
        NoopStateMachine,
    )?;

    // Randomize the election timeout to reduce election collisions.
    node.randomize_election_timeout(10, 20);

    let (leader_status_tx, leader_status_rx) = 
        watch::channel::<Option<ServerId>>(None);
    

    // Get Raft Runtime
    // Runtime consitute of Node, Transport and participating clusters
    let mut runtime = RaftRuntime::new(
        node,
        transport,
        config.cluster_servers,
    ).with_leader_status_publisher(leader_status_tx);

    // Start HTTP server independently from the Raft runtime.
    let mut http_server = http_server::HttpServer::start(
        config.http_listen_addr,
        listener,
        leader_status_rx.clone(),
    );

    // Make Raft runtime as a tokio task and spawn it.
    let mut runtime_handle = tokio::spawn(async move {
        runtime.run().await
    });

    // Now we have indepedent branches, we know each is infinite loop
    // server runs indefinitely and same as the Raft runtime
    loop {
        tokio::select! {
            // HTTP Server - Handle the API
            result = http_server.task() => {
                match result {
                    Ok(()) => {
                        // Expecatation - Server will never return, if it
                        // return for any reason Ok() or Err(error) we will
                        // attempt to restart the HTTP server
                        tracing::warn!(
                            "HTTP supervisor exited unexpectedly"
                        );
                    }
                    Err(error) => {
                        tracing::error!(
                            %error,
                            "HTTP supervisor task failed"
                        );
                    }
                }

                // Restart HTTP supervision without restarting Raft.
                http_server.restart();
            }

            // ---- Raft Runtime ----

            result = &mut runtime_handle => {
                let runtime_error: Box<dyn Error> = match result {
                    Ok(Ok(())) => io::Error::new(
                        io::ErrorKind::Other,
                        "Raft runtime exited unexpectedly",
                    ).into(),

                    Ok(Err(error)) => error.into(),

                    Err(error) => error.into(),
                };

                // Gracefully stop the HTTP server before exiting.
                http_server.shutdown().await;

                return Err(runtime_error);
            }
        }
    }
}
