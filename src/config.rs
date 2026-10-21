use std::env;
use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;

use rustyraft::raft::{
    state::ServerId,
    transport::PeerAddress,
};

const RAFT_PORT_BASE: u64 = 7000;
const HTTP_PORT_BASE: u64 = 3000;
const MAX_NODE_COUNT: u64 = 32;

pub struct NodeConfig {
    pub node_id: ServerId,
    pub listen_addr: SocketAddr,
    pub http_listen_addr: SocketAddr,
    pub storage_dir: PathBuf,
    pub peers: Vec<PeerAddress>,
    pub cluster_servers: Vec<ServerId>,
}

impl NodeConfig {
    pub fn from_args() -> Result<Self, Box<dyn Error>> {
        let args: Vec<String> = env::args().collect();

        if args.len() != 3 {
            return Err(format!(
                "Usage: {} <node-id> <node-count>",
                args.first()
                    .map(String::as_str)
                    .unwrap_or("rustyraft")
            )
            .into());
        }

        let node_id_value: u64 = args[1].parse()?;
        let node_count: u64 = args[2].parse()?;

        if node_count == 0 || node_count > MAX_NODE_COUNT {
            return Err(format!(
                "Node count must be between 1 and {}",
                MAX_NODE_COUNT
            )
            .into());
        }

        if node_id_value == 0 || node_id_value > node_count {
            return Err(format!(
                "Node ID must be between 1 and {}",
                node_count
            )
            .into());
        }

        let node_id = ServerId::new(node_id_value);

        let listen_addr = Self::raft_address(node_id_value);
        let http_listen_addr = Self::http_address(node_id_value);

        let storage_dir = PathBuf::from(format!(
            "./data/node{}",
            node_id_value
        ));

        let mut peers = Vec::new();
        let mut cluster_servers = Vec::new();

        for id in 1..=node_count {
            let server_id = ServerId::new(id);
            cluster_servers.push(server_id);

            if id != node_id_value {
                peers.push(PeerAddress {
                    server_id,
                    address: Self::raft_address(id),
                });
            }
        }

        Ok(Self {
            node_id,
            listen_addr,
            http_listen_addr,
            storage_dir,
            peers,
            cluster_servers,
        })
    }

    fn raft_address(node_id: u64) -> SocketAddr {
        SocketAddr::from((
            [127, 0, 0, 1],
            (RAFT_PORT_BASE + node_id) as u16,
        ))
    }

    fn http_address(node_id: u64) -> SocketAddr {
        SocketAddr::from((
            [127, 0, 0, 1],
            (HTTP_PORT_BASE + node_id) as u16,
        ))
    }
}