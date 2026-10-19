use std::env;
use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;

use rustyraft::raft::{state::ServerId, transport::PeerAddress};

pub struct NodeConfig {
    pub node_id: ServerId,
    pub listen_addr: SocketAddr,
    pub storage_dir: PathBuf,
    pub peers: Vec<PeerAddress>,
    pub cluster_servers: Vec<ServerId>,
}

impl NodeConfig {
    pub fn from_args() -> Result<Self, Box<dyn Error>> {
        let args: Vec<String> = env::args().collect();

        if args.len() < 4 {
            return Err(format!(
                "Usage: {} <node-id> <listen-addr> <storage-dir> \
                 [peer-id@peer-addr ...]",
                args.first().map(String::as_str).unwrap_or("rustyraft")
            )
            .into());
        }

        let node_id = ServerId::new(args[1].parse()?);
        let listen_addr: SocketAddr = args[2].parse()?;
        let storage_dir = PathBuf::from(&args[3]);

        let mut peers = Vec::new();
        let mut cluster_servers = vec![node_id];

        for peer_arg in &args[4..] {
            let (id, address) = peer_arg
                .split_once('@')
                .ok_or("Peer must use the format <peer-id>@<address>")?;

            let peer_id = ServerId::new(id.parse()?);
            let peer_addr: SocketAddr = address.parse()?;

            if cluster_servers.contains(&peer_id) {
                return Err(format!(
                    "Duplicate node ID: {}",
                    peer_id.value()
                )
                .into());
            }

            cluster_servers.push(peer_id);

            peers.push(PeerAddress {
                server_id: peer_id,
                address: peer_addr,
            });
        }

        cluster_servers.sort();

        Ok(Self {
            node_id,
            listen_addr,
            storage_dir,
            peers,
            cluster_servers,
        })
    }
}