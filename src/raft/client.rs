use serde::Serialize;
use tokio::sync::oneshot;

use crate::raft::state::ServerId;

// Current status of the client command
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandStatus {
    Pending, 
    Commited,
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CommandStatusSnapshot {
    pub request_id: String, 
    pub log_index: u64, 
    pub status: CommandStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientCommandError {
    NotLeader {
        known_leader: Option<ServerId>,
    },
    RequestIdConflict,
    StorageFalilure,
}

pub enum ClientRequest<C> {
    Submit {
        request_id: String, 
        command: C, 
        reply_to: oneshot::Sender<
            Result<CommandStatusSnapshot, ClientCommandError>,
        >,
    },
    GetStatus {
        request_id: String, 
        reply_to: oneshot::Sender<
            Result<CommandStatusSnapshot, ClientCommandError>,
        >,
    }
}
