//! Actions produced by a RaftNode
//! 
//! A raft node owns protocol decisions
//! it doesn't performs any IO. When a state transition requries an 
//! external side effect, the node produces the RaftAction and runtime execute
//! it

use crate::raft::rpc::{
    AppendEntriesRequest, 
    AppendEntriesResponse, 
    RequestVoteRequest, 
    RequestVoteResponse,
};

use crate::raft::state::ServerId;
use crate::raft::transport::{RaftMessage, RaftMessagePayload};

pub enum RaftAction<C> {
    SendRequestVote {
        target: ServerId,
        request: RequestVoteRequest,
    },

    SendRequestVoteResponse {
        target: ServerId,
        response: RequestVoteResponse,
    },

    SendAppendEntries {
        target: ServerId,
        request: AppendEntriesRequest<C>,
    },

    SendAppendEntriesResponse {
        target: ServerId,
        response: AppendEntriesResponse,
    },
}

impl<C> RaftAction<C> {
    pub fn into_message(
        self,
        from: ServerId,
    ) -> RaftMessage<C> {
        match self {
            RaftAction::SendRequestVote {
                target,
                request,
            } => RaftMessage::new(
                from,
                target,
                RaftMessagePayload::RequestVote(
                    request,
                ),
            ),

            RaftAction::SendRequestVoteResponse {
                target,
                response,
            } => RaftMessage::new(
                from,
                target,
                RaftMessagePayload::RequestVoteResponse(
                    response,
                ),
            ),

            RaftAction::SendAppendEntries {
                target,
                request,
            } => RaftMessage::new(
                from,
                target,
                RaftMessagePayload::AppendEntries(
                    request,
                ),
            ),

            RaftAction::SendAppendEntriesResponse {
                target,
                response,
            } => RaftMessage::new(
                from,
                target,
                RaftMessagePayload::AppendEntriesResponse(
                    response,
                ),
            ),
        }
    }
}