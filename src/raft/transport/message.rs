//! Messages exchanged between Raft servers.
//!
//! The transport owns the sender and receiver information.
//! The payload contains the Raft RPC itself.
//! 

use std::marker::PhantomData;

use crate::raft::rpc::{
    AppendEntriesRequest,
    RequestVoteRequest,
};

use crate::raft::state::ServerId;

#[derive(Debug)]
pub struct RaftMessage<C> {
    pub from: ServerId,
    pub to: ServerId, 
    pub payload: RaftMessagePayload<C>,
}

#[derive(Debug)]
pub enum RaftMessagePayload<C> {
    RequestVote(RequestVoteRequest),
    AppendEntries(AppendEntriesRequest<C>),
}

impl<C> RaftMessage<C> {
    pub fn new(
        from: ServerId,
        to: ServerId,
        payload: RaftMessagePayload<C>,
    ) -> Self {
        Self {
            from, 
            to,
            payload,
        }
    }

}