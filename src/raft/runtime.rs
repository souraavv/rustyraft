//! Runtime for driving a single Raft node
//! 
//! The runtime does own the execution of the Raft node while the transport
//! reamins reponsible for moving messages
//! 
//! The runtime does not implement Raft protocol decision 
//! It only drives the node, create out going messages, receives incoming 
//! messages, and dispatches them to the node.

use crate::raft::node::RaftNode;
use crate::raft::rpc::RequestVoteRequest;
use crate::raft::state::{
    Role,
    ServerId,
};
use crate::raft::transport::{
    RaftMessage,
    RaftMessagePayload,
    Transport,
};
use crate::raft::storage::RaftStorage;
use crate::raft::state_machine::StateMachine;

use std::fmt::Debug;

pub struct RaftRuntime<C, St, S, T> 
where
    St: RaftStorage<C>,
    S: StateMachine<C>,
    St::Error: Debug,
    T: Transport<C>,
{
    node: RaftNode<C, St, S>,
    transport: T, 
    cluster_servers: Vec<ServerId>,
}

impl <C, St, S, T> RaftRuntime<C, St, S, T>
where 
    St: RaftStorage<C>,
    S: StateMachine<C>,
    St: RaftStorage<C>,
    St::Error: Debug, // associated error type must implement Debug
    T: Transport<C>,
{
    pub fn new(
        node: RaftNode<C, St, S>,
        transport: T,
        cluster_servers: Vec<ServerId>,
    ) -> Self {
        Self {
            node,
            transport,
            cluster_servers,
        }
    }

    pub fn tick(
        &mut self,
    ) 
    where 
        C: Clone,
    {
        let previous_term = 
            self.node.current_term();

        self.node.tick();

        if self.node.role() == Role::Candidate
            && self.node.current_term() > previous_term
        {
            self.send_request_votes();
        }

        if self.node.role() == Role::Leader {
            self.send_heartbeats();
        }
    }

    fn send_heartbeats(
        &mut self,
    )
    where 
        C: Clone,
    {
        let requests =
            self.node.heartbeat_requests();

        for (follower_id, request) in requests {
            self.transport.send(
                RaftMessage::new(
                    self.node.id(),
                    follower_id,
                    RaftMessagePayload::AppendEntries(
                        request,
                    ),
                ),
            );
        }
    }

    fn send_request_votes(
        &mut self,
    ) {
        let term =
            self.node.current_term();

        let candidate_id =
            self.node.id();

        let last_log_index =
            self.node.last_log_index();

        let last_log_term =
            self.node.last_log_term();

        for server_id in
            self.cluster_servers.iter().copied()
        {
            if server_id == candidate_id {
                continue;
            }

            let request =
                RequestVoteRequest::new(
                    term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                );

            self.transport.send(
                RaftMessage::new(
                    candidate_id,
                    server_id,
                    RaftMessagePayload::RequestVote(
                        request,
                    ),
                ),
            );
        }
    }

    pub fn receive(
        &mut self,
    ) -> Option<RaftMessage<C>> {
        self.transport.receive()
    }


    pub fn handle_message(
        &mut self, 
        message: RaftMessage<C>,
    ) {
        match message.payload {
            RaftMessagePayload::RequestVote(
                request,
            ) => {
                let response = 
                    self.node.handle_request_vote(request);
            
                self.transport.send(
                    RaftMessage::new(
                        self.node.id(),
                        message.from,
                        RaftMessagePayload::RequestVoteResponse(
                            response,
                        ),
                    ),
                );
            }

            RaftMessagePayload::RequestVoteResponse(
                response,
            ) => {
                self.node
                    .handle_request_vote_response(
                        message.from, 
                        response,
                        &self.cluster_servers,
                    );
            }
            
            RaftMessagePayload::AppendEntries(
                request,
            ) => {
                let response = 
                    self.node
                        .handle_append_entries(
                            request,
                        );
                
                self.transport.send(
                    RaftMessage::new(
                        self.node.id(),
                        message.from,
                    RaftMessagePayload::AppendEntriesResponse(
                        response,
                    )),
                );
            }

            RaftMessagePayload::AppendEntriesResponse(
                response,
            ) => {
                self.node
                    .handle_append_entries_response(
                        message.from, 
                        response,
                );
            }
        }
    }

    // --- helpers ----
    pub fn node(
        &self,
    ) -> &RaftNode<C, St, S> {
        &self.node
    }

    pub fn mut_node(
        &mut self,
    ) -> &mut RaftNode<C, St, S> {
        &mut self.node
    }

    pub fn transport(
        &mut self, 
    ) -> &mut T {
        &mut self.transport
    }

}
