
use std::collections::HashMap;

use rustyraft::raft::{
    RaftNode,
    ServerId,
};

use rustyraft::raft::transport::{
    InMemoryTransport, RaftMessage, RaftMessagePayload,
};


use rustyraft::raft::rpc::{
    RequestVoteRequest, RequestVoteResponse,
};


pub struct TestCluster<C> {
    nodes: HashMap<ServerId, RaftNode<C>>,
    transport: InMemoryTransport<C>,
}

pub struct RequestVoteDelivery {
    pub from: ServerId,
    pub to: ServerId,
}

impl<C> TestCluster<C> {
    pub fn new(
        server_ids: &[ServerId],
    ) -> Self {
        let nodes = server_ids
            .iter()
            .copied()
            .map(|server_id| {
                (
                    server_id,
                    RaftNode::new(server_id),
                )
            })
            .collect();

        Self {
            nodes,
            transport: InMemoryTransport::new(),
        }
    }
}


impl<C> TestCluster<C> {
    pub fn node(
        &self,
        server_id: ServerId,
    ) -> Option<&RaftNode<C>> {
        self.nodes.get(&server_id)
    }

    pub fn node_mut(
        &mut self,
        server_id: ServerId,
    ) -> Option<&mut RaftNode<C>> {
        self.nodes.get_mut(&server_id)
    }

    pub fn transport(
        &mut self,
    ) -> &mut InMemoryTransport<C> {
        &mut self.transport
    }
}

impl <C: Clone> TestCluster<C> {

    pub fn start_election(
        &mut self,
        candidate_id: ServerId,
    ) {
        // get all the server id from the clusters
        let server_ids: Vec<ServerId> =
            self.nodes.keys().copied().collect();

        // get the candidate with the provided id
        let candidate =
            self.nodes.get_mut(&candidate_id)
                .expect("candidate should exists");
    
        // candidate is starting the election
        candidate.start_election();

        let term = candidate.current_term();
        let candidate_id = candidate.id();
        let last_log_index = candidate.last_log_index();
        let last_log_term = candidate.last_log_term();

        for server_id in server_ids {
            if server_id == candidate_id {
                continue;
            }

            let request = RequestVoteRequest::new(
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            );

            let message = RaftMessage::new(
                candidate_id,
                server_id,
                RaftMessagePayload::<C>::RequestVote(
                    request,
                ),
            );

            self.transport.send(message);
        }
    }

    pub fn deliver_to(
        &mut self,
        server_id: ServerId,
    ) ->  Option<RequestVoteDelivery> {

        let message = self
            .transport
            .deliver_to(server_id)?;
        let from = message.from;
        let to = message.to;
        
        let destination = message.to;

        let node = 
                self.nodes.get_mut(&destination)
                .expect("destination node should exists");
        
        match message.payload {
            RaftMessagePayload::RequestVote(
                request
            ) => {
                let response = 
                    node.handle_request_vote(
                        request,
                    );
                
                // response ownership is moved here
                self.transport.send(
                    RaftMessage::new(
                        to,
                        from,
                        RaftMessagePayload::<C>::RequestVoteResponse(
                            response,
                        ),
                    ),
                );

                Some(RequestVoteDelivery {
                    from,
                    to,
                })
            } 

            RaftMessagePayload::AppendEntries(
                request,
            ) => {
                let response = 
                    node.handle_append_entries(request);

                // we will add later
                tracing::debug!(
                    from = message.from.value(),
                    to = destination.value(),
                    success = response.success,
                    "Delivered AppendEntries"
                );
                
                // response ownership is moved here.
                self.transport.send(
                    RaftMessage::new(
                        to,
                        from,
                        RaftMessagePayload::<C>::AppendEntriesResponse(
                            response,
                        ),
                    ),
                );

                None
            }
            _ => None
        }
    }

    pub fn deliver_request_vote_response(
        &mut self, 
        candidate_id: ServerId,
    ) -> bool {
        let message =
            match self
                .transport
                .deliver_to(candidate_id) 
        {
            Some(message) => message, 
            None => return false,
        };

        let from  = message.from;

        let response =
            match message.payload {
                RaftMessagePayload::RequestVoteResponse(
                    response,
                ) => response,

                _ => {
                    return false;
                }
            };

        let cluster_ids: Vec<ServerId> =
            self.nodes.keys().copied().collect();

        let candidate =
            self.nodes
                .get_mut(&candidate_id)
                .expect(
                    "candidate should exists"
                );
        
        candidate.handle_request_vote_response(
            from, 
            response, 
            &cluster_ids
        );
        
        true
    }

    pub fn deliver_append_entries_response(
        &mut self, 
        leader_id: ServerId, 
    ) -> bool {

        let message = match self.transport.deliver_to(leader_id) {
            Some(message) => message,
            None => return false,
        };

        let from = message.from; 

        let response = match message.payload {
            RaftMessagePayload::AppendEntriesResponse(
                response,
            ) => response, 

            _ => {
                return false;
            }
        };

        let leader = 
            self.nodes.get_mut(&leader_id).expect("leader should exists");

        leader.handle_append_entries_response(
            from, 
            response, 
            leader.last_log_index()
        );

        true
    }

}

