
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

pub struct MessageDelivery {
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
    ) -> Option<MessageDelivery> {
        let message =
            self.transport
                .deliver_to(server_id)?;

        let from = message.from;
        let to = message.to;

        let cluster_servers: Vec<ServerId> =
            self.nodes
                .keys()
                .copied()
                .collect();

        let mut response_message = None;

        {
            let node =
                self.nodes
                    .get_mut(&to)
                    .expect(
                        "message destination should exist"
                    );

            match message.payload {
                RaftMessagePayload::RequestVote(
                    request,
                ) => {
                    let response =
                        node.handle_request_vote(
                            request,
                        );

                    response_message = Some(
                        RaftMessage::new(
                            to,
                            from,
                            RaftMessagePayload::<C>::RequestVoteResponse(
                                response,
                            ),
                        ),
                    );
                }

                RaftMessagePayload::RequestVoteResponse(
                    response,
                ) => {
                    node.handle_request_vote_response(
                        from,
                        response,
                        &cluster_servers,
                    );
                }

                RaftMessagePayload::AppendEntries(
                    request,
                ) => {
                    let response =
                        node.handle_append_entries(
                            request,
                        );

                    tracing::debug!(
                        from = from.value(),
                        to = to.value(),
                        success = response.success,
                        "Delivered AppendEntries"
                    );

                    response_message = Some(
                        RaftMessage::new(
                            to,
                            from,
                            RaftMessagePayload::<C>::AppendEntriesResponse(
                                response,
                            ),
                        ),
                    );
                }

                RaftMessagePayload::AppendEntriesResponse(
                    response,
                ) => {
                    node.handle_append_entries_response(
                        from,
                        response,
                    );
                }
            }
        }

        if let Some(response) =
            response_message
        {
            self.transport.send(response);
        }

        Some(MessageDelivery {
            from,
            to,
        })
    }

    /// From a given leader_id to a given follower_id
    pub fn send_append_entries(
        &mut self,
        leader_id: ServerId,
        follower_id: ServerId,
    ) -> bool {

        // create the request which leader will provide you by building
        // append entry requst for a given follower
        let request = {
            let leader =
                self.nodes
                    .get(&leader_id)
                    .expect(
                        "leader should exist"
                    );

            match leader.build_append_entries(
                follower_id,
            ) {
                Some(request) => request,
                None => return false,
            }
        };

        // Sending the RaftMessage on the transport layer
        self.transport.send(
            RaftMessage::new(
                leader_id,
                follower_id,
                RaftMessagePayload::<C>::AppendEntries(
                    request,
                ),
            ),
        );

        true
    }

    pub fn drop_to(
        &mut self,
        server_id: ServerId,
    ) -> bool {
        self.transport.drop_to(
            server_id,
        )
    }

}

