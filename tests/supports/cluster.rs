use std::collections::HashMap;

use rustyraft::raft::{
    RaftNode,
    Role,
    ServerId,
};

use rustyraft::raft::state_machine::{
    NoopStateMachine,
};

use rustyraft::raft::transport::{
    InMemoryTransport,
    RaftMessage,
    RaftMessagePayload,
};

use super::node::{
    new_test_storage,
    TestStorage,
};


use rustyraft::raft::rpc::{
    RequestVoteRequest,
    RequestVoteResponse,
};


pub struct TestCluster {
    nodes: HashMap<
        ServerId,
        RaftNode<String, TestStorage>,
    >,
    transport: InMemoryTransport<String>,
}

pub struct MessageDelivery {
    pub from: ServerId,
    pub to: ServerId,
}

impl TestCluster {
    pub fn new(
        server_ids: &[ServerId],
    ) -> Self {
        let nodes = server_ids
            .iter()
            .copied()
            .map(|server_id| {
                (
                    server_id,
                    RaftNode::new(
                        server_id,
                        new_test_storage(),
                    ),
                )
            })
            .collect();

        Self {
            nodes,
            transport: InMemoryTransport::new(),
        }
    }
}


impl TestCluster {
    pub fn node(
        &self,
        server_id: ServerId,
    ) -> Option<&RaftNode<String, TestStorage>> {
        self.nodes.get(&server_id)
    }

    pub fn node_mut(
        &mut self,
        server_id: ServerId,
    ) -> Option<&mut RaftNode<String, TestStorage>> {
        self.nodes.get_mut(&server_id)
    }

    pub fn transport(
        &mut self,
    ) -> &mut InMemoryTransport<String> {
        &mut self.transport
    }
}

impl TestCluster {

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
                RaftMessagePayload::<String>::RequestVote(
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

        Some(
            self.deliver_message(
                message,
            )
        )
    }

    pub fn deliver_at(
        &mut self,
        position: usize,
    ) -> Option<MessageDelivery> {
        let message =
            self.transport
                .deliver_at(position)?;

        Some(
            self.deliver_message(
                message,
            )
        )
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
                RaftMessagePayload::<String>::AppendEntries(
                    request,
                ),
            ),
        );

        true
    }

    /// Crashes a node and returns its persistent storage.
    pub fn crash_node(
        &mut self,
        server_id: ServerId,
    ) -> Option<TestStorage> {
        let node =
            self.nodes.remove(
                &server_id,
            )?;

        // Remove messages that are waiting to be
        // delivered to the crashed node.
        self.transport.drop_to(
            server_id,
        );

        Some(
            node.into_storage()
        )
    }

    /// Restarts a node from its recovered persistent storage.
    pub fn restart_node(
        &mut self,
        server_id: ServerId,
        storage: TestStorage,
    ) {
        let node =
            RaftNode::from_storage(
                server_id,
                storage,
                NoopStateMachine,
            );

        self.nodes.insert(
            server_id,
            node,
        );
    }

    fn deliver_message(
        &mut self,
        message: RaftMessage<String>,
    ) -> MessageDelivery {
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
                            RaftMessagePayload::<String>::RequestVoteResponse(
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
                            RaftMessagePayload::<String>::AppendEntriesResponse(
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
            self.transport.send(
                response,
            );
        }

        MessageDelivery {
            from,
            to,
        }
    }

    pub fn drop_to(
        &mut self,
        server_id: ServerId,
    ) -> bool {
        self.transport.drop_to(
            server_id,
        )
    }

    /// Advances logical time for one node.
    ///
    /// A follower or candidate may start an election when its
    /// election timer expires. A leader may generate heartbeat or
    /// replication requests when its heartbeat timer expires.
    pub fn tick(&mut self, server_id: ServerId) {
        let server_ids: Vec<ServerId> =
            self.nodes.keys().copied().collect();

        let election_request = {
            let node = self.nodes
                .get_mut(&server_id)
                .expect("node should exist");

            let previous_term = node.current_term();

            node.tick();

            let started_election =
                node.role() == Role::Candidate
                && node.current_term() > previous_term;

            if started_election {
                Some(node.build_request_vote())
            } else {
                None
            }
        };

        if let Some(request) = election_request {
            for peer_id in server_ids {
                if peer_id == server_id {
                    continue;
                }

                let request = RequestVoteRequest::new(
                    request.term,
                    request.candidate_id,
                    request.last_log_index,
                    request.last_log_term,
                );

                self.transport.send(
                    RaftMessage::new(
                        server_id,
                        peer_id,
                        RaftMessagePayload::<String>::RequestVote(
                            request,
                        ),
                    ),
                );
            }

            return;
        }

        let requests = {
            let node = self.nodes
                .get_mut(&server_id)
                .expect("node should exist");

            if node.role() == Role::Leader {
                node.heartbeat_requests()
            } else {
                Vec::new()
            }
        };

        for (follower_id, request) in requests {
            self.transport.send(
                RaftMessage::new(
                    server_id,
                    follower_id,
                    RaftMessagePayload::<String>::AppendEntries(
                        request,
                    ),
                ),
            );
        }
    }

    /// Delivers exactly one pending transport message.
    pub fn deliver_next(&mut self) -> Option<MessageDelivery> {
        let message = self.transport.deliver_next()?;
        Some(self.deliver_message(message))
    }

    /// Advances logical time once and delivers one message.
    pub fn step(
        &mut self,
        server_id: ServerId,
    ) -> Option<MessageDelivery> {
        self.tick(server_id);
        self.deliver_next()
    }

}