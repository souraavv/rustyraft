use std::collections::HashMap;

use rustyraft::raft::{
    RaftNode,
    ServerId,
};

use rustyraft::raft::action::RaftAction;

use rustyraft::raft::state_machine::NoopStateMachine;

use rustyraft::raft::transport::{
    InMemoryTransport,
    RaftMessage,
    RaftMessagePayload,
    Transport,
};

use super::node::{
    new_test_storage,
    TestStorage,
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
        // Get all the server ids from the cluster.
        let server_ids: Vec<ServerId> =
            self.nodes
                .keys()
                .copied()
                .collect();

        // Get the candidate with the provided id.
        let candidate =
            self.nodes
                .get_mut(&candidate_id)
                .expect(
                    "candidate should exist",
                );

        // Candidate is starting the election.
        candidate.start_election();

        // Build the RequestVote actions from the candidate.
        let actions =
            candidate.request_vote_actions(
                &server_ids,
            );

        self.execute_actions(
            candidate_id,
            actions,
        );
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

    /// From a given leader_id to a given follower_id.
    pub fn send_append_entries(
        &mut self,
        leader_id: ServerId,
        follower_id: ServerId,
    ) -> bool {
        // Create the request which leader will provide you
        // by building an AppendEntries request for a given
        // follower.
        let request = {
            let leader =
                self.nodes
                    .get(&leader_id)
                    .expect(
                        "leader should exist",
                    );

            match leader.build_append_entries(
                follower_id,
            ) {
                Some(request) => request,
                None => return false,
            }
        };

        // Create the same RaftAction that the RaftNode
        // would produce for AppendEntries.
        let action =
            RaftAction::SendAppendEntries {
                target: follower_id,
                request,
            };

        // Execute the action through the test transport.
        self.execute_actions(
            leader_id,
            vec![action],
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

    /// Drops one pending message destined for the given server.
    ///
    /// This is intentionally exposed by the test cluster so tests
    /// can simulate message loss without knowing about the
    /// underlying transport implementation.
    pub fn drop_to(
        &mut self,
        server_id: ServerId,
    ) -> bool {
        self.transport.drop_to(
            server_id,
        )
    }

    /// Execute all protocol actions produced by a node.
    ///
    /// The Raft node only produces RaftAction values. The test
    /// cluster acts as the runtime and converts those actions into
    /// transport messages.
    fn execute_actions(
        &mut self,
        from: ServerId,
        actions: Vec<RaftAction<String>>,
    ) {
        for action in actions {
            let message =
                action.into_message(
                    from,
                );

            self.transport.send(
                message,
            );
        }
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

        let actions: Vec<RaftAction<String>> = {
            let node =
                self.nodes
                    .get_mut(&to)
                    .expect(
                        "message destination should exist",
                    );

            match message.payload {
                RaftMessagePayload::RequestVote(
                    request,
                ) => {
                    node.handle_request_vote(
                        from,
                        request,
                    )
                }

                RaftMessagePayload::RequestVoteResponse(
                    response,
                ) => {
                    node.handle_request_vote_response(
                        from,
                        response,
                        &cluster_servers,
                    )
                }

                RaftMessagePayload::AppendEntries(
                    request,
                ) => {
                    node.handle_append_entries(
                        from,
                        request,
                    )
                }

                RaftMessagePayload::AppendEntriesResponse(
                    response,
                ) => {
                    node.handle_append_entries_response(
                        from,
                        response,
                    )
                }
            }
        };

        self.execute_actions(
            to,
            actions,
        );

        MessageDelivery {
            from,
            to,
        }
    }

    /// Delivers exactly one pending transport message.
    pub fn deliver_next(
        &mut self,
    ) -> Option<MessageDelivery> {
        let message =
            self.transport
                .deliver_next()?;

        Some(
            self.deliver_message(
                message,
            )
        )
    }

    /// Deliver the first pending message that matches
    /// the supplied predicate.
    ///
    /// Unrelated messages remain in the transport queue.
    pub fn deliver_matching<F>(
        &mut self,
        predicate: F,
    ) -> Option<MessageDelivery>
    where
        F: Fn(&RaftMessage<String>) -> bool,
    {
        let message =
            self.transport.deliver_matching(predicate)?;

        let from = message.from;
        let to = message.to;

        self.deliver_message(message);

        Some(MessageDelivery {
            from,
            to,
        })
    }

    /// Advances logical time for one node.
    ///
    /// A follower or candidate may start an election when its
    /// election timer expires. A leader may generate heartbeat or
    /// replication requests when its heartbeat timer expires.
    ///
    /// The node produces protocol actions. The test cluster converts
    /// those actions into transport messages, just like the runtime.
    pub fn tick(
        &mut self,
        server_id: ServerId,
    ) {
        let server_ids: Vec<ServerId> =
            self.nodes
                .keys()
                .copied()
                .collect();

        let actions: Vec<RaftAction<String>> = {
            let node =
                self.nodes
                    .get_mut(&server_id)
                    .expect(
                        "node should exist",
                    );

            node.tick(
                &server_ids,
            )
        };

        self.execute_actions(
            server_id,
            actions,
        );
    }

    /// Advances logical time once and delivers one message.
    pub fn step(
        &mut self,
        server_id: ServerId,
    ) -> Option<MessageDelivery> {
        self.tick(
            server_id,
        );

        self.deliver_next()
    }

    /// Partitions communication from one server to another.
    ///
    /// This is directional. Messages from `first` to `second`
    /// are blocked, while messages from `second` to `first`
    /// remain unaffected.
    pub fn partition(
        &mut self,
        first: ServerId,
        second: ServerId,
    ) {
        self.transport.partition(
            first,
            second,
        );
    }

    /// Heals communication from one server to another.
    ///
    /// This is directional. Messages from `first` to `second`
    /// are allowed again.
    pub fn heal(
        &mut self,
        first: ServerId,
        second: ServerId,
    ) {
        self.transport.heal(
            first,
            second,
        );
    }

    /// Partitions communication in both directions between
    /// two servers.
    pub fn partition_bidirectional(
        &mut self,
        first: ServerId,
        second: ServerId,
    ) {
        self.transport.partition_bidirectional(
            first,
            second,
        );
    }

    /// Heals communication in both directions between
    /// two servers.
    pub fn heal_bidirectional(
        &mut self,
        first: ServerId,
        second: ServerId,
    ) {
        self.transport.heal_bidirectional(
            first,
            second,
        );
    }
}
