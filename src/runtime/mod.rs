//! Runtime for driving a single Raft node
//!
//! The runtime owns the execution of the Raft node while the transport
//! remains responsible for moving messages.
//!
//! The runtime does not implement Raft protocol decisions.
//! It only drives the node, executes outgoing actions, receives incoming
//! messages, and dispatches them to the node.
//!
//! Ticks are internally driven.
//!
//! handle_message is externally driven.

use crate::raft::action::RaftAction;
use crate::raft::node::RaftNode;
use crate::raft::state::ServerId;
use crate::raft::state_machine::StateMachine;
use crate::raft::storage::RaftStorage;
use crate::raft::transport::{
    RaftMessage,
    RaftMessagePayload,
    Transport,
};

use std::fmt::Debug;

pub struct RaftRuntime<C, St, S, T>
where
    St: RaftStorage<C>,
    S: StateMachine<C>,
    St::Error: Debug,
    T: Transport<C>,
    T::Error: Debug,
{
    node: RaftNode<C, St, S>,
    transport: T,
    cluster_servers: Vec<ServerId>,
}

impl<C, St, S, T> RaftRuntime<C, St, S, T>
where
    St: RaftStorage<C>,
    S: StateMachine<C>,
    St::Error: Debug,
    T: Transport<C>,
    T::Error: Debug,
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

    /// Advances the node's timers and executes any protocol actions
    /// produced by the resulting state transition.
    ///
    /// The node owns the Raft protocol decision.
    /// The runtime is responsible only for executing the actions.
    pub async fn tick(
        &mut self,
    )
    where
        C: Clone,
    {
        let actions =
            self.node.tick(
                &self.cluster_servers,
            );

        self.execute_actions(actions)
            .await;
    }

    /// Executes actions produced by the Raft node.
    ///
    /// The runtime deliberately does not decide why an action is required.
    /// It only translates the action into a transport message and sends it.
    async fn execute_actions(
        &mut self,
        actions: Vec<RaftAction<C>>,
    ) {
        let from =
            self.node.id();

        for action in actions {
            let message =
                action.into_message(from);

            tracing::debug!(
                from = message.from.value(),
                to = message.to.value(),
                "Executing Raft action"
            );

            if let Err(error) =
                self.transport
                    .send(message)
                    .await
            {
                tracing::warn!(
                    ?error,
                    "Failed to send Raft message"
                );
            }
        }
    }

    pub async fn receive(
        &mut self,
    ) -> Option<RaftMessage<C>> {
        self.transport
            .receive()
            .await
    }

    /// Handles an incoming Raft message.
    ///
    /// The runtime is responsible for dispatching the message to the
    /// appropriate RaftNode handler. Any actions produced by the node
    /// are executed through the transport.
    pub async fn handle_message(
        &mut self,
        message: RaftMessage<C>,
    )
    where
        C: Clone,
    {
        let from =
            message.from;

        match message.payload {
            RaftMessagePayload::RequestVote(
                request,
            ) => {
                let actions =
                    self.node
                        .handle_request_vote(
                            from,
                            request,
                        );

                self.execute_actions(
                    actions,
                )
                .await;
            }

            RaftMessagePayload::RequestVoteResponse(
                response,
            ) => {
                let actions =
                    self.node
                        .handle_request_vote_response(
                            from,
                            response,
                            &self.cluster_servers,
                        );

                self.execute_actions(
                    actions,
                )
                .await;
            }

            RaftMessagePayload::AppendEntries(
                request,
            ) => {
                let actions =
                    self.node
                        .handle_append_entries(
                            from,
                            request,
                        );

                self.execute_actions(
                    actions,
                )
                .await;
            }

            RaftMessagePayload::AppendEntriesResponse(
                response,
            ) => {
                let actions =
                    self.node
                        .handle_append_entries_response(
                            from,
                            response,
                        );

                self.execute_actions(
                    actions,
                )
                .await;
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