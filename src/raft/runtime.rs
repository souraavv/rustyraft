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
use crate::raft::client::{
    ClientCommandError,
    ClientRequest,
    CommandStatus,
    CommandStatusSnapshot,
};
use crate::raft::node::{NodeError, RaftNode};
use crate::raft::state::{LogIndex, Role, ServerId};
use crate::raft::state_machine::StateMachine;
use crate::raft::storage::RaftStorage;
use crate::raft::transport::{
    RaftMessage,
    RaftMessagePayload,
    Transport,
};

use std::fmt::Debug;
use tokio::sync::{mpsc, watch};

enum RuntimeEvent<C> {
    Tick,
    Message(Option<RaftMessage<C>>),
    Client(Option<ClientRequest<C>>),
}

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
    leader_status_tx: Option<watch::Sender<Option<ServerId>>>,
    client_requests: Option<mpsc::Receiver<ClientRequest<C>>>,
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
            leader_status_tx: None,
            client_requests: None,
        }
    }

    /// Connects the client request channel to the runtime.
    ///
    /// The runtime becomes the receiver and sole processor of client
    /// requests. HTTP handlers retain only the corresponding sender.
    pub fn with_client_requests(
        mut self,
        receiver: mpsc::Receiver<ClientRequest<C>>,
    ) -> Self {
        self.client_requests = Some(receiver);
        self
    }

    pub fn with_leader_status_publisher(
        mut self,
        sender: watch::Sender<Option<ServerId>>,
    ) -> Self {
        self.leader_status_tx = Some(sender);
        self.publish_leader_status();
        self
    }

    fn publish_leader_status(&self) {
        if let Some(sender) = &self.leader_status_tx {
            sender.send_replace(self.node.known_leader());
        }
    }

    /// Advances the node's timers and executes any protocol actions
    /// produced by the resulting state transition.
    ///
    /// The node owns the Raft protocol decision.
    /// The runtime is responsible only for executing the actions.
    pub async fn tick(
        &mut self,
    ) -> Result<(), NodeError<St::Error>>
    where
        C: Clone,
    {
        let actions = self.node.tick(&self.cluster_servers)?;

        self.execute_actions(actions).await;
        self.publish_leader_status();

        Ok(())
    }

    /// Executes actions produced by the Raft node.
    ///
    /// The runtime deliberately does not decide why an action is required.
    /// It only translates the action into a transport message and sends it.
    async fn execute_actions(
        &mut self,
        actions: Vec<RaftAction<C>>,
    ) {
        let from = self.node.id();

        for action in actions {
            let message = action.into_message(from);

            tracing::debug!(
                from = message.from.value(),
                to = message.to.value(),
                "Executing Raft action"
            );

            if let Err(error) = self.transport.send(message).await {
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
        self.transport.receive().await
    }

    /// Handles an incoming Raft message.
    ///
    /// The runtime is responsible for dispatching the message to the
    /// appropriate RaftNode handler. Any actions produced by the node
    /// are executed through the transport.
    pub async fn handle_message(
        &mut self,
        message: RaftMessage<C>,
    ) -> Result<(), NodeError<St::Error>>
    where
        C: Clone,
    {
        let from = message.from;

        match message.payload {
            RaftMessagePayload::RequestVote(request) => {
                let actions = self.node.handle_request_vote(
                    from,
                    request,
                )?;

                self.execute_actions(actions).await;
            }

            RaftMessagePayload::RequestVoteResponse(response) => {
                let actions = self.node.handle_request_vote_response(
                    from,
                    response,
                    &self.cluster_servers,
                )?;

                self.execute_actions(actions).await;
            }

            RaftMessagePayload::AppendEntries(request) => {
                let actions = self.node.handle_append_entries(
                    from,
                    request,
                )?;

                self.execute_actions(actions).await;
            }

            RaftMessagePayload::AppendEntriesResponse(response) => {
                let actions = self.node.handle_append_entries_response(
                    from,
                    response,
                )?;

                self.execute_actions(actions).await;
            }
        }

        self.publish_leader_status();

        Ok(())
    }

    /// Builds a snapshot of a command's status from the local log and
    /// commit index.
    fn command_status_snapshot(
        &self,
        request_id: String,
        index: LogIndex,
    ) -> CommandStatusSnapshot {
        let status = if index <= self.node.commit_index() {
            CommandStatus::Committed
        } else {
            CommandStatus::Pending
        };

        CommandStatusSnapshot {
            request_id,
            log_index: index.value(),
            status,
        }
    }

    /// Processes a client request.
    ///
    /// Client requests are handled by the same runtime that processes
    /// Raft messages and timer ticks. The HTTP layer never accesses the
    /// RaftNode directly.
    async fn handle_client_request(
        &mut self,
        request: ClientRequest<C>,
    ) -> Result<(), NodeError<St::Error>>
    where
        C: Clone,
    {
        match request {
            ClientRequest::Submit {
                request_id,
                command,
                reply_to,
            } => {
                // Only the leader may accept a new command.
                if self.node.role() != Role::Leader {
                    let result = Err(
                        ClientCommandError::NotLeader {
                            known_leader: self.node.known_leader(),
                        },
                    );

                    let _ = reply_to.send(result);
                    return Ok(());
                }

                // If this request ID already exists locally, return its
                // existing status instead of appending another entry.
                if let Some(index) =
                    self.node.client_entry_index(&request_id)
                {
                    let snapshot = self.command_status_snapshot(
                        request_id,
                        index,
                    );

                    let _ = reply_to.send(Ok(snapshot));
                    return Ok(());
                }

                // Append the command and request ID to the leader's log.
                let index = match self.node.append_client_entry(
                    request_id.clone(),
                    command,
                ) {
                    Ok(Some(index)) => index,

                    Ok(None) => {
                        let result = Err(
                            ClientCommandError::NotLeader {
                                known_leader:
                                    self.node.known_leader(),
                            },
                        );

                        let _ = reply_to.send(result);
                        return Ok(());
                    }

                    Err(error) => {
                        let _ = reply_to.send(Err(
                            ClientCommandError::StorageFailure,
                        ));

                        return Err(error);
                    }
                };

                // Recalculate commitment. This also allows a single-node
                // cluster to commit its own current-term entry.
                self.node.update_commit_index();

                if self.node.apply_committed_entries().is_err() {
                    tracing::error!(
                        server_id = self.node.id().value(),
                        log_index = index.value(),
                        "Failed to apply committed client command"
                    );
                }

                let snapshot = self.command_status_snapshot(
                    request_id,
                    index,
                );

                // A Pending response means the entry is appended but
                // has not yet reached the local commit index.
                let _ = reply_to.send(Ok(snapshot));

                // Generate replication actions when the node's
                // heartbeat policy says they are due. If no actions
                // are due yet, a subsequent timer tick will retry.
                let actions = self.node.append_entries_actions();

                self.execute_actions(actions).await;
            }

            ClientRequest::GetStatus {
                request_id,
                reply_to,
            } => {
                let result = match self.node.client_entry_index(
                    &request_id,
                ) {
                    Some(index) => Ok(
                        self.command_status_snapshot(
                            request_id,
                            index,
                        ),
                    ),

                    None => Err(
                        ClientCommandError::RequestNotFound,
                    ),
                };

                let _ = reply_to.send(result);
            }
        }

        self.publish_leader_status();

        Ok(())
    }

    /// Runs the node until the transport's receive channel closes.
    pub async fn run(
        &mut self,
    ) -> Result<(), NodeError<St::Error>>
    where
        C: Clone,
    {
        use std::time::Duration;
        use tokio::time::{interval, MissedTickBehavior};

        let mut ticker = interval(Duration::from_millis(100));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            // Select only the next event here. Process it afterwards so
            // the channel borrows end before handler methods borrow self.
            let event = {
                let transport = &mut self.transport;
                let client_requests = &mut self.client_requests;

                tokio::select! {
                    _ = ticker.tick() => {
                        RuntimeEvent::Tick
                    }

                    message = transport.receive() => {
                        RuntimeEvent::Message(message)
                    }

                    request = async {
                        match client_requests.as_mut() {
                            Some(receiver) => receiver.recv().await,
                            None => {
                                std::future::pending::<
                                    Option<ClientRequest<C>>
                                >().await
                            }
                        }
                    } => {
                        RuntimeEvent::Client(request)
                    }
                }
            };

            match event {
                RuntimeEvent::Tick => {
                    self.tick().await?;
                }

                RuntimeEvent::Message(Some(message)) => {
                    self.handle_message(message).await?;
                }

                RuntimeEvent::Message(None) => {
                    tracing::error!(
                        "Transport receive channel closed"
                    );

                    return Ok(());
                }

                RuntimeEvent::Client(Some(request)) => {
                    self.handle_client_request(request).await?;
                }

                RuntimeEvent::Client(None) => {
                    // All client senders have been dropped. Disable this
                    // branch so it cannot repeatedly wake the event loop.
                    self.client_requests = None;

                    tracing::debug!(
                        "Client request channel closed"
                    );
                }
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