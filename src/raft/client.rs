use tokio::sync::{
    mpsc,
    oneshot,
};

use serde::Serialize;

use crate::raft::state::ServerId;

/// Current status of a client command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandStatus {
    Pending,
    Committed,
    Aborted,
}

/// Snapshot of a command's current status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CommandStatusSnapshot {
    pub request_id: String,
    pub log_index: u64,
    pub status: CommandStatus,
}

/// Errors that can be returned to a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientCommandError {
    NotLeader {
        known_leader: Option<ServerId>,
    },
    RequestIdConflict,
    RequestNotFound,
    StorageFailure,
    Unavailable,
}

/// Requests sent from the client-facing API to the Raft runtime.
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
    },
}

/// Handle used by external components to submit commands to the runtime.
///
/// The handle does not own or mutate the RaftNode. It communicates with
/// the runtime through a bounded channel.
pub struct ClientHandle<C> {
    sender: mpsc::Sender<ClientRequest<C>>,
}

impl<C> Clone for ClientHandle<C> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<C> ClientHandle<C>
where
    C: Send + 'static,
{
    /// Creates a client handle and the receiver owned by the runtime.
    pub fn channel(
        capacity: usize,
    ) -> (
        Self,
        mpsc::Receiver<ClientRequest<C>>,
    ) {
        let (sender, receiver) =
            mpsc::channel(capacity);

        (
            Self {
                sender,
            },
            receiver,
        )
    }

    /// Submits a command and waits for the runtime's response.
    pub async fn submit(
        &self,
        request_id: String,
        command: C,
    ) -> Result<
        CommandStatusSnapshot,
        ClientCommandError,
    > {
        // one shot channel - the recieve will remain here and
        // the sender is passed down to the raft runtime as part of request
        let (reply_to, reply_rx) =
            oneshot::channel();

        let request = ClientRequest::Submit {
            request_id,
            command,
            reply_to,
        };

        // Runtime has the reciver side of this outer sender 
        self.sender
            .send(request)
            .await
            .map_err(|_| ClientCommandError::Unavailable)?;

        reply_rx
            .await
            .map_err(|_| ClientCommandError::Unavailable)?
    }

    /// Queries the status of a previously submitted command.
    pub async fn get_status(
        &self,
        request_id: String,
    ) -> Result<
        CommandStatusSnapshot,
        ClientCommandError,
    > {
        let (reply_to, reply_rx) =
            oneshot::channel();

        let request = ClientRequest::GetStatus {
            request_id,
            reply_to,
        };

        self.sender
            .send(request)
            .await
            .map_err(|_| ClientCommandError::Unavailable)?;

        reply_rx
            .await
            .map_err(|_| ClientCommandError::Unavailable)?
    }
}


// --- unit test case ----

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn submit_sends_request_and_receives_reply() {
        let (client, mut receiver) =
            ClientHandle::<String>::channel(1);

        let task = tokio::spawn(async move {
            client
                .submit(
                    "request-1".to_string(),
                    "SET x=1".to_string(),
                )
                .await
        });

        match receiver.recv().await.expect("request should arrive") {
            ClientRequest::Submit {
                request_id,
                command,
                reply_to,
            } => {
                assert_eq!(request_id, "request-1");
                assert_eq!(command, "SET x=1");

                let expected = CommandStatusSnapshot {
                    request_id: "request-1".to_string(),
                    log_index: 1,
                    status: CommandStatus::Pending,
                };

                reply_to
                    .send(Ok(expected.clone()))
                    .expect("reply receiver should exist");

                assert_eq!(
                    task.await.expect("submit task should finish"),
                    Ok(expected),
                );
            }

            ClientRequest::GetStatus { .. } => {
                panic!("expected Submit request");
            }
        }
    }

    #[tokio::test]
    async fn get_status_sends_request_and_receives_reply() {
        let (client, mut receiver) =
            ClientHandle::<String>::channel(1);

        let task = tokio::spawn(async move {
            client.get_status("request-1".to_string()).await
        });

        match receiver.recv().await.expect("request should arrive") {
            ClientRequest::GetStatus {
                request_id,
                reply_to,
            } => {
                assert_eq!(request_id, "request-1");

                let expected = CommandStatusSnapshot {
                    request_id: "request-1".to_string(),
                    log_index: 1,
                    status: CommandStatus::Committed,
                };

                reply_to
                    .send(Ok(expected.clone()))
                    .expect("reply receiver should exist");

                assert_eq!(
                    task.await.expect("status task should finish"),
                    Ok(expected),
                );
            }

            ClientRequest::Submit { .. } => {
                panic!("expected GetStatus request");
            }
        }
    }

    #[tokio::test]
    async fn submit_returns_unavailable_when_receiver_is_dropped() {
        let (client, receiver) =
            ClientHandle::<String>::channel(1);

        drop(receiver);

        assert_eq!(
            client
                .submit(
                    "request-1".to_string(),
                    "SET x=1".to_string(),
                )
                .await,
            Err(ClientCommandError::Unavailable),
        );
    }

    #[tokio::test]
    async fn get_status_returns_unavailable_when_receiver_is_dropped() {
        let (client, receiver) =
            ClientHandle::<String>::channel(1);

        drop(receiver);

        assert_eq!(
            client.get_status("request-1".to_string()).await,
            Err(ClientCommandError::Unavailable),
        );
    }

    #[tokio::test]
    async fn submit_returns_unavailable_when_reply_sender_is_dropped() {
        let (client, mut receiver) =
            ClientHandle::<String>::channel(1);

        let task = tokio::spawn(async move {
            client
                .submit(
                    "request-1".to_string(),
                    "SET x=1".to_string(),
                )
                .await
        });

        match receiver.recv().await.expect("request should arrive") {
            ClientRequest::Submit { reply_to, .. } => {
                // Simulate the runtime disappearing before it replies.
                drop(reply_to);
            }

            ClientRequest::GetStatus { .. } => {
                panic!("expected Submit request");
            }
        }

        assert_eq!(
            task.await.expect("submit task should finish"),
            Err(ClientCommandError::Unavailable),
        );
    }

    #[tokio::test]
    async fn bounded_channel_does_not_enqueue_beyond_capacity() {
        use tokio::time::{sleep, Duration};

        let (client, mut receiver) =
            ClientHandle::<String>::channel(1);

        let first_client = client.clone();

        let first = tokio::spawn(async move {
            first_client
                .submit(
                    "request-1".to_string(),
                    "A".to_string(),
                )
                .await
        });

        // Allow the first request to occupy the only queue slot.
        sleep(Duration::from_millis(10)).await;
        assert_eq!(receiver.len(), 1);

        let second = tokio::spawn(async move {
            client
                .submit(
                    "request-2".to_string(),
                    "B".to_string(),
                )
                .await
        });

        sleep(Duration::from_millis(10)).await;

        // The second request cannot enter a full bounded queue.
        assert_eq!(receiver.len(), 1);

        drop(receiver);

        assert_eq!(
            first.await.expect("first task should finish"),
            Err(ClientCommandError::Unavailable),
        );

        assert_eq!(
            second.await.expect("second task should finish"),
            Err(ClientCommandError::Unavailable),
        );
    }
}