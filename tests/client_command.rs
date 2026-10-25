mod supports;

use std::convert::Infallible;
use std::time::Duration;

use rustyraft::raft::client::{
    ClientCommandError,
    ClientHandle,
    ClientRequest,
    CommandStatus,
};
use rustyraft::raft::node::{NodeError, RaftNode};
use rustyraft::raft::rpc::{
    AppendEntriesResponse,
    RequestVoteResponse,
};
use rustyraft::raft::runtime::RaftRuntime;
use rustyraft::raft::state::{
    LogIndex,
    Role,
    ServerId,
};
use rustyraft::raft::state_machine::NoopStateMachine;
use rustyraft::raft::transport::{
    RaftMessage,
    RaftMessagePayload,
    TcpTransportError,
    Transport,
};

use supports::node::{new_node, TestStorage};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;

struct TestTransport<C> {
    inbound: mpsc::Receiver<RaftMessage<C>>,
    outbound: mpsc::UnboundedSender<RaftMessage<C>>,
}

fn test_transport<C>() -> (
    TestTransport<C>,
    mpsc::Sender<RaftMessage<C>>,
    mpsc::UnboundedReceiver<RaftMessage<C>>,
) {
    let (inbound_tx, inbound_rx) = mpsc::channel(32);
    let (outbound_tx, outbound_rx) = mpsc::unbounded_channel();

    (
        TestTransport {
            inbound: inbound_rx,
            outbound: outbound_tx,
        },
        inbound_tx,
        outbound_rx,
    )
}

impl<C> Transport<C> for TestTransport<C>
where
    C: Send + 'static,
{
    type Error = Infallible;

    async fn send(
        &mut self,
        message: RaftMessage<C>,
    ) -> Result<(), TcpTransportError> {
        let _ = self.outbound.send(message);
        Ok(())
    }

    async fn receive(&mut self) -> Option<RaftMessage<C>> {
        self.inbound.recv().await
    }
}

type Runtime = RaftRuntime<
    String,
    TestStorage,
    NoopStateMachine,
    TestTransport<String>,
>;

type RuntimeTask = JoinHandle<(
    Result<(), NodeError<Infallible>>,
    Runtime,
)>;

fn make_leader(
    server_id: ServerId,
    cluster_servers: &[ServerId],
) -> RaftNode<String, TestStorage> {
    let mut node = new_node(server_id);

    node.start_election().expect("election should start");
    let term = node.current_term();

    // Simulate a peer vote for a multi-node test cluster.
    // For a single-node test, the candidate's own vote is sufficient.
    let voter_id = cluster_servers
        .iter()
        .copied()
        .find(|id| *id != server_id)
        .unwrap_or(server_id);

    node.handle_request_vote_response(
        voter_id,
        RequestVoteResponse::granted(term),
        cluster_servers,
    )
    .expect("vote response should be processed");

    assert_eq!(node.role(), Role::Leader);
    node
}

fn start_runtime(
    node: RaftNode<String, TestStorage>,
    cluster_servers: Vec<ServerId>,
    transport: TestTransport<String>,
    requests: mpsc::Receiver<ClientRequest<String>>,
) -> RuntimeTask {
    tokio::spawn(async move {
        let mut runtime = RaftRuntime::new(
            node,
            transport,
            cluster_servers,
        )
        .with_client_requests(requests);

        let result = runtime.run().await;
        (result, runtime)
    })
}

async fn stop_runtime(
    inbound_tx: mpsc::Sender<RaftMessage<String>>,
    task: RuntimeTask,
) -> Runtime {
    drop(inbound_tx);

    let (result, runtime) = timeout(Duration::from_secs(2), task)
        .await
        .expect("runtime should stop")
        .expect("runtime task should not panic");

    result.expect("runtime should exit cleanly");
    runtime
}

#[test]
fn client_entry_retains_request_id_and_index() {
    let id = ServerId::new(1);
    let cluster = [id];
    let mut node = make_leader(id, &cluster);

    let index = node
        .append_client_entry(
            "request-1".to_string(),
            "SET x=1".to_string(),
        )
        .expect("storage append should succeed")
        .expect("leader should accept command");

    assert_eq!(node.client_entry_index("request-1"), Some(index));

    let entry = node.log_at(index).expect("entry should exist");
    assert_eq!(entry.request_id(), Some("request-1"));
    assert_eq!(entry.command().expect("entry has command"), "SET x=1");
    assert_eq!(node.client_entry_index("missing"), None);
}

#[test]
fn follower_cannot_append_client_entry() {
    let mut follower = new_node(ServerId::new(1));

    assert_eq!(
        follower
            .append_client_entry(
                "request-1".to_string(),
                "SET x=1".to_string(),
            )
            .expect("storage should not fail"),
        None,
    );

    assert_eq!(follower.last_log_index(), LogIndex::ZERO);
}

#[tokio::test]
async fn single_node_leader_commits_and_queries_command() {
    let id = ServerId::new(1);
    let cluster = vec![id];
    let node = make_leader(id, &cluster);

    let (transport, inbound_tx, _outbound_rx) =
        test_transport::<String>();
    let (client, requests) = ClientHandle::<String>::channel(16);

    let task = start_runtime(node, cluster, transport, requests);

    let submitted = timeout(
        Duration::from_secs(2),
        client.submit(
            "request-1".to_string(),
            "SET x=1".to_string(),
        ),
    )
    .await
    .expect("submission should not time out")
    .expect("submission should succeed");

    assert_eq!(submitted.status, CommandStatus::Committed);
    assert_eq!(submitted.log_index, 1);

    let duplicate = client
        .submit(
            "request-1".to_string(),
            "SET x=1".to_string(),
        )
        .await
        .expect("duplicate request should return existing status");

    assert_eq!(duplicate, submitted);

    let queried = client
        .get_status("request-1".to_string())
        .await
        .expect("status should be available");

    assert_eq!(queried, submitted);

    let runtime = stop_runtime(inbound_tx, task).await;
    assert_eq!(runtime.node().last_log_index(), LogIndex::new(1));
}

#[tokio::test]
async fn follower_rejects_command_without_mutating_its_log() {
    let id = ServerId::new(1);
    let cluster = vec![id, ServerId::new(2), ServerId::new(3)];
    let node = new_node(id);

    let (transport, inbound_tx, _outbound_rx) =
        test_transport::<String>();
    let (client, requests) = ClientHandle::<String>::channel(16);

    let task = start_runtime(node, cluster, transport, requests);

    assert_eq!(
        client
            .submit(
                "request-1".to_string(),
                "SET x=1".to_string(),
            )
            .await,
        Err(ClientCommandError::NotLeader {
            known_leader: None,
        }),
    );

    let runtime = stop_runtime(inbound_tx, task).await;
    assert_eq!(runtime.node().last_log_index(), LogIndex::ZERO);
}

#[tokio::test]
async fn command_stays_pending_without_majority_acknowledgement() {
    let id = ServerId::new(1);
    let cluster = vec![id, ServerId::new(2), ServerId::new(3)];
    let node = make_leader(id, &cluster);

    let (transport, inbound_tx, _outbound_rx) =
        test_transport::<String>();
    let (client, requests) = ClientHandle::<String>::channel(16);

    let task = start_runtime(node, cluster, transport, requests);

    let submitted = client
        .submit(
            "request-1".to_string(),
            "SET x=1".to_string(),
        )
        .await
        .expect("leader should append command");

    assert_eq!(submitted.status, CommandStatus::Pending);

    let queried = client
        .get_status("request-1".to_string())
        .await
        .expect("pending entry should be discoverable");

    assert_eq!(queried.status, CommandStatus::Pending);

    let runtime = stop_runtime(inbound_tx, task).await;
    assert_eq!(runtime.node().commit_index(), LogIndex::ZERO);
}

#[tokio::test]
async fn follower_acknowledgement_allows_majority_commit() {
    let leader_id = ServerId::new(1);
    let cluster = vec![
        leader_id,
        ServerId::new(2),
        ServerId::new(3),
    ];

    let node = make_leader(leader_id, &cluster);
    let term = node.current_term();

    let (transport, inbound_tx, mut outbound_rx) =
        test_transport::<String>();
    let (client, requests) = ClientHandle::<String>::channel(16);

    let task = start_runtime(node, cluster, transport, requests);

    let submitted = client
        .submit(
            "request-1".to_string(),
            "SET x=1".to_string(),
        )
        .await
        .expect("leader should append command");

    assert_eq!(submitted.status, CommandStatus::Pending);

    // Ignore initial heartbeats; wait for AppendEntries with the command.
    let follower_id = timeout(
        Duration::from_secs(2),
        async {
            loop {
                let message = outbound_rx
                    .recv()
                    .await
                    .expect("outbound channel should remain open");

                if matches!(
                    &message.payload,
                    RaftMessagePayload::AppendEntries(request)
                        if !request.entries.is_empty()
                ) {
                    break message.to;
                }
            }
        },
    )
    .await
    .expect("command should be replicated");

    inbound_tx
        .send(RaftMessage::new(
            follower_id,
            leader_id,
            RaftMessagePayload::AppendEntriesResponse(
                AppendEntriesResponse::success(
                    term,
                    LogIndex::new(1),
                ),
            ),
        ))
        .await
        .expect("follower response should reach runtime");

    let committed = timeout(
        Duration::from_secs(2),
        async {
            loop {
                let snapshot = client
                    .get_status("request-1".to_string())
                    .await
                    .expect("status query should succeed");

                if snapshot.status == CommandStatus::Committed {
                    break snapshot;
                }

                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
    )
    .await
    .expect("quorum acknowledgement should commit command");

    assert_eq!(committed.log_index, 1);

    let runtime = stop_runtime(inbound_tx, task).await;
    assert_eq!(runtime.node().commit_index(), LogIndex::new(1));
}