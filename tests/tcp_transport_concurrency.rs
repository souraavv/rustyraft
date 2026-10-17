
use std::net::SocketAddr;

use rustyraft::raft::{
    LogIndex,
    ServerId,
    Term,
};
use rustyraft::raft::rpc::AppendEntriesRequest;
use rustyraft::raft::transport::{
    PeerAddress,
    RaftMessage,
    RaftMessagePayload,
    TcpTransport,
};
use tokio::net::TcpListener;
use tokio::time::{sleep, timeout, Duration};

async fn reserve_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to reserve localhost port");

    let address = listener
        .local_addr()
        .expect("failed to read local address");

    drop(listener);
    address
}

fn append_entries(
    from: ServerId,
    to: ServerId,
    term: u64,
) -> RaftMessage<String> {
    RaftMessage::new(
        from,
        to,
        RaftMessagePayload::AppendEntries(
            AppendEntriesRequest {
                term: Term::new(term),
                leader_id: from,
                prev_log_index: LogIndex::ZERO,
                prev_log_term: Term::ZERO,
                entries: vec![],
                leader_commit: LogIndex::ZERO,
            },
        ),
    )
}

async fn wait_for_connections<C>(
    transport: &mut TcpTransport<C>,
    expected: usize,
) where
    C: serde::Serialize
        + serde::de::DeserializeOwned
        + Send
        + 'static,
{
    timeout(Duration::from_secs(3), async {
        loop {
            transport.maintain_connections().await;

            if transport.connection_manager().len() >= expected {
                return;
            }

            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("timed out waiting for TCP connections");
}

#[tokio::test(
    flavor = "multi_thread",
    worker_threads = 4
)]
async fn unreachable_peer_does_not_block_healthy_peer() {
    let node1 = ServerId::new(1);
    let node2 = ServerId::new(2);
    let node3 = ServerId::new(3);

    let address1 = reserve_address().await;
    let unreachable_address = reserve_address().await;
    let address3 = reserve_address().await;

    // No transport is bound to node2's address.
    let mut transport1 = TcpTransport::<String>::bind(
        node1,
        address1,
        vec![
            PeerAddress {
                server_id: node2,
                address: unreachable_address,
            },
            PeerAddress {
                server_id: node3,
                address: address3,
            },
        ],
    )
    .await
    .expect("failed to bind node1");

    let transport3 = TcpTransport::<String>::bind(
        node3,
        address3,
        vec![
            PeerAddress {
                server_id: node1,
                address: address1,
            },
            PeerAddress {
                server_id: node2,
                address: unreachable_address,
            },
        ],
    )
    .await
    .expect("failed to bind node3");

    // Node3 must receive inbound connections.
    let node3_task = tokio::spawn(async move {
        let mut transport = transport3;
        transport.receive_message().await
    });

    // Node1 attempts both peers. The unavailable node2 must not
    // prevent its connection to healthy node3 from being established.
    wait_for_connections(&mut transport1, 1).await;

    assert!(
        transport1.connection_manager().contains(node3),
        "node1 should connect to healthy node3",
    );

    assert!(
        !transport1.connection_manager().contains(node2),
        "node1 should not report an established node2 connection",
    );

    node3_task.abort();
}

#[tokio::test(
    flavor = "multi_thread",
    worker_threads = 4
)]
async fn messages_work_in_both_directions_over_one_connection() {
    let node1 = ServerId::new(1);
    let node2 = ServerId::new(2);

    let address1 = reserve_address().await;
    let address2 = reserve_address().await;

    let mut transport1 = TcpTransport::<String>::bind(
        node1,
        address1,
        vec![PeerAddress {
            server_id: node2,
            address: address2,
        }],
    )
    .await
    .expect("failed to bind node1");

    let transport2 = TcpTransport::<String>::bind(
        node2,
        address2,
        vec![PeerAddress {
            server_id: node1,
            address: address1,
        }],
    )
    .await
    .expect("failed to bind node2");

    // Node2 receives the first message, then returns its transport
    // so the test can send a response over the same connection.
    let node2_task = tokio::spawn(async move {
        let mut transport = transport2;
        let message = transport.receive_message().await;
        (transport, message)
    });

    wait_for_connections(&mut transport1, 1).await;

    transport1
        .send_message(append_entries(node1, node2, 1))
        .await
        .expect("node1 failed to send request");

    let (mut transport2, received) = timeout(
        Duration::from_secs(3),
        node2_task,
    )
    .await
    .expect("node2 timed out")
    .expect("node2 task panicked");

    let received = received.expect("node2 received no message");
    assert_eq!(received.from, node1);
    assert_eq!(received.to, node2);

    // The established TCP connection is bidirectional. Node2 can
    // send on its registered inbound connection without connecting
    // outbound to node1.
    transport2
        .send_message(append_entries(node2, node1, 2))
        .await
        .expect("node2 failed to send response");

    let response = timeout(
        Duration::from_secs(3),
        transport1.receive_message(),
    )
    .await
    .expect("node1 timed out waiting for response")
    .expect("node1 transport closed");

    assert_eq!(response.from, node2);
    assert_eq!(response.to, node1);

    match response.payload {
        RaftMessagePayload::AppendEntries(request) => {
            assert_eq!(request.term, Term::new(2));
            assert_eq!(request.leader_id, node2);
        }
        payload => {
            panic!("unexpected response payload: {:?}", payload);
        }
    }

    assert_eq!(transport1.connection_manager().len(), 1);
    assert_eq!(transport2.connection_manager().len(), 1);
}

#[tokio::test]
async fn lower_id_owns_outbound_connection() {
    let node1 = ServerId::new(1);
    let node2 = ServerId::new(2);

    let address1 = reserve_address().await;
    let address2 = reserve_address().await;

    let transport1 = TcpTransport::<String>::bind(
        node1,
        address1,
        vec![PeerAddress {
            server_id: node2,
            address: address2,
        }],
    )
    .await
    .expect("failed to bind node1");

    let transport2 = TcpTransport::<String>::bind(
        node2,
        address2,
        vec![PeerAddress {
            server_id: node1,
            address: address1,
        }],
    )
    .await
    .expect("failed to bind node2");

    assert!(
        transport1
            .connection_manager()
            .owns_outbound_connection(node2),
    );

    assert!(
        !transport2
            .connection_manager()
            .owns_outbound_connection(node1),
    );
}
