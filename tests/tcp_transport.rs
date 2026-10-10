//! Integration tests for the real TCP transport.
//!
//! These tests exercise the production TCP transport using real
//! loopback TCP connections.
//!
//! The test deliberately does not involve RaftNode or RaftRuntime.
//! Its purpose is to verify:
//! - TCP listener binding
//! - peer connection establishment
//! - handshake
//! - connection ownership
//! - frame encoding/decoding
//! - RaftMessage serialization
//! - asynchronous message delivery

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
use tokio::time::{
    sleep,
    timeout,
    Duration,
};

/// Reserves an available localhost port.
///
/// The listener is immediately dropped so that the actual transport
/// can bind to the same address.
///
/// This is sufficient for an integration test, although there is
/// technically a small race between releasing the port and binding
/// the transport.
async fn reserve_address() -> SocketAddr {
    let listener =
        TcpListener::bind(
            "127.0.0.1:0",
        )
        .await
        .expect("failed to reserve localhost port");

    let address =
        listener
            .local_addr()
            .expect("failed to get reserved address");

    drop(listener);

    address
}

#[tokio::test(
    flavor = "multi_thread",
    worker_threads = 4
)]
async fn three_node_tcp_transport_delivers_messages()
{
    let node1 = ServerId::new(1);
    let node2 = ServerId::new(2);
    let node3 = ServerId::new(3);

    // ------------------------------------------------------------
    // Reserve addresses for all three nodes.
    // ------------------------------------------------------------

    let address1 =
        reserve_address().await;

    let address2 =
        reserve_address().await;

    let address3 =
        reserve_address().await;

    // ------------------------------------------------------------
    // Create all three TCP transports.
    //
    // Connection ownership is deterministic:
    //
    //     lower ServerId -> higher ServerId
    //
    // Therefore:
    //
    //     node1 -> node2
    //     node1 -> node3
    //     node2 -> node3
    //
    // ------------------------------------------------------------

    let mut transport1 =
        TcpTransport::<String>::bind(
            node1,
            address1,
            vec![
                PeerAddress {
                    server_id: node2,
                    address: address2,
                },
                PeerAddress {
                    server_id: node3,
                    address: address3,
                },
            ],
        )
        .await
        .expect("failed to bind node1 transport");

    let mut transport2 =
        TcpTransport::<String>::bind(
            node2,
            address2,
            vec![
                PeerAddress {
                    server_id: node1,
                    address: address1,
                },
                PeerAddress {
                    server_id: node3,
                    address: address3,
                },
            ],
        )
        .await
        .expect("failed to bind node2 transport");

    let mut transport3 =
        TcpTransport::<String>::bind(
            node3,
            address3,
            vec![
                PeerAddress {
                    server_id: node1,
                    address: address1,
                },
                PeerAddress {
                    server_id: node2,
                    address: address2,
                },
            ],
        )
        .await
        .expect("failed to bind node3 transport");

    // ------------------------------------------------------------
    // Start receive loops for node2 and node3.
    //
    // receive_message() also accepts incoming connections, so these
    // tasks must be running before node1 attempts to connect.
    // ------------------------------------------------------------

    let node2_task =
        tokio::spawn(
            async move {
                transport2
                    .receive_message()
                    .await
            },
        );

    let node3_task =
        tokio::spawn(
            async move {
                transport3
                    .receive_message()
                    .await
            },
        );

    // Give the receiver tasks an opportunity to start waiting on
    // their TCP listeners.
    sleep(
        Duration::from_millis(20),
    )
    .await;

    // ------------------------------------------------------------
    // Establish node1 -> node2 and node1 -> node3 connections.
    //
    // node1 owns the outbound connections because its ServerId is
    // lower than both node2 and node3.
    // ------------------------------------------------------------

    timeout(
        Duration::from_secs(2),
        async {
            while transport1
                .connection_manager()
                .len()
                < 2
            {
                transport1
                    .maintain_connections()
                    .await;

                sleep(
                    Duration::from_millis(10),
                )
                .await;
            }
        },
    )
    .await
    .expect(
        "timed out waiting for node1 TCP connections",
    );

    assert_eq!(
        transport1
            .connection_manager()
            .len(),
        2,
        "node1 should have connections to node2 and node3",
    );

    // ------------------------------------------------------------
    // Build a real Raft message.
    // ------------------------------------------------------------

    let message_to_node2 =
        RaftMessage::new(
            node1,
            node2,
            RaftMessagePayload::AppendEntries(
                AppendEntriesRequest {
                    term: Term::new(1),
                    leader_id: node1,
                    prev_log_index: LogIndex::ZERO,
                    prev_log_term: Term::ZERO,
                    entries: vec![],
                    leader_commit: LogIndex::ZERO,
                },
            ),
        );

    let message_to_node3 =
        RaftMessage::new(
            node1,
            node3,
            RaftMessagePayload::AppendEntries(
                AppendEntriesRequest {
                    term: Term::new(1),
                    leader_id: node1,
                    prev_log_index: LogIndex::ZERO,
                    prev_log_term: Term::ZERO,
                    entries: vec![],
                    leader_commit: LogIndex::ZERO,
                },
            ),
        );

    // ------------------------------------------------------------
    // Send both messages through the real TCP transport.
    // ------------------------------------------------------------

    transport1
        .send_message(
            message_to_node2,
        )
        .await
        .expect(
            "failed to send message to node2",
        );

    transport1
        .send_message(
            message_to_node3,
        )
        .await
        .expect(
            "failed to send message to node3",
        );

    // ------------------------------------------------------------
    // Receive the messages on node2 and node3.
    // ------------------------------------------------------------

    let received_by_node2 =
        timeout(
            Duration::from_secs(2),
            node2_task,
        )
        .await
        .expect(
            "timed out waiting for node2 message",
        )
        .expect(
            "node2 receive task panicked",
        )
        .expect(
            "node2 transport closed before receiving message",
        );

    let received_by_node3 =
        timeout(
            Duration::from_secs(2),
            node3_task,
        )
        .await
        .expect(
            "timed out waiting for node3 message",
        )
        .expect(
            "node3 receive task panicked",
        )
        .expect(
            "node3 transport closed before receiving message",
        );

    // ------------------------------------------------------------
    // Verify node2 received the correct message.
    // ------------------------------------------------------------

    assert_eq!(
        received_by_node2.from,
        node1,
    );

    assert_eq!(
        received_by_node2.to,
        node2,
    );

    match received_by_node2.payload {
        RaftMessagePayload::AppendEntries(
            request,
        ) => {
            assert_eq!(
                request.term,
                Term::new(1),
            );

            assert_eq!(
                request.leader_id,
                node1,
            );

            assert!(
                request.entries.is_empty(),
            );

            assert_eq!(
                request.leader_commit,
                LogIndex::ZERO,
            );
        }

        payload => {
            panic!(
                "node2 received unexpected payload: {:?}",
                payload,
            );
        }
    }

    // ------------------------------------------------------------
    // Verify node3 received the correct message.
    // ------------------------------------------------------------

    assert_eq!(
        received_by_node3.from,
        node1,
    );

    assert_eq!(
        received_by_node3.to,
        node3,
    );

    match received_by_node3.payload {
        RaftMessagePayload::AppendEntries(
            request,
        ) => {
            assert_eq!(
                request.term,
                Term::new(1),
            );

            assert_eq!(
                request.leader_id,
                node1,
            );

            assert!(
                request.entries.is_empty(),
            );

            assert_eq!(
                request.leader_commit,
                LogIndex::ZERO,
            );
        }

        payload => {
            panic!(
                "node3 received unexpected payload: {:?}",
                payload,
            );
        }
    }
}

#[tokio::test(
    flavor = "multi_thread",
    worker_threads = 4
)]
async fn tcp_transport_delivers_multiple_messages_over_one_connection()
{
    let node1 = ServerId::new(1);
    let node2 = ServerId::new(2);

    let address1 = reserve_address().await;
    let address2 = reserve_address().await;

    let mut transport1 =
        TcpTransport::<String>::bind(
            node1,
            address1,
            vec![
                PeerAddress {
                    server_id: node2,
                    address: address2,
                },
            ],
        )
        .await
        .expect("failed to bind node1 transport");

    let transport2 =
        TcpTransport::<String>::bind(
            node2,
            address2,
            vec![
                PeerAddress {
                    server_id: node1,
                    address: address1,
                },
            ],
        )
        .await
        .expect("failed to bind node2 transport");

    // Node 2 continuously receives messages using its transport.
    let (received_tx, mut received_rx) =
        tokio::sync::mpsc::channel(3);

    let node2_task = tokio::spawn(async move {
        let mut transport = transport2;

        while let Some(message) =
            transport.receive_message().await
        {
            if received_tx.send(message).await.is_err() {
                break;
            }
        }
    });

    // Establish the connection owned by node 1.
    timeout(
        Duration::from_secs(3),
        async {
            loop {
                transport1.maintain_connections().await;

                if transport1.connection_manager().len() == 1 {
                    break;
                }

                sleep(Duration::from_millis(10)).await;
            }
        },
    )
    .await
    .expect("timed out waiting for TCP connection");

    // Send three messages with distinct terms so that their
    // order and contents can be verified.
    for term_value in 1..=3 {
        let term = Term::new(term_value);

        let message =
            RaftMessage::new(
                node1,
                node2,
                RaftMessagePayload::AppendEntries(
                    AppendEntriesRequest {
                        term,
                        leader_id: node1,
                        prev_log_index: LogIndex::ZERO,
                        prev_log_term: Term::ZERO,
                        entries: vec![],
                        leader_commit: LogIndex::ZERO,
                    },
                ),
            );

        transport1
            .send_message(message)
            .await
            .expect("failed to send Raft message");
    }

    // Receive and verify all three messages in order.
    for expected_term in 1..=3 {
        let message =
            timeout(
                Duration::from_secs(3),
                received_rx.recv(),
            )
            .await
            .expect("timed out waiting for message")
            .expect("node2 receive channel closed");

        assert_eq!(message.from, node1);
        assert_eq!(message.to, node2);

        match message.payload {
            RaftMessagePayload::AppendEntries(request) => {
                assert_eq!(
                    request.term,
                    Term::new(expected_term),
                    "messages must arrive in send order",
                );

                assert_eq!(request.leader_id, node1);
                assert!(request.entries.is_empty());
            }

            payload => {
                panic!(
                    "unexpected payload received: {:?}",
                    payload,
                );
            }
        }
    }

    // The connection should remain registered after all messages.
    assert_eq!(
        transport1.connection_manager().len(),
        1,
        "expected the persistent connection to remain active",
    );

    node2_task.abort();
}