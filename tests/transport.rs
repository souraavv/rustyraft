use rustyraft::raft::{
    LogIndex,
    RaftNode,
    Role,
    ServerId,
    Term,
};

use rustyraft::raft::rpc::{
    AppendEntriesRequest,
    RequestVoteResponse,
    RequestVoteRequest,
};

use rustyraft::raft::transport::{
    InMemoryTransport,
    RaftMessage,
    RaftMessagePayload,
};

#[test]
fn message_preserves_sender_and_receiver() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let request =
        AppendEntriesRequest::<String>::heartbeat(
            Term::new(1),
            leader_id,
            LogIndex::ZERO,
            Term::ZERO,
            LogIndex::ZERO,
        );

    let message = RaftMessage::new(
        leader_id,
        follower_id,
        RaftMessagePayload::AppendEntries(request),
    );

    assert_eq!(
        message.from,
        leader_id
    );

    assert_eq!(
        message.to,
        follower_id
    );

    match message.payload {
        RaftMessagePayload::AppendEntries(request) => {
            assert_eq!(
                request.term,
                Term::new(1)
            );

            assert_eq!(
                request.leader_id,
                leader_id
            );

            assert!(
                request.entries.is_empty()
            );
        }

        _ => {
            panic!(
                "expected AppendEntries message"
            );
        }
    }
}

#[test]
fn send_queues_message_until_delivered() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let request =
        AppendEntriesRequest::<String>::heartbeat(
            Term::new(1),
            leader_id,
            LogIndex::ZERO,
            Term::ZERO,
            LogIndex::ZERO,
        );

    let message = RaftMessage::new(
        leader_id,
        follower_id,
        RaftMessagePayload::AppendEntries(request),
    );

    let mut transport =
        InMemoryTransport::new();

    assert!(
        !transport.has_pending()
    );

    transport.send(message);

    assert!(
        transport.has_pending()
    );

    assert_eq!(
        transport.pending_count(),
        1
    );
}

#[test]
fn deliver_next_returns_messages_in_order() {
    let first_sender = ServerId::new(1);
    let second_sender = ServerId::new(2);
    let receiver = ServerId::new(3);

    let first = RaftMessage::new(
        first_sender,
        receiver,
        RaftMessagePayload::AppendEntries(
            AppendEntriesRequest::<String>::heartbeat(
                Term::new(1),
                first_sender,
                LogIndex::ZERO,
                Term::ZERO,
                LogIndex::ZERO,
            ),
        ),
    );

    let second = RaftMessage::new(
        second_sender,
        receiver,
        RaftMessagePayload::AppendEntries(
            AppendEntriesRequest::<String>::heartbeat(
                Term::new(1),
                second_sender,
                LogIndex::ZERO,
                Term::ZERO,
                LogIndex::ZERO,
            ),
        ),
    );

    let mut transport =
        InMemoryTransport::new();

    transport.send(first);
    transport.send(second);

    let first_message =
        transport
            .deliver_next()
            .expect(
                "first message"
            );

    assert_eq!(
        first_message.from,
        first_sender
    );

    let second_message =
        transport
            .deliver_next()
            .expect(
                "second message"
            );

    assert_eq!(
        second_message.from,
        second_sender
    );

    assert!(
        transport
            .deliver_next()
            .is_none()
    );
}

#[test]
fn peek_does_not_remove_message() {
    let sender = ServerId::new(1);
    let receiver = ServerId::new(2);

    let message = RaftMessage::new(
        sender,
        receiver,
        RaftMessagePayload::AppendEntries(
            AppendEntriesRequest::<String>::heartbeat(
                Term::new(1),
                sender,
                LogIndex::ZERO,
                Term::ZERO,
                LogIndex::ZERO,
            ),
        ),
    );

    let mut transport =
        InMemoryTransport::new();

    transport.send(message);

    assert!(
        transport.peek().is_some()
    );

    assert_eq!(
        transport.pending_count(),
        1
    );

    let delivered =
        transport
            .deliver_next()
            .expect(
                "message should exist"
            );

    assert_eq!(
        delivered.from,
        sender
    );

    assert!(
        transport.peek().is_none()
    );
}

#[test]
fn transport_delivers_append_entries_to_follower() {
    let leader_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let cluster = [
        leader_id,
        follower_id,
    ];

    let mut leader =
        RaftNode::<String>::new(leader_id);

    let mut follower =
        RaftNode::<String>::new(follower_id);

    // Elect the leader.
    leader.start_election();

    leader.handle_request_vote_response(
        follower_id,
        RequestVoteResponse::granted(
            Term::new(1),
        ),
        &cluster,
    );

    assert_eq!(
        leader.role(),
        Role::Leader
    );

    // Build an AppendEntries request
    // from the leader.
    let request =
        leader
            .build_append_entries(follower_id)
            .expect(
                "leader should build AppendEntries"
            );

    let message = RaftMessage::new(
        leader_id,
        follower_id,
        RaftMessagePayload::AppendEntries(
            request,
        ),
    );

    let mut transport =
        InMemoryTransport::new();

    transport.send(message);

    assert_eq!(
        transport.pending_count(),
        1
    );

    // Deliver the message.
    let message =
        transport
            .deliver_next()
            .expect(
                "message should be delivered"
            );

    assert_eq!(
        message.from,
        leader_id
    );

    assert_eq!(
        message.to,
        follower_id
    );

    // The transport has delivered
    // the message. Now the destination
    // node handles it.
    let response =
        match message.payload {
            RaftMessagePayload::AppendEntries(
                request,
            ) => {
                follower
                    .handle_append_entries(
                        request,
                    )
            }

            _ => {
                panic!(
                    "expected AppendEntries"
                );
            }
        };

    assert!(
        response.success
    );

    assert_eq!(
        follower.current_term(),
        Term::new(1)
    );

    assert_eq!(
        follower.role(),
        Role::Follower
    );
}

#[test]
fn transport_delivers_request_vote_to_follower() {
    let candidate_id = ServerId::new(1);
    let follower_id = ServerId::new(2);

    let cluster = [
        candidate_id,
        follower_id,
    ];

    let mut candidate =
        RaftNode::<String>::new(candidate_id);

    let mut follower =
        RaftNode::<String>::new(follower_id);

    candidate.start_election();

    let request =
        candidate.build_request_vote();

    let message = RaftMessage::new(
        candidate_id,
        follower_id,
        RaftMessagePayload::<String>::RequestVote(
            request,
        ),
    );

    let mut transport =
        InMemoryTransport::new();

    transport.send(message);

    let message =
        transport
            .deliver_next()
            .expect(
                "RequestVote should be delivered"
            );

    assert_eq!(
        message.from,
        candidate_id
    );

    assert_eq!(
        message.to,
        follower_id
    );

    let response =
        match message.payload {
            RaftMessagePayload::RequestVote(
                request,
            ) => {
                follower
                    .handle_request_vote(
                        request,
                    )
            }

            _ => {
                panic!(
                    "expected RequestVote"
                );
            }
        };

    assert!(
        response.vote_granted
    );

    candidate.handle_request_vote_response(
        follower_id,
        response,
        &cluster,
    );

    assert_eq!(
        candidate.role(),
        Role::Leader
    );
}

#[test]
fn transport_can_drop_message_for_server() {
    let mut transport =
        InMemoryTransport::<String>::new();

    let message_one =
        RaftMessage::new(
            ServerId::new(1),
            ServerId::new(2),
            RaftMessagePayload::<String>::RequestVote(
                RequestVoteRequest::new(
                    Term::new(1),
                    ServerId::new(1),
                    LogIndex::ZERO,
                    Term::ZERO,
                ),
            ),
        );

    let message_two =
        RaftMessage::new(
            ServerId::new(1),
            ServerId::new(3),
            RaftMessagePayload::<String>::RequestVote(
                RequestVoteRequest::new(
                    Term::new(1),
                    ServerId::new(1),
                    LogIndex::ZERO,
                    Term::ZERO,
                ),
            ),
        );

    transport.send(message_one);
    transport.send(message_two);

    assert_eq!(
        transport.pending_count(),
        2,
    );

    assert!(
        transport.drop_to(
            ServerId::new(2),
        )
    );

    assert_eq!(
        transport.pending_count(),
        1,
    );

    let remaining =
        transport
            .deliver_next()
            .expect(
                "message to server 3 should remain",
            );

    assert_eq!(
        remaining.to,
        ServerId::new(3),
    );

    assert!(
        !transport.drop_to(
            ServerId::new(99),
        )
    );
}

#[test]
fn transport_can_reorder_messages() {
    let mut transport =
        InMemoryTransport::<String>::new();

    let message_one =
        RaftMessage::new(
            ServerId::new(1),
            ServerId::new(2),
            RaftMessagePayload::<String>::RequestVote(
                RequestVoteRequest::new(
                    Term::new(1),
                    ServerId::new(1),
                    LogIndex::ZERO,
                    Term::ZERO,
                ),
            ),
        );

    let message_two =
        RaftMessage::new(
            ServerId::new(1),
            ServerId::new(2),
            RaftMessagePayload::<String>::RequestVote(
                RequestVoteRequest::new(
                    Term::new(2),
                    ServerId::new(1),
                    LogIndex::ZERO,
                    Term::ZERO,
                ),
            ),
        );

    transport.send(message_one);
    transport.send(message_two);

    assert_eq!(
        transport.pending_count(),
        2,
    );

    // Queue position is zero-based.
    // Position 0 is the first message.
    // Position 1 is the second message.
    let delivered =
        transport
            .deliver_at(1)
            .expect(
                "second message should exist",
            );

    match delivered.payload {
        RaftMessagePayload::RequestVote(
            request,
        ) => {
            assert_eq!(
                request.term,
                Term::new(2),
            );
        }

        _ => panic!(
            "expected RequestVote message"
        ),
    }

    assert_eq!(
        transport.pending_count(),
        1,
    );

    let remaining =
        transport
            .deliver_next()
            .expect(
                "first message should remain",
            );

    match remaining.payload {
        RaftMessagePayload::RequestVote(
            request,
        ) => {
            assert_eq!(
                request.term,
                Term::new(1),
            );
        }

        _ => panic!(
            "expected RequestVote message"
        ),
    }

    assert_eq!(
        transport.pending_count(),
        0,
    );
}