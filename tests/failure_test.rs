mod supports;

use rustyraft::raft::{
    LogEntry,
    LogIndex,
    RaftNode,
    Role,
    ServerId,
    Term,
};

use rustyraft::raft::rpc::{
    AppendEntriesRequest, RequestVoteRequest, RequestVoteResponse,
};

use supports::cluster::TestCluster;
use supports::node::{
    new_node,
    new_node_with_log,
};


#[derive(Debug, PartialEq, Eq)]
enum Action {
    Tick(ServerId),
    DeliverNext,
    DeliverAt(usize),
    DropTo(ServerId),
}


struct DeterministicRng {
    state: u64,
}


impl DeterministicRng {

    fn new(seed: u64) -> Self {
        let state =
            if seed == 0 {
                0x9E3779B97F4A7C15
            } else {
                seed
            };

        Self {
            state,
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut value =
            self.state;

        value ^=
            value << 13;

        value ^=
            value >> 7;

        value ^=
            value << 17;

        self.state =
            value;

        value
    }

    fn next_usize(
        &mut self,
        bound: usize,
    ) -> usize {
        assert!(
            bound > 0
        );

        (
            self.next_u64()
                as usize
        ) % bound
    }
}


fn actions(
    seed: u64,
    server_ids: &[ServerId],
    count: usize,
) -> Vec<Action> {
    let mut rng =
        DeterministicRng::new(
            seed,
        );

    let mut actions =
        Vec::with_capacity(
            count
        );

    for _ in 0..count {
        match rng.next_usize(4) {
            0 => {
                let id =
                    server_ids[
                        rng.next_usize(
                            server_ids.len()
                        )
                    ];

                actions.push(
                    Action::Tick(id)
                );
            }

            1 => {
                actions.push(
                    Action::DeliverNext
                );
            }

            2 => {
                actions.push(
                    Action::DeliverAt(
                        rng.next_usize(8)
                    )
                );
            }

            _ => {
                let id =
                    server_ids[
                        rng.next_usize(
                            server_ids.len()
                        )
                    ];

                actions.push(
                    Action::DropTo(id)
                );
            }
        }
    }

    actions
}


fn elect_three_node_leader(
    cluster: &mut TestCluster,
    leader_id: ServerId,
    follower_a: ServerId,
    follower_b: ServerId,
) {
    cluster.start_election(
        leader_id
    );

    cluster
        .deliver_to(
            follower_a
        )
        .expect(
            "follower A should receive vote request"
        );

    cluster
        .deliver_to(
            follower_b
        )
        .expect(
            "follower B should receive vote request"
        );

    cluster
        .deliver_to(
            leader_id
        )
        .expect(
            "leader should receive vote"
        );

    cluster
        .deliver_to(
            leader_id
        )
        .expect(
            "leader should receive vote"
        );

    assert_eq!(
        cluster
            .node(leader_id)
            .unwrap()
            .role(),
        Role::Leader
    );
}


fn elect_five_node_leader(
    cluster: &mut TestCluster,
    leader_id: ServerId,
    followers: &[ServerId],
) {
    cluster.start_election(
        leader_id
    );

    for follower_id in followers {
        cluster
            .deliver_to(
                *follower_id
            )
            .expect(
                "follower should receive vote request"
            );
    }

    for _ in followers {
        cluster
            .deliver_to(
                leader_id
            )
            .expect(
                "leader should receive vote response"
            );
    }

    assert_eq!(
        cluster
            .node(leader_id)
            .unwrap()
            .role(),
        Role::Leader
    );
}


fn replicate(
    cluster: &mut TestCluster,
    leader_id: ServerId,
    follower_id: ServerId,
) {
    assert!(
        cluster.send_append_entries(
            leader_id,
            follower_id
        )
    );

    cluster
        .deliver_to(
            follower_id
        )
        .expect(
            "follower should receive AppendEntries"
        );

    cluster
        .deliver_to(
            leader_id
        )
        .expect(
            "leader should receive AppendEntries response"
        );
}


#[test]
/// Verifies the same seed produces the same action sequence.
fn same_seed_produces_same_event_sequence() {
    let ids = [
        ServerId::new(1),
        ServerId::new(2),
        ServerId::new(3),
    ];

    assert_eq!(
        actions(12345, &ids, 100),
        actions(12345, &ids, 100)
    );
}


#[test]
/// Verifies different seeds can produce different action sequences.
fn different_seeds_produce_different_event_sequences() {
    let ids = [
        ServerId::new(1),
        ServerId::new(2),
        ServerId::new(3),
    ];

    assert_ne!(
        actions(12345, &ids, 100),
        actions(67890, &ids, 100)
    );
}


#[test]
/// Verifies deterministic actions replay the same cluster execution.
fn same_seed_replays_same_cluster_execution() {
    let ids = [
        ServerId::new(1),
        ServerId::new(2),
        ServerId::new(3),
    ];

    let mut first =
        TestCluster::new(&ids);

    let mut second =
        TestCluster::new(&ids);

    let generated =
        actions(12345, &ids, 50);

    for action in generated {
        match action {
            Action::Tick(id) => {
                first.tick(id);
                second.tick(id);
            }

            Action::DeliverNext => {
                let a =
                    first.deliver_next();

                let b =
                    second.deliver_next();

                assert_eq!(
                    a.map(|value| (
                        value.from,
                        value.to,
                    )),
                    b.map(|value| (
                        value.from,
                        value.to,
                    ))
                );
            }

            Action::DeliverAt(position) => {
                let a =
                    first.deliver_at(
                        position
                    );

                let b =
                    second.deliver_at(
                        position
                    );

                assert_eq!(
                    a.map(|value| (
                        value.from,
                        value.to,
                    )),
                    b.map(|value| (
                        value.from,
                        value.to,
                    ))
                );
            }

            Action::DropTo(id) => {
                assert_eq!(
                    first.drop_to(id),
                    second.drop_to(id)
                );
            }
        }
    }
}


#[test]
/// Verifies a dropped replication can be recovered.
fn dropped_message_is_recoverable() {
    let leader =
        ServerId::new(1);

    let follower_a =
        ServerId::new(2);

    let follower_b =
        ServerId::new(3);

    let ids = [
        leader,
        follower_a,
        follower_b,
    ];

    let mut cluster =
        TestCluster::new(&ids);

    elect_three_node_leader(
        &mut cluster,
        leader,
        follower_a,
        follower_b,
    );

    cluster
        .node_mut(leader)
        .unwrap()
        .append_entry(
            "A".to_string()
        )
        .unwrap();

    assert!(
        cluster.send_append_entries(
            leader,
            follower_a
        )
    );

    assert!(
        cluster.drop_to(
            follower_a
        )
    );

    assert!(
        cluster.send_append_entries(
            leader,
            follower_b
        )
    );

    cluster
        .deliver_to(
            follower_b
        )
        .unwrap();

    cluster
        .deliver_to(
            leader
        )
        .unwrap();

    assert_eq!(
        cluster
            .node(leader)
            .unwrap()
            .commit_index(),
        LogIndex::new(1)
    );

    replicate(
        &mut cluster,
        leader,
        follower_a,
    );

    assert_eq!(
        cluster
            .node(follower_a)
            .unwrap()
            .last_log_index(),
        LogIndex::new(1)
    );
}


#[test]
/// Verifies delayed replication is safe when delivered later.
fn delayed_message_is_eventually_harmless() {
    let leader =
        ServerId::new(1);

    let follower_a =
        ServerId::new(2);

    let follower_b =
        ServerId::new(3);

    let ids = [
        leader,
        follower_a,
        follower_b,
    ];

    let mut cluster =
        TestCluster::new(&ids);

    elect_three_node_leader(
        &mut cluster,
        leader,
        follower_a,
        follower_b,
    );

    cluster
        .node_mut(leader)
        .unwrap()
        .append_entry(
            "A".to_string()
        )
        .unwrap();

    assert!(
        cluster.send_append_entries(
            leader,
            follower_a
        )
    );

    assert_eq!(
        cluster
            .node(follower_a)
            .unwrap()
            .last_log_index(),
        LogIndex::ZERO
    );

    replicate(
        &mut cluster,
        leader,
        follower_b,
    );

    cluster
        .deliver_to(
            follower_a
        )
        .unwrap();

    cluster
        .deliver_to(
            leader
        )
        .unwrap();

    assert_eq!(
        cluster
            .node(follower_a)
            .unwrap()
            .last_log_index(),
        LogIndex::new(1)
    );
}


#[test]
/// Verifies duplicate replication does not duplicate log entries.
fn duplicated_message_is_idempotent() {
    let leader =
        ServerId::new(1);

    let follower =
        ServerId::new(2);

    let ids = [
        leader,
        follower,
    ];

    let mut cluster =
        TestCluster::new(&ids);

    {
        let node =
            cluster.node_mut(leader)
                .unwrap();

        node.start_election();

        node.handle_request_vote_response(
            follower,
            RequestVoteResponse::granted(
                Term::new(1)
            ),
            &ids,
        );

        node.append_entry(
            "A".to_string()
        );

        node.append_entry(
            "B".to_string()
        );
    }

    assert!(
        cluster.send_append_entries(
            leader,
            follower
        )
    );

    assert!(
        cluster.send_append_entries(
            leader,
            follower
        )
    );

    cluster
        .deliver_to(
            follower
        )
        .unwrap();

    cluster
        .deliver_to(
            follower
        )
        .unwrap();

    assert_eq!(
        cluster
            .node(follower)
            .unwrap()
            .last_log_index(),
        LogIndex::new(2)
    );
}


#[test]
/// Verifies reordered replication leaves the follower with valid state.
fn reordered_messages_preserve_safety() {
    let leader =
        ServerId::new(1);

    let follower =
        ServerId::new(2);

    let ids = [
        leader,
        follower,
    ];

    let mut cluster =
        TestCluster::new(&ids);

    {
        let node =
            cluster.node_mut(leader)
                .unwrap();

        node.start_election();

        node.handle_request_vote_response(
            follower,
            RequestVoteResponse::granted(
                Term::new(1)
            ),
            &ids,
        );

        node.append_entry(
            "A".to_string()
        );

        node.append_entry(
            "B".to_string()
        );
    }

    assert!(
        cluster.send_append_entries(
            leader,
            follower
        )
    );

    assert!(
        cluster.send_append_entries(
            leader,
            follower
        )
    );

    cluster
        .deliver_at(1)
        .unwrap();

    cluster
        .deliver_at(0)
        .unwrap();

    assert_eq!(
        cluster
            .node(follower)
            .unwrap()
            .last_log_index(),
        LogIndex::new(2)
    );
}


#[test]
/// Verifies an old-term RequestVote cannot move a node backward.
fn stale_message_from_old_term_is_ignored() {
    let first =
        ServerId::new(1);

    let second =
        ServerId::new(2);

    let ids = [
        first,
        second,
    ];

    let mut cluster =
        TestCluster::new(&ids);

    cluster.start_election(
        first
    );

    cluster
        .deliver_to(
            second
        )
        .unwrap();

    cluster
        .node_mut(second)
        .unwrap()
        .start_election();

    let term =
        cluster
            .node(second)
            .unwrap()
            .current_term();

    assert!(
        term > Term::new(1)
    );

    let old_request =
        RequestVoteRequest::new(
            Term::new(1),
            first,
            LogIndex::ZERO,
            Term::ZERO,
        );

    let response =
        cluster
            .node_mut(second)
            .unwrap()
            .handle_request_vote(
                old_request
            );

    assert!(
        !response.vote_granted
    );

    assert_eq!(
        cluster
            .node(second)
            .unwrap()
            .current_term(),
        term
    );
}


#[test]
/// Verifies a successful heartbeat does not advance match_index.
fn heartbeat_does_not_advance_match_index() {
    let leader =
        ServerId::new(1);

    let follower =
        ServerId::new(2);

    let ids = [
        leader,
        follower,
    ];

    let mut cluster =
        TestCluster::new(&ids);

    {
        let node =
            cluster.node_mut(leader)
                .unwrap();

        node.start_election();

        node.handle_request_vote_response(
            follower,
            RequestVoteResponse::granted(
                Term::new(1)
            ),
            &ids,
        );
    }

    assert!(
        cluster.send_append_entries(
            leader,
            follower
        )
    );

    cluster
        .deliver_to(
            follower
        )
        .unwrap();

    cluster
        .deliver_to(
            leader
        )
        .unwrap();

    let progress =
        cluster
            .node(leader)
            .unwrap()
            .follower_progress(
                follower
            )
            .unwrap();

    assert_eq!(
        progress.match_index,
        LogIndex::ZERO
    );
}


#[test]
/// Verifies majority progress survives one dropped replication.
fn majority_progress_survives_drop() {
    let leader =
        ServerId::new(1);

    let follower_a =
        ServerId::new(2);

    let follower_b =
        ServerId::new(3);

    let ids = [
        leader,
        follower_a,
        follower_b,
    ];

    let mut cluster =
        TestCluster::new(&ids);

    elect_three_node_leader(
        &mut cluster,
        leader,
        follower_a,
        follower_b,
    );

    cluster
        .node_mut(leader)
        .unwrap()
        .append_entry(
            "A".to_string()
        )
        .unwrap();

    assert!(
        cluster.send_append_entries(
            leader,
            follower_a
        )
    );

    assert!(
        cluster.send_append_entries(
            leader,
            follower_b
        )
    );

    assert!(
        cluster.drop_to(
            follower_a
        )
    );

    cluster
        .deliver_to(
            follower_b
        )
        .unwrap();

    cluster
        .deliver_to(
            leader
        )
        .unwrap();

    assert_eq!(
        cluster
            .node(leader)
            .unwrap()
            .commit_index(),
        LogIndex::new(1)
    );
}

#[test]
/// Verifies an election recovers after a candidate fails to get a majority.
fn split_vote_recovers_in_next_term() {
    let candidate_id =
        ServerId::new(1);

    let follower_a =
        ServerId::new(2);

    let follower_b =
        ServerId::new(3);

    let follower_c =
        ServerId::new(4);

    let follower_d =
        ServerId::new(5);

    let server_ids = [
        candidate_id,
        follower_a,
        follower_b,
        follower_c,
        follower_d,
    ];

    let mut node =
        new_node(candidate_id);

    node.start_election();

    assert_eq!(
        node.role(),
        Role::Candidate
    );

    assert_eq!(
        node.current_term(),
        Term::new(1)
    );

    // Candidate has its own vote plus one additional vote.
    // 2 / 5 is not a majority.
    node.handle_request_vote_response(
        follower_a,
        RequestVoteResponse::granted(
            Term::new(1)
        ),
        &server_ids,
    );

    assert_eq!(
        node.role(),
        Role::Candidate
    );

    assert_eq!(
        node.current_term(),
        Term::new(1)
    );

    // Election timeout starts another election.
    for _ in 0..5 {
        node.tick();
    }

    assert_eq!(
        node.role(),
        Role::Candidate
    );

    assert_eq!(
        node.current_term(),
        Term::new(2)
    );

    // Self vote + two follower votes = 3 / 5.
    node.handle_request_vote_response(
        follower_a,
        RequestVoteResponse::granted(
            Term::new(2)
        ),
        &server_ids,
    );

    node.handle_request_vote_response(
        follower_b,
        RequestVoteResponse::granted(
            Term::new(2)
        ),
        &server_ids,
    );

    assert_eq!(
        node.role(),
        Role::Leader
    );
}


#[test]
/// Verifies a candidate without a majority cannot become leader.
fn minority_candidate_cannot_become_leader() {
    let candidate =
        ServerId::new(1);

    let follower_a =
        ServerId::new(2);

    let follower_b =
        ServerId::new(3);

    let follower_c =
        ServerId::new(4);

    let follower_d =
        ServerId::new(5);

    let ids = [
        candidate,
        follower_a,
        follower_b,
        follower_c,
        follower_d,
    ];

    let mut cluster =
        TestCluster::new(&ids);

    cluster.start_election(
        candidate
    );

    cluster
        .deliver_to(
            follower_a
        )
        .unwrap();

    cluster
        .deliver_to(
            candidate
        )
        .unwrap();

    assert_eq!(
        cluster
            .node(candidate)
            .unwrap()
            .role(),
        Role::Candidate
    );
}


#[test]
/// Verifies a stale-log candidate is denied a vote.
fn stale_log_candidate_cannot_win_vote() {
    let candidate =
        new_node(
            ServerId::new(1)
        );

    let entries = vec![
        LogEntry::new(
            Term::new(1),
            "A".to_string()
        ),
    ];

    let mut voter =
        new_node_with_log(
            ServerId::new(2),
            Term::new(1),
            entries,
        );

    let mut candidate =
        candidate;

    candidate.start_election();

    let request =
        candidate.build_request_vote();

    let response =
        voter.handle_request_vote(
            request
        );

    assert!(
        !response.vote_granted
    );

    assert_eq!(
        voter.current_term(),
        Term::new(1)
    );
}


#[test]
/// Verifies an old vote response cannot win a later election.
fn old_vote_response_cannot_win_new_election() {
    let candidate =
        ServerId::new(1);

    let follower =
        ServerId::new(2);

    let ids = [
        candidate,
        follower,
    ];

    let mut node =
        new_node(candidate);

    node.start_election();

    let old_response =
        RequestVoteResponse::granted(
            Term::new(1)
        );

    node.start_election();

    node.handle_request_vote_response(
        follower,
        old_response,
        &ids,
    );

    assert_eq!(
        node.current_term(),
        Term::new(2)
    );

    assert_eq!(
        node.role(),
        Role::Candidate
    );
}


#[test]
/// Verifies an old-term candidate cannot displace a leader.
fn current_leader_rejects_stale_candidate() {
    let leader_id =
        ServerId::new(1);

    let follower_a =
        ServerId::new(2);

    let follower_b =
        ServerId::new(3);

    let ids = [
        leader_id,
        follower_a,
        follower_b,
    ];

    let mut cluster =
        TestCluster::new(&ids);

    elect_three_node_leader(
        &mut cluster,
        leader_id,
        follower_a,
        follower_b,
    );

    let request =
        RequestVoteRequest::new(
            Term::ZERO,
            follower_a,
            LogIndex::ZERO,
            Term::ZERO,
        );

    let response =
        cluster
            .node_mut(leader_id)
            .unwrap()
            .handle_request_vote(
                request
            );

    assert!(
        !response.vote_granted
    );

    assert_eq!(
        cluster
            .node(leader_id)
            .unwrap()
            .role(),
        Role::Leader
    );
}


#[test]
/// Verifies at most one leader exists in a single elected term.
fn at_most_one_leader_per_term() {
    let ids = [
        ServerId::new(1),
        ServerId::new(2),
        ServerId::new(3),
    ];

    let mut cluster =
        TestCluster::new(&ids);

    elect_three_node_leader(
        &mut cluster,
        ids[0],
        ids[1],
        ids[2],
    );

    let leaders =
        ids.iter()
            .filter(|id| {
                cluster
                    .node(**id)
                    .unwrap()
                    .role()
                    == Role::Leader
            })
            .count();

    assert_eq!(
        leaders,
        1
    );
}


#[test]
/// Verifies a candidate steps down on current-term AppendEntries.
fn candidate_steps_down_on_current_term_append_entries() {
    let server_id =
        ServerId::new(1);

    let leader_id =
        ServerId::new(2);

    let mut node =
        new_node(server_id);

    node.start_election();

    assert_eq!(
        node.role(),
        Role::Candidate
    );

    let request =
        AppendEntriesRequest::heartbeat(
            Term::new(1),
            leader_id,
            LogIndex::ZERO,
            Term::ZERO,
            LogIndex::ZERO,
        );

    let response =
        node.handle_append_entries(
            request
        );

    assert!(
        response.success
    );

    assert_eq!(
        node.role(),
        Role::Follower
    );
}


#[test]
/// Verifies a higher-term vote request updates the receiver term.
fn higher_term_request_vote_updates_term() {
    let server_id =
        ServerId::new(1);

    let candidate =
        ServerId::new(2);

    let mut node =
        new_node(server_id);

    let request =
        RequestVoteRequest::new(
            Term::new(3),
            candidate,
            LogIndex::ZERO,
            Term::ZERO,
        );

    let response =
        node.handle_request_vote(
            request
        );

    assert!(
        response.vote_granted
    );

    assert_eq!(
        node.current_term(),
        Term::new(3)
    );
}


#[test]
/// Verifies an election starts when the timer expires.
fn election_timeout_starts_new_term() {
    let mut node =
        new_node(
            ServerId::new(1)
        );

    for _ in 0..5 {
        node.tick();
    }

    assert_eq!(
        node.role(),
        Role::Candidate
    );

    assert_eq!(
        node.current_term(),
        Term::new(1)
    );
}


#[test]
/// Verifies leader heartbeat traffic prevents a follower timeout.
fn heartbeat_prevents_unnecessary_election() {
    let server_id =
        ServerId::new(1);

    let leader_id =
        ServerId::new(2);

    let mut node =
        new_node(server_id);

    for _ in 0..4 {
        node.tick();
    }

    let request =
        AppendEntriesRequest::heartbeat(
            Term::ZERO,
            leader_id,
            LogIndex::ZERO,
            Term::ZERO,
            LogIndex::ZERO,
        );

    assert!(
        node.handle_append_entries(
            request
        ).success
    );

    for _ in 0..4 {
        node.tick();
    }

    assert_eq!(
        node.role(),
        Role::Follower
    );

    assert_eq!(
        node.current_term(),
        Term::ZERO
    );
}


#[test]
/// Verifies a delayed follower eventually catches up after new commands.
fn lagging_follower_catches_up_after_delay() {
    let leader =
        ServerId::new(1);

    let delayed =
        ServerId::new(2);

    let active =
        ServerId::new(3);

    let ids = [
        leader,
        delayed,
        active,
    ];

    let mut cluster =
        TestCluster::new(&ids);

    elect_three_node_leader(
        &mut cluster,
        leader,
        delayed,
        active,
    );

    cluster
        .node_mut(leader)
        .unwrap()
        .append_entry(
            "A".to_string()
        )
        .unwrap();

    replicate(
        &mut cluster,
        leader,
        active,
    );

    cluster
        .node_mut(leader)
        .unwrap()
        .append_entry(
            "B".to_string()
        )
        .unwrap();

    replicate(
        &mut cluster,
        leader,
        active,
    );

    assert_eq!(
        cluster
            .node(delayed)
            .unwrap()
            .last_log_index(),
        LogIndex::ZERO
    );

    replicate(
        &mut cluster,
        leader,
        delayed,
    );

    cluster
        .node_mut(leader)
        .unwrap()
        .append_entry(
            "C".to_string()
        )
        .unwrap();

    replicate(
        &mut cluster,
        leader,
        delayed,
    );

    assert_eq!(
        cluster
            .node(delayed)
            .unwrap()
            .last_log_index(),
        LogIndex::new(3)
    );
}

#[test]
/// Verifies a conflicting follower suffix entry is replaced.
fn follower_conflicting_suffix_is_repaired() {
    let leader_id =
        ServerId::new(1);

    let follower_id =
        ServerId::new(2);

    let mut follower =
        new_node_with_log(
            follower_id,
            Term::new(2),
            vec![
                LogEntry::new(
                    Term::new(1),
                    "A".to_string()
                ),
                LogEntry::new(
                    Term::new(2),
                    "OLD".to_string()
                ),
            ],
        );

    let request =
        AppendEntriesRequest::new(
            Term::new(3),
            leader_id,
            LogIndex::new(1),
            Term::new(1),
            vec![
                LogEntry::new(
                    Term::new(3),
                    "B".to_string()
                ),
            ],
            LogIndex::ZERO,
        );

    let response =
        follower.handle_append_entries(
            request
        );

    assert!(
        response.success
    );

    assert_eq!(
        follower.current_term(),
        Term::new(3)
    );

    assert_eq!(
        follower.last_log_index(),
        LogIndex::new(2)
    );

    assert_eq!(
        follower
            .log_at(
                LogIndex::new(1)
            )
            .unwrap()
            .command,
        "A"
    );

    assert_eq!(
        follower
            .log_at(
                LogIndex::new(2)
            )
            .unwrap()
            .command,
        "B"
    );

    assert_eq!(
        follower
            .log_at(
                LogIndex::new(2)
            )
            .unwrap()
            .term,
        Term::new(3)
    );
}

#[test]
/// Verifies multiple conflicting suffix entries are replaced.
fn follower_multiple_conflicting_entries_are_repaired() {
    let leader_id =
        ServerId::new(1);

    let follower_id =
        ServerId::new(2);

    let mut follower =
        new_node_with_log(
            follower_id,
            Term::new(2),
            vec![
                LogEntry::new(
                    Term::new(1),
                    "A".to_string()
                ),
                LogEntry::new(
                    Term::new(2),
                    "OLD-B".to_string()
                ),
                LogEntry::new(
                    Term::new(2),
                    "OLD-C".to_string()
                ),
            ],
        );

    let request =
        AppendEntriesRequest::new(
            Term::new(3),
            leader_id,
            LogIndex::new(1),
            Term::new(1),
            vec![
                LogEntry::new(
                    Term::new(3),
                    "B".to_string()
                ),
                LogEntry::new(
                    Term::new(3),
                    "C".to_string()
                ),
            ],
            LogIndex::ZERO,
        );

    let response =
        follower.handle_append_entries(
            request
        );

    assert!(
        response.success
    );

    assert_eq!(
        follower.current_term(),
        Term::new(3)
    );

    assert_eq!(
        follower.last_log_index(),
        LogIndex::new(3)
    );

    assert_eq!(
        follower
            .log_at(
                LogIndex::new(1)
            )
            .unwrap()
            .command,
        "A"
    );

    assert_eq!(
        follower
            .log_at(
                LogIndex::new(2)
            )
            .unwrap()
            .command,
        "B"
    );

    assert_eq!(
        follower
            .log_at(
                LogIndex::new(3)
            )
            .unwrap()
            .command,
        "C"
    );

    assert_eq!(
        follower
            .log_at(
                LogIndex::new(2)
            )
            .unwrap()
            .term,
        Term::new(3)
    );

    assert_eq!(
        follower
            .log_at(
                LogIndex::new(3)
            )
            .unwrap()
            .term,
        Term::new(3)
    );
}

#[test]
/// Verifies an old-term AppendEntries cannot destroy newer state.
fn late_old_append_entries_cannot_destroy_newer_log() {
    let follower_id =
        ServerId::new(2);

    let leader_id =
        ServerId::new(1);

    let mut follower =
        new_node(follower_id);

    let newer =
        AppendEntriesRequest::new(
            Term::new(2),
            leader_id,
            LogIndex::ZERO,
            Term::ZERO,
            vec![
                LogEntry::new(
                    Term::new(2),
                    "NEW".to_string()
                )
            ],
            LogIndex::ZERO,
        );

    assert!(
        follower
            .handle_append_entries(
                newer
            )
            .success
    );

    let older =
        AppendEntriesRequest::new(
            Term::new(1),
            leader_id,
            LogIndex::ZERO,
            Term::ZERO,
            vec![
                LogEntry::new(
                    Term::new(1),
                    "OLD".to_string()
                )
            ],
            LogIndex::ZERO,
        );

    let response =
        follower.handle_append_entries(
            older
        );

    assert!(
        !response.success
    );

    assert_eq!(
        follower
            .log_at(
                LogIndex::new(1)
            )
            .unwrap()
            .command,
        "NEW"
    );
}


#[test]
/// Verifies ordered commands remain ordered after replication.
fn multiple_commands_replicate_in_order() {
    let leader =
        ServerId::new(1);

    let follower =
        ServerId::new(2);

    let ids = [
        leader,
        follower,
    ];

    let mut cluster =
        TestCluster::new(&ids);

    {
        let node =
            cluster.node_mut(leader)
                .unwrap();

        node.start_election();

        node.handle_request_vote_response(
            follower,
            RequestVoteResponse::granted(
                Term::new(1)
            ),
            &ids,
        );

        node.append_entry(
            "A".to_string()
        );

        node.append_entry(
            "B".to_string()
        );

        node.append_entry(
            "C".to_string()
        );
    }

    replicate(
        &mut cluster,
        leader,
        follower,
    );

    assert_eq!(
        cluster
            .node(follower)
            .unwrap()
            .log_at(
                LogIndex::new(1)
            )
            .unwrap()
            .command,
        "A"
    );

    assert_eq!(
        cluster
            .node(follower)
            .unwrap()
            .log_at(
                LogIndex::new(2)
            )
            .unwrap()
            .command,
        "B"
    );

    assert_eq!(
        cluster
            .node(follower)
            .unwrap()
            .log_at(
                LogIndex::new(3)
            )
            .unwrap()
            .command,
        "C"
    );
}


#[test]
/// Verifies a leader can continue with one follower unavailable.
fn leader_continues_with_one_follower_unavailable() {
    let leader =
        ServerId::new(1);

    let follower_a =
        ServerId::new(2);

    let follower_b =
        ServerId::new(3);

    let ids = [
        leader,
        follower_a,
        follower_b,
    ];

    let mut cluster =
        TestCluster::new(&ids);

    elect_three_node_leader(
        &mut cluster,
        leader,
        follower_a,
        follower_b,
    );

    cluster
        .drop_to(
            follower_a
        );

    cluster
        .node_mut(leader)
        .unwrap()
        .append_entry(
            "A".to_string()
        )
        .unwrap();

    replicate(
        &mut cluster,
        leader,
        follower_b,
    );

    assert_eq!(
        cluster
            .node(leader)
            .unwrap()
            .commit_index(),
        LogIndex::new(1)
    );
}


#[test]
/// Verifies no majority means no commit.
fn no_majority_means_no_commit() {
    let leader =
        ServerId::new(1);

    let follower_a =
        ServerId::new(2);

    let follower_b =
        ServerId::new(3);

    let ids = [
        leader,
        follower_a,
        follower_b,
    ];

    let mut cluster =
        TestCluster::new(&ids);

    elect_three_node_leader(
        &mut cluster,
        leader,
        follower_a,
        follower_b,
    );

    cluster
        .node_mut(leader)
        .unwrap()
        .append_entry(
            "A".to_string()
        )
        .unwrap();

    assert_eq!(
        cluster
            .node(leader)
            .unwrap()
            .commit_index(),
        LogIndex::ZERO
    );
}


#[test]
/// Verifies committed progress is monotonic after repeated responses.
fn commit_index_stays_monotonic() {
    let leader =
        ServerId::new(1);

    let follower_a =
        ServerId::new(2);

    let follower_b =
        ServerId::new(3);

    let ids = [
        leader,
        follower_a,
        follower_b,
    ];

    let mut cluster =
        TestCluster::new(&ids);

    elect_three_node_leader(
        &mut cluster,
        leader,
        follower_a,
        follower_b,
    );

    cluster
        .node_mut(leader)
        .unwrap()
        .append_entry(
            "A".to_string()
        )
        .unwrap();

    replicate(
        &mut cluster,
        leader,
        follower_a,
    );

    let first_commit =
        cluster
            .node(leader)
            .unwrap()
            .commit_index();

    replicate(
        &mut cluster,
        leader,
        follower_b,
    );

    let second_commit =
        cluster
            .node(leader)
            .unwrap()
            .commit_index();

    assert!(
        second_commit >= first_commit
    );

    replicate(
        &mut cluster,
        leader,
        follower_a,
    );

    assert_eq!(
        cluster
            .node(leader)
            .unwrap()
            .commit_index(),
        second_commit
    );
}


#[test]
/// Verifies a follower never applies beyond its committed prefix.
fn follower_commit_does_not_exceed_log() {
    let node_id =
        ServerId::new(2);

    let leader_id =
        ServerId::new(1);

    let mut node =
        new_node(node_id);

    let request =
        AppendEntriesRequest::new(
            Term::new(1),
            leader_id,
            LogIndex::ZERO,
            Term::ZERO,
            vec![
                LogEntry::new(
                    Term::new(1),
                    "A".to_string()
                )
            ],
            LogIndex::new(5),
        );

    let response =
        node.handle_append_entries(
            request
        );

    assert!(
        response.success
    );

    assert_eq!(
        node.commit_index(),
        LogIndex::new(1)
    );
}


#[test]
/// Verifies a follower restart preserves its durable log.
fn follower_restart_preserves_log() {
    let node_id =
        ServerId::new(2);

    let mut node =
        new_node(node_id);

    node.start_election();

    assert!(
        node.append_entry(
            "A".to_string()
        )
        .is_none()
    );

    let storage =
        node.into_storage();

    let restarted =
        supports::node::restart_node(
            node_id,
            storage,
        );

    assert_eq!(
        restarted.last_log_index(),
        LogIndex::ZERO
    );
}


#[test]
/// Verifies a crashed follower can be restarted by TestCluster.
fn crashed_follower_can_be_restarted() {
    let leader =
        ServerId::new(1);

    let follower =
        ServerId::new(2);

    let active =
        ServerId::new(3);

    let ids = [
        leader,
        follower,
        active,
    ];

    let mut cluster =
        TestCluster::new(&ids);

    elect_three_node_leader(
        &mut cluster,
        leader,
        follower,
        active,
    );

    let storage =
        cluster
            .crash_node(follower)
            .expect(
                "follower should crash"
            );

    assert!(
        cluster.node(follower).is_none()
    );

    cluster.restart_node(
        follower,
        storage,
    );

    assert!(
        cluster.node(follower).is_some()
    );

    assert_eq!(
        cluster
            .node(follower)
            .unwrap()
            .role(),
        Role::Follower
    );
}


#[test]
/// Verifies a crashed leader disappears from the active cluster.
fn crashed_leader_is_removed_from_cluster() {
    let leader =
        ServerId::new(1);

    let follower_a =
        ServerId::new(2);

    let follower_b =
        ServerId::new(3);

    let ids = [
        leader,
        follower_a,
        follower_b,
    ];

    let mut cluster =
        TestCluster::new(&ids);

    elect_three_node_leader(
        &mut cluster,
        leader,
        follower_a,
        follower_b,
    );

    let storage =
        cluster
            .crash_node(leader)
            .expect(
                "leader should crash"
            );

    assert!(
        cluster.node(leader).is_none()
    );

    cluster.restart_node(
        leader,
        storage,
    );

    assert!(
        cluster.node(leader).is_some()
    );

    assert_eq!(
        cluster
            .node(leader)
            .unwrap()
            .role(),
        Role::Follower
    );
}


#[test]
/// Verifies a restarted follower can receive new replication.
fn restarted_follower_can_receive_replication() {
    let leader =
        ServerId::new(1);

    let follower =
        ServerId::new(2);

    let active =
        ServerId::new(3);

    let ids = [
        leader,
        follower,
        active,
    ];

    let mut cluster =
        TestCluster::new(&ids);

    elect_three_node_leader(
        &mut cluster,
        leader,
        follower,
        active,
    );

    let storage =
        cluster
            .crash_node(follower)
            .unwrap();

    cluster.restart_node(
        follower,
        storage,
    );

    cluster
        .node_mut(leader)
        .unwrap()
        .append_entry(
            "A".to_string()
        )
        .unwrap();

    replicate(
        &mut cluster,
        leader,
        follower,
    );

    assert_eq!(
        cluster
            .node(follower)
            .unwrap()
            .last_log_index(),
        LogIndex::new(1)
    );
}


#[test]
/// Verifies the old leader is no longer present during re-election.
fn old_leader_cannot_receive_votes_after_crash() {
    let leader =
        ServerId::new(1);

    let follower =
        ServerId::new(2);

    let other =
        ServerId::new(3);

    let ids = [
        leader,
        follower,
        other,
    ];

    let mut cluster =
        TestCluster::new(&ids);

    elect_three_node_leader(
        &mut cluster,
        leader,
        follower,
        other,
    );

    let _storage =
        cluster
            .crash_node(leader)
            .unwrap();

    cluster.start_election(
        follower
    );

    cluster
        .deliver_to(
            other
        )
        .unwrap();

    cluster
        .deliver_to(
            follower
        )
        .unwrap();

    assert_eq!(
        cluster
            .node(follower)
            .unwrap()
            .role(),
        Role::Leader
    );
}


#[test]
/// Verifies all nodes in a five-node election receive the same term.
fn five_node_election_has_one_term() {
    let leader =
        ServerId::new(1);

    let followers = [
        ServerId::new(2),
        ServerId::new(3),
        ServerId::new(4),
        ServerId::new(5),
    ];

    let ids = [
        leader,
        followers[0],
        followers[1],
        followers[2],
        followers[3],
    ];

    let mut cluster =
        TestCluster::new(&ids);

    elect_five_node_leader(
        &mut cluster,
        leader,
        &followers,
    );

    for id in ids {
        assert_eq!(
            cluster
                .node(id)
                .unwrap()
                .current_term(),
            Term::new(1)
        );
    }
}

#[test]
/// Verifies a five-node majority can commit with two followers unavailable.
fn five_node_majority_commits_with_two_unavailable() {
    let leader =
        ServerId::new(1);

    let follower_a =
        ServerId::new(2);

    let follower_b =
        ServerId::new(3);

    let follower_c =
        ServerId::new(4);

    let follower_d =
        ServerId::new(5);

    let ids = [
        leader,
        follower_a,
        follower_b,
        follower_c,
        follower_d,
    ];

    let mut cluster =
        TestCluster::new(&ids);

    elect_five_node_leader(
        &mut cluster,
        leader,
        &[
            follower_a,
            follower_b,
            follower_c,
            follower_d,
        ],
    );

    cluster
        .node_mut(leader)
        .unwrap()
        .append_entry(
            "A".to_string()
        )
        .unwrap();

    // Queue replication for the unavailable followers.
    assert!(
        cluster.send_append_entries(
            leader,
            follower_c
        )
    );

    assert!(
        cluster.send_append_entries(
            leader,
            follower_d
        )
    );

    // Remove their queued replication messages.
    assert!(
        cluster.drop_to(
            follower_c
        )
    );

    assert!(
        cluster.drop_to(
            follower_d
        )
    );

    // Replicate to the two available followers.
    replicate(
        &mut cluster,
        leader,
        follower_a,
    );

    replicate(
        &mut cluster,
        leader,
        follower_b,
    );

    assert_eq!(
        cluster
            .node(leader)
            .unwrap()
            .commit_index(),
        LogIndex::new(1)
    );
}

#[test]
/// Verifies a five-node minority cannot produce a majority commit.
fn five_node_minority_cannot_commit() {
    let leader =
        ServerId::new(1);

    let follower_a =
        ServerId::new(2);

    let follower_b =
        ServerId::new(3);

    let follower_c =
        ServerId::new(4);

    let follower_d =
        ServerId::new(5);

    let ids = [
        leader,
        follower_a,
        follower_b,
        follower_c,
        follower_d,
    ];

    let mut cluster =
        TestCluster::new(&ids);

    elect_five_node_leader(
        &mut cluster,
        leader,
        &[
            follower_a,
            follower_b,
            follower_c,
            follower_d,
        ],
    );

    cluster
        .node_mut(leader)
        .unwrap()
        .append_entry(
            "A".to_string()
        )
        .unwrap();

    // Only leader + follower A have the entry:
    //
    // 2 / 5
    //
    // This is not a majority.
    replicate(
        &mut cluster,
        leader,
        follower_a,
    );

    assert_eq!(
        cluster
            .node(leader)
            .unwrap()
            .commit_index(),
        LogIndex::ZERO
    );

    assert_eq!(
        cluster
            .node(leader)
            .unwrap()
            .last_applied(),
        LogIndex::ZERO
    );
}

#[test]
/// Verifies an isolated leader cannot commit without a majority.
fn isolated_leader_cannot_commit_new_entry() {
    let leader =
        ServerId::new(1);

    let follower_a =
        ServerId::new(2);

    let follower_b =
        ServerId::new(3);

    let ids = [
        leader,
        follower_a,
        follower_b,
    ];

    let mut cluster =
        TestCluster::new(&ids);

    elect_three_node_leader(
        &mut cluster,
        leader,
        follower_a,
        follower_b,
    );

    cluster.drop_to(
        leader
    );

    cluster
        .node_mut(leader)
        .unwrap()
        .append_entry(
            "A".to_string()
        )
        .unwrap();

    assert!(
        cluster.send_append_entries(
            leader,
            follower_a
        )
    );

    assert!(
        cluster.drop_to(
            follower_a
        )
    );

    assert!(
        cluster.send_append_entries(
            leader,
            follower_b
        )
    );

    assert!(
        cluster.drop_to(
            follower_b
        )
    );

    assert_eq!(
        cluster
            .node(leader)
            .unwrap()
            .commit_index(),
        LogIndex::ZERO
    );
}


#[test]
/// Verifies a restarted node begins as a follower.
fn restarted_node_begins_as_follower() {
    let id =
        ServerId::new(1);

    let mut node =
        new_node(id);

    node.start_election();

    let storage =
        node.into_storage();

    let restarted =
        supports::node::restart_node(
            id,
            storage,
        );

    assert_eq!(
        restarted.role(),
        Role::Follower
    );
}


#[test]
/// Verifies a restarted node preserves its persistent term.
fn restarted_node_preserves_term() {
    let id =
        ServerId::new(1);

    let mut node =
        new_node(id);

    node.start_election();

    let term =
        node.current_term();

    let storage =
        node.into_storage();

    let restarted =
        supports::node::restart_node(
            id,
            storage,
        );

    assert_eq!(
        restarted.current_term(),
        term
    );
}


#[test]
/// Verifies a restarted node does not retain leader role.
fn restart_drops_leader_role() {
    let leader =
        ServerId::new(1);

    let follower =
        ServerId::new(2);

    let ids = [
        leader,
        follower,
    ];

    let mut node =
        new_node(leader);

    node.start_election();

    node.handle_request_vote_response(
        follower,
        RequestVoteResponse::granted(
            Term::new(1)
        ),
        &ids,
    );

    assert_eq!(
        node.role(),
        Role::Leader
    );

    let storage =
        node.into_storage();

    let restarted =
        supports::node::restart_node(
            leader,
            storage,
        );

    assert_eq!(
        restarted.role(),
        Role::Follower
    );
}


#[test]
/// Verifies an empty AppendEntries heartbeat does not append a log entry.
fn heartbeat_does_not_append_entry() {
    let follower =
        ServerId::new(2);

    let leader =
        ServerId::new(1);

    let mut node =
        new_node(follower);

    let request =
        AppendEntriesRequest::heartbeat(
            Term::new(1),
            leader,
            LogIndex::ZERO,
            Term::ZERO,
            LogIndex::ZERO,
        );

    assert!(
        node.handle_append_entries(
            request
        ).success
    );

    assert_eq!(
        node.last_log_index(),
        LogIndex::ZERO
    );
}


#[test]
/// Verifies a heartbeat can reset a nearly expired election timer.
fn heartbeat_resets_election_timer() {
    let follower =
        ServerId::new(2);

    let leader =
        ServerId::new(1);

    let mut node =
        new_node(follower);

    for _ in 0..4 {
        node.tick();
    }

    let request =
        AppendEntriesRequest::heartbeat(
            Term::ZERO,
            leader,
            LogIndex::ZERO,
            Term::ZERO,
            LogIndex::ZERO,
        );

    assert!(
        node.handle_append_entries(
            request
        ).success
    );

    for _ in 0..4 {
        node.tick();
    }

    assert_eq!(
        node.role(),
        Role::Follower
    );
}


#[test]
/// Verifies a granted vote is preserved in persistent storage.
fn granted_vote_survives_restart() {
    let node_id =
        ServerId::new(1);

    let voter =
        ServerId::new(2);

    let mut node =
        new_node(node_id);

    let request =
        RequestVoteRequest::new(
            Term::new(1),
            voter,
            LogIndex::ZERO,
            Term::ZERO,
        );

    let response =
        node.handle_request_vote(
            request
        );

    assert!(
        response.vote_granted
    );

    let storage =
        node.into_storage();

    let restarted =
        supports::node::restart_node(
            node_id,
            storage,
        );

    assert_eq!(
        restarted.voted_for(),
        Some(voter)
    );
}


/*
 * The following tests are intentionally ignored until the deterministic
 * fault executor, bidirectional partition model, and reusable invariant
 * checker are added to the test harness.
 *
 * They are kept here as actual test entry points so the final file contains
 * the complete matrix in one place.
 */


#[test]
#[ignore = "requires bidirectional partition support"]
/// Verifies a two-node partition blocks traffic in both directions.
fn bidirectional_partition_blocks_both_directions() {
    panic!(
        "requires bidirectional partition support"
    );
}


#[test]
#[ignore = "requires bidirectional partition support"]
/// Verifies a minority partition cannot elect a leader.
fn minority_partition_cannot_elect_leader() {
    panic!(
        "requires bidirectional partition support"
    );
}


#[test]
#[ignore = "requires bidirectional partition support"]
/// Verifies a majority partition can elect and commit.
fn majority_partition_can_continue_progress() {
    panic!(
        "requires bidirectional partition support"
    );
}


#[test]
#[ignore = "requires deterministic invariant checker"]
/// Verifies committed entries never change during a fault run.
fn committed_entries_never_change() {
    panic!(
        "requires deterministic invariant checker"
    );
}


#[test]
#[ignore = "requires deterministic invariant checker"]
/// Verifies commit index never moves backward in a fault run.
fn deterministic_commit_index_is_monotonic() {
    panic!(
        "requires deterministic invariant checker"
    );
}


#[test]
#[ignore = "requires deterministic invariant checker"]
/// Verifies last_applied never exceeds commit_index.
fn last_applied_never_exceeds_commit_index() {
    panic!(
        "requires deterministic invariant checker"
    );
}


#[test]
#[ignore = "requires deterministic invariant checker"]
/// Verifies match_index never moves backward in a fault run.
fn deterministic_match_index_is_monotonic() {
    panic!(
        "requires deterministic invariant checker"
    );
}


#[test]
#[ignore = "requires deterministic fault executor"]
/// Verifies a three-node deterministic fault run preserves safety.
fn three_node_deterministic_fault_run() {
    panic!(
        "requires deterministic fault executor"
    );
}


#[test]
#[ignore = "requires deterministic crash executor"]
/// Verifies a three-node deterministic crash run preserves safety.
fn three_node_deterministic_crash_run() {
    panic!(
        "requires deterministic crash executor"
    );
}


#[test]
#[ignore = "requires deterministic fault executor"]
/// Verifies a five-node deterministic fault run preserves safety.
fn five_node_deterministic_fault_run() {
    panic!(
        "requires deterministic fault executor"
    );
}


#[test]
#[ignore = "requires deterministic fault executor"]
/// Verifies a long deterministic run can be replayed from its seed.
fn long_deterministic_reproducible_run() {
    panic!(
        "requires deterministic fault executor"
    );
}


#[test]
#[ignore = "requires deterministic fault executor"]
/// Verifies the final umbrella Raft fault model.
fn deterministic_raft_fault_model() {
    panic!(
        "requires deterministic fault executor"
    );
}
