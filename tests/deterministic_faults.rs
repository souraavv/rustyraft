mod supports;

use rustyraft::raft::ServerId;

use supports::fault::{
    DeterministicFaultGenerator,
};


#[test]
/// Verifies the same seed produces the same fault sequence.
fn same_seed_produces_same_event_sequence() {
    let server_ids = [
        ServerId::new(1),
        ServerId::new(2),
        ServerId::new(3),
    ];

    let mut first =
        DeterministicFaultGenerator::new(
            12345,
            &server_ids,
        );

    let mut second =
        DeterministicFaultGenerator::new(
            12345,
            &server_ids,
        );

    let first_sequence =
        first.generate(100);

    let second_sequence =
        second.generate(100);

    assert_eq!(
        first_sequence,
        second_sequence,
    );
}


#[test]
/// Verifies different seeds can produce different fault sequences.
fn different_seeds_produce_different_event_sequences() {
    let server_ids = [
        ServerId::new(1),
        ServerId::new(2),
        ServerId::new(3),
    ];

    let mut first =
        DeterministicFaultGenerator::new(
            12345,
            &server_ids,
        );

    let mut second =
        DeterministicFaultGenerator::new(
            67890,
            &server_ids,
        );

    let first_sequence =
        first.generate(100);

    let second_sequence =
        second.generate(100);

    assert_ne!(
        first_sequence,
        second_sequence,
    );
}