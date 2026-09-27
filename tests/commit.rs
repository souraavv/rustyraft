use rustyraft::raft::commit::find_commit_index;
use rustyraft::raft::{
    LogIndex,
    Term,
};

fn term_at(
    entries: &[(u64, u64)],
    index: LogIndex,
) -> Option<Term> {
    entries
        .iter()
        .find(|(entry_index, _)| {
            *entry_index == index.value()
        })
        .map(|(_, term)| Term::new(*term))
}

#[test]
fn empty_match_indexes_do_not_advance_commit() {
    let commit = find_commit_index(
        LogIndex::ZERO,
        Term::new(1),
        &[],
        |_| Some(Term::new(1)),
    );

    assert_eq!(commit, LogIndex::ZERO);
}

#[test]
fn majority_replication_commits_entry() {
    let match_indexes = vec![
        LogIndex::new(5),
        LogIndex::new(5),
        LogIndex::new(5),
        LogIndex::new(2),
        LogIndex::new(1),
    ];

    let entries = vec![(5, 3)];

    let commit = find_commit_index(
        LogIndex::new(2),
        Term::new(3),
        &match_indexes,
        |index| term_at(&entries, index),
    );

    assert_eq!(commit, LogIndex::new(5));
}

#[test]
fn minority_replication_does_not_commit_entry() {
    let match_indexes = vec![
        LogIndex::new(5),
        LogIndex::new(5),
        LogIndex::new(2),
        LogIndex::new(1),
        LogIndex::new(1),
    ];

    let entries = vec![(5, 3)];

    let commit = find_commit_index(
        LogIndex::new(2),
        Term::new(3),
        &match_indexes,
        |index| term_at(&entries, index),
    );

    assert_eq!(commit, LogIndex::new(2));
}

#[test]
fn older_term_entry_is_not_committed_directly() {
    let match_indexes = vec![
        LogIndex::new(5),
        LogIndex::new(5),
        LogIndex::new(5),
    ];

    let entries = vec![
        (4, 2),
        (5, 3),
    ];

    let commit = find_commit_index(
        LogIndex::new(2),
        Term::new(3),
        &match_indexes,
        |index| term_at(&entries, index),
    );

    assert_eq!(commit, LogIndex::new(5));
}

#[test]
fn commit_index_never_moves_backwards() {
    let match_indexes = vec![
        LogIndex::new(3),
        LogIndex::new(3),
        LogIndex::new(3),
    ];

    let entries = vec![(3, 3)];

    let commit = find_commit_index(
        LogIndex::new(5),
        Term::new(3),
        &match_indexes,
        |index| term_at(&entries, index),
    );

    assert_eq!(commit, LogIndex::new(5));
}

#[test]
fn highest_majority_replicated_current_term_entry_is_committed() {
    let match_indexes = vec![
        LogIndex::new(7),
        LogIndex::new(7),
        LogIndex::new(6),
        LogIndex::new(5),
        LogIndex::new(4),
    ];

    let entries = vec![
        (4, 2),
        (5, 3),
        (6, 3),
        (7, 3),
    ];

    let commit = find_commit_index(
        LogIndex::new(4),
        Term::new(3),
        &match_indexes,
        |index| term_at(&entries, index),
    );

    assert_eq!(commit, LogIndex::new(6));
}

#[test]
fn commit_advances_when_current_term_entry_reaches_majority() {
    let current_term = Term::new(1);

    // Five-server cluster:
    //
    // Server 1: replicated through index 3
    // Server 2: replicated through index 3
    // Server 3: replicated through index 3
    // Server 4: replicated through index 2
    // Server 5: replicated through index 1
    //
    // Index 3 is therefore stored on a majority (3/5).
    let match_indexes = vec![
        LogIndex::new(3),
        LogIndex::new(3),
        LogIndex::new(3),
        LogIndex::new(2),
        LogIndex::new(1),
    ];

    let commit_index = find_commit_index(
        LogIndex::ZERO,
        current_term,
        &match_indexes,
        |index| {
            if index == LogIndex::new(3) {
                Some(current_term)
            } else {
                None
            }
        },
    );

    assert_eq!(
        commit_index,
        LogIndex::new(3)
    );
}

#[test]
fn commit_does_not_advance_for_entry_from_older_term() {
    let current_term = Term::new(2);

    let match_indexes = vec![
        LogIndex::new(3),
        LogIndex::new(3),
        LogIndex::new(3),
        LogIndex::new(2),
        LogIndex::new(1),
    ];

    let commit_index = find_commit_index(
        LogIndex::ZERO,
        current_term,
        &match_indexes,
        |index| {
            if index == LogIndex::new(3) {
                Some(Term::new(1))
            } else {
                None
            }
        },
    );

    assert_eq!(
        commit_index,
        LogIndex::ZERO
    );
}

#[test]
fn commit_advances_through_older_term_entry() {
    let current_term = Term::new(2);

    // Five-server cluster.
    //
    // Index 2 belongs to an older term.
    // Index 3 belongs to the current term.
    //
    // Index 3 is replicated on a majority, so the leader can
    // advance commit_index to 3. That also commits index 2.
    let match_indexes = vec![
        LogIndex::new(3),
        LogIndex::new(3),
        LogIndex::new(3),
        LogIndex::new(1),
        LogIndex::new(1),
    ];

    let commit_index = find_commit_index(
        LogIndex::ZERO,
        current_term,
        &match_indexes,
        |index| match index {
            index if index == LogIndex::new(2) => {
                Some(Term::new(1))
            }
            index if index == LogIndex::new(3) => {
                Some(current_term)
            }
            _ => None,
        },
    );

    assert_eq!(
        commit_index,
        LogIndex::new(3)
    );
}

#[test]
fn commit_index_never_moves_backward() {
    let current_term = Term::new(2);

    let current_commit = LogIndex::new(3);

    let match_indexes = vec![
        LogIndex::new(3),
        LogIndex::new(3),
        LogIndex::new(2),
        LogIndex::new(1),
        LogIndex::new(1),
    ];

    let commit_index = find_commit_index(
        current_commit,
        current_term,
        &match_indexes,
        |index| {
            if index == LogIndex::new(3) {
                Some(current_term)
            } else {
                None
            }
        },
    );

    assert_eq!(
        commit_index,
        LogIndex::new(3)
    );
}

