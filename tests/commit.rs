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