use std::fs;
use std::path::PathBuf;
use std::time::{
    SystemTime,
    UNIX_EPOCH,
};

use rustyraft::raft::{
    LogEntry,
    LogIndex,
    ServerId,
    Term,
};

use rustyraft::raft::storage::{
    DurableStorage,
    FileStorage,
    InMemoryStorage,
    PersistentMetadata,
};


/// Verifies that storage preserves persistent metadata and log entries.
#[test]
fn storage_preserves_metadata_and_log() {
    let mut storage = InMemoryStorage::<String>::new();

    storage
        .save_metadata(PersistentMetadata::new(
            Term::new(3),
            Some(ServerId::new(2)),
        ))
        .unwrap();

    let first_index = storage
        .append_log_entry(
            LogEntry::new(
                Term::new(2),
                String::from("A"),
            ),
        )
        .unwrap();

    let second_index = storage
        .append_log_entry(
            LogEntry::new(
                Term::new(3),
                String::from("B"),
            ),
        )
        .unwrap();

    let metadata = storage.load_metadata().unwrap();

    assert_eq!(
        metadata.current_term(),
        Term::new(3),
    );
    assert_eq!(
        metadata.voted_for(),
        Some(ServerId::new(2)),
    );

    assert_eq!(first_index.value(), 1);
    assert_eq!(second_index.value(), 2);

    assert_eq!(
        storage.last_log_index().unwrap().value(),
        2,
    );

    assert_eq!(
        storage.term_at(first_index).unwrap(),
        Some(Term::new(2)),
    );

    assert_eq!(
        storage
            .log_at(second_index)
            .unwrap()
            .map(|entry| entry.command),
        Some(String::from("B")),
    );
}

/// Verifies that a new storage instance starts empty.
#[test]
fn storage_starts_empty() {
    let storage = InMemoryStorage::<String>::new();

    let metadata = storage.load_metadata().unwrap();

    assert_eq!(
        metadata.current_term(),
        Term::ZERO,
    );
    assert_eq!(
        metadata.voted_for(),
        None,
    );
    assert_eq!(
        storage.last_log_index().unwrap(),
        LogIndex::ZERO,
    );
    assert!(
        storage
            .term_at(LogIndex::new(1))
            .unwrap()
            .is_none()
    );
    assert!(
        storage
            .log_at(LogIndex::new(1))
            .unwrap()
            .is_none()
    );
}


/// Verifies that saving metadata replaces the previous metadata.
#[test]
fn storage_replaces_persistent_metadata() {
    let mut storage = InMemoryStorage::<String>::new();

    storage
        .save_metadata(PersistentMetadata::new(
            Term::new(4),
            Some(ServerId::new(7)),
        ))
        .unwrap();

    storage
        .save_metadata(PersistentMetadata::new(
            Term::new(5),
            Some(ServerId::new(3)),
        ))
        .unwrap();

    let metadata = storage.load_metadata().unwrap();

    assert_eq!(
        metadata.current_term(),
        Term::new(5),
    );
    assert_eq!(
        metadata.voted_for(),
        Some(ServerId::new(3)),
    );
}

/// Verifies that entries_from returns the requested log suffix.
#[test]
fn storage_returns_entries_from_index() {
    let mut storage = InMemoryStorage::<String>::new();

    storage
        .append_log_entry(
            LogEntry::new(
                Term::new(1),
                String::from("A"),
            ),
        )
        .unwrap();

    storage
        .append_log_entry(
            LogEntry::new(
                Term::new(1),
                String::from("B"),
            ),
        )
        .unwrap();

    storage
        .append_log_entry(
            LogEntry::new(
                Term::new(2),
                String::from("C"),
            ),
        )
        .unwrap();

    let entries = storage
        .entries_from(LogIndex::new(2))
        .unwrap();

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].term, Term::new(1));
    assert_eq!(
        entries[0].command,
        String::from("B"),
    );
    assert_eq!(entries[1].term, Term::new(2));
    assert_eq!(
        entries[1].command,
        String::from("C"),
    );
}

/// Verifies that reading beyond the end of the log returns no entries.
#[test]
fn storage_returns_empty_suffix_beyond_last_index() {
    let mut storage = InMemoryStorage::<String>::new();

    storage
        .append_log_entry(
            LogEntry::new(
                Term::new(1),
                String::from("A"),
            ),
        )
        .unwrap();

    let entries = storage
        .entries_from(LogIndex::new(2))
        .unwrap();

    assert!(entries.is_empty());
}

/// Verifies that truncation removes the selected entry and its suffix.
#[test]
fn storage_truncates_log_from_index() {
    let mut storage = InMemoryStorage::<String>::new();

    storage
        .append_log_entry(
            LogEntry::new(
                Term::new(1),
                String::from("A"),
            ),
        )
        .unwrap();

    storage
        .append_log_entry(
            LogEntry::new(
                Term::new(1),
                String::from("B"),
            ),
        )
        .unwrap();

    storage
        .append_log_entry(
            LogEntry::new(
                Term::new(2),
                String::from("C"),
            ),
        )
        .unwrap();

    storage
        .truncate_log_from(LogIndex::new(2))
        .unwrap();

    assert_eq!(
        storage.last_log_index().unwrap(),
        LogIndex::new(1),
    );

    assert_eq!(
        storage.term_at(LogIndex::new(1)).unwrap(),
        Some(Term::new(1)),
    );

    assert_eq!(
        storage.term_at(LogIndex::new(2)).unwrap(),
        None,
    );
    assert_eq!(
        storage.term_at(LogIndex::new(3)).unwrap(),
        None,
    );
}

/// Verifies that a new entry can be appended after truncation.
#[test]
fn storage_appends_after_log_truncation() {
    let mut storage = InMemoryStorage::<String>::new();

    storage
        .append_log_entry(
            LogEntry::new(
                Term::new(1),
                String::from("A"),
            ),
        )
        .unwrap();

    storage
        .append_log_entry(
            LogEntry::new(
                Term::new(1),
                String::from("B"),
            ),
        )
        .unwrap();

    storage
        .truncate_log_from(LogIndex::new(2))
        .unwrap();

    let new_index = storage
        .append_log_entry(
            LogEntry::new(
                Term::new(3),
                String::from("C"),
            ),
        )
        .unwrap();

    assert_eq!(
        new_index,
        LogIndex::new(2),
    );

    assert_eq!(
        storage.term_at(LogIndex::new(2)).unwrap(),
        Some(Term::new(3)),
    );

    assert_eq!(
        storage
            .log_at(LogIndex::new(2))
            .unwrap()
            .map(|entry| entry.command),
        Some(String::from("C")),
    );
}

fn test_storage_path(
    name: &str,
) -> PathBuf {
    let timestamp =
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect(
                "system time should be valid",
            )
            .as_nanos();

    std::env::temp_dir()
        .join(
            format!(
                "rustyraft-{}-{}",
                name,
                timestamp,
            )
        )
}

fn cleanup_storage(
    path: &PathBuf,
) {
    let _ = fs::remove_dir_all(path);
}

/// Verifies file storage persists and recovers Raft state.
#[test]
fn file_storage_persists_and_recovers_state() {
    let path =
        test_storage_path(
            "persists-state",
        );

    let server_id =
        ServerId::new(1);

    {
        let mut storage =
            FileStorage::<String>::open(
                &path,
            )
            .expect(
                "file storage should open",
            );

        storage
            .save_metadata(
                PersistentMetadata::new(
                    Term::new(1),
                    Some(server_id),
                ),
            )
            .expect(
                "metadata should be saved",
            );

        storage
            .append_log_entry(
                LogEntry::new(
                    Term::new(1),
                    "A".to_string(),
                ),
            )
            .expect(
                "entry A should be appended",
            );

        storage
            .append_log_entry(
                LogEntry::new(
                    Term::new(1),
                    "B".to_string(),
                ),
            )
            .expect(
                "entry B should be appended",
            );

        let metadata =
            storage
                .load_metadata()
                .expect(
                    "metadata should load",
                );

        assert_eq!(
            metadata.current_term(),
            Term::new(1),
        );

        assert_eq!(
            metadata.voted_for(),
            Some(server_id),
        );

        assert_eq!(
            storage
                .last_log_index()
                .expect(
                    "last log index should load",
                ),
            LogIndex::new(2),
        );
    }

    {
        let storage =
            FileStorage::<String>::open(
                &path,
            )
            .expect(
                "file storage should reopen",
            );

        let metadata =
            storage
                .load_metadata()
                .expect(
                    "metadata should recover",
                );

        assert_eq!(
            metadata.current_term(),
            Term::new(1),
        );

        assert_eq!(
            metadata.voted_for(),
            Some(server_id),
        );

        assert_eq!(
            storage
                .last_log_index()
                .expect(
                    "last log index should recover",
                ),
            LogIndex::new(2),
        );

        assert_eq!(
            storage
                .term_at(
                    LogIndex::new(1),
                )
                .expect(
                    "term at index 1 should load",
                ),
            Some(Term::new(1)),
        );

        assert_eq!(
            storage
                .term_at(
                    LogIndex::new(2),
                )
                .expect(
                    "term at index 2 should load",
                ),
            Some(Term::new(1)),
        );

        let entry =
            storage
                .log_at(
                    LogIndex::new(1),
                )
                .expect(
                    "entry should load",
                )
                .expect(
                    "entry 1 should exist",
                );

        assert_eq!(
            entry.command,
            "A".to_string(),
        );

        let entry =
            storage
                .log_at(
                    LogIndex::new(2),
                )
                .expect(
                    "entry should load",
                )
                .expect(
                    "entry 2 should exist",
                );

        assert_eq!(
            entry.command,
            "B".to_string(),
        );
    }

    cleanup_storage(
        &path,
    );
}

/// Verifies truncation survives reopening file storage.
#[test]
fn file_storage_persists_log_truncation() {
    let path =
        test_storage_path(
            "persists-truncation",
        );

    {
        let mut storage =
            FileStorage::<String>::open(
                &path,
            )
            .expect(
                "file storage should open",
            );

        storage
            .append_log_entry(
                LogEntry::new(
                    Term::new(1),
                    "A".to_string(),
                ),
            )
            .expect(
                "entry A should be appended",
            );

        storage
            .append_log_entry(
                LogEntry::new(
                    Term::new(1),
                    "B".to_string(),
                ),
            )
            .expect(
                "entry B should be appended",
            );

        storage
            .append_log_entry(
                LogEntry::new(
                    Term::new(2),
                    "C".to_string(),
                ),
            )
            .expect(
                "entry C should be appended",
            );

        storage
            .truncate_log_from(
                LogIndex::new(3),
            )
            .expect(
                "log should be truncated",
            );

        assert_eq!(
            storage
                .last_log_index()
                .expect(
                    "last log index should load",
                ),
            LogIndex::new(2),
        );
    }

    {
        let storage =
            FileStorage::<String>::open(
                &path,
            )
            .expect(
                "file storage should reopen",
            );

        assert_eq!(
            storage
                .last_log_index()
                .expect(
                    "last log index should recover",
                ),
            LogIndex::new(2),
        );

        assert!(
            storage
                .log_at(
                    LogIndex::new(3),
                )
                .expect(
                    "entry lookup should succeed",
                )
                .is_none()
        );
    }

    cleanup_storage(
        &path,
    );
}