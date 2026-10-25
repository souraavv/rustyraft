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
    PersistentMetadata,
};

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
    let _ =
        fs::remove_dir_all(
            path,
        );
}

/// Verifies file storage survives restart and accepts new writes.
#[test]
fn file_storage_recovers_then_accepts_new_entry() {
    let path =
        test_storage_path(
            "restart-and-append",
        );

    let server_id =
        ServerId::new(2);

    /*
     * First server lifetime.
     */
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
                    Term::new(4),
                    Some(server_id),
                ),
            )
            .expect(
                "metadata should be saved",
            );

        storage
            .append_log_entry(
                LogEntry::new(
                    Term::new(4),
                    String::from("A"),
                ),
            )
            .expect(
                "entry A should be appended",
            );
    }

    /*
     * Simulate restart.
     *
     * The new storage instance must recover the old state.
     */
    {
        let mut storage =
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
            Term::new(4),
        );

        assert_eq!(
            metadata.voted_for(),
            Some(server_id),
        );

        assert_eq!(
            storage
                .last_log_index()
                .expect(
                    "last index should recover",
                ),
            LogIndex::new(1),
        );

        assert_eq!(
            storage
                .log_at(
                    LogIndex::new(1),
                )
                .expect(
                    "log entry should load",
                )
                .and_then(|entry| entry.command().cloned()),
            Some(
                String::from("A")
            ),
        );

        /*
         * Continue operating after restart.
         */
        storage
            .append_log_entry(
                LogEntry::new(
                    Term::new(5),
                    String::from("B"),
                ),
            )
            .expect(
                "entry B should be appended",
            );
    }

    /*
     * Restart again and verify both entries.
     */
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
                    "last index should recover",
                ),
            LogIndex::new(2),
        );

        assert_eq!(
            storage
                .log_at(
                    LogIndex::new(1),
                )
                .expect(
                    "entry A should load",
                )
                .and_then(|entry| entry.command().cloned()),
            Some(
                String::from("A")
            ),
        );

        assert_eq!(
            storage
                .log_at(
                    LogIndex::new(2),
                )
                .expect(
                    "entry B should load",
                )
                .and_then(|entry| entry.command().cloned()),
            Some(
                String::from("B")
            ),
        );

        assert_eq!(
            storage
                .term_at(
                    LogIndex::new(2),
                )
                .expect(
                    "term should load",
                ),
            Some(
                Term::new(5)
            ),
        );
    }

    cleanup_storage(
        &path,
    );
}

#[test]
fn file_storage_preserves_request_ids_across_restart() {
    let path = test_storage_path("client-request-id");

    {
        let mut storage = FileStorage::<String>::open(&path)
            .expect("storage should open");

        storage
            .append_log_entry(LogEntry::new(
                Term::new(1),
                "ordinary-command".to_string(),
            ))
            .expect("ordinary entry should append");

        storage
            .append_log_entry(LogEntry::new_client_command(
                Term::new(1),
                "request-1".to_string(),
                "SET x=1".to_string(),
            ))
            .expect("client entry should append");
    }

    {
        let storage = FileStorage::<String>::open(&path)
            .expect("storage should recover");

        let ordinary = storage
            .log_at(LogIndex::new(1))
            .expect("ordinary entry should load")
            .expect("ordinary entry should exist");

        assert_eq!(ordinary.request_id(), None);
        assert_eq!(ordinary.command().expect("log entry should contain a command"), "ordinary-command");

        let client_entry = storage
            .log_at(LogIndex::new(2))
            .expect("client entry should load")
            .expect("client entry should exist");

        assert_eq!(client_entry.request_id(), Some("request-1"));
        assert_eq!(client_entry.command().expect("log entry should contain a command"), "SET x=1");
    }

    cleanup_storage(&path);
}

#[test]
fn file_storage_preserves_noop_entries_across_restart() {
    let path = test_storage_path("noop-entry");

    {
        let mut storage = FileStorage::<String>::open(&path)
            .expect("storage should open");

        storage
            .append_log_entry(LogEntry::new(
                Term::new(1),
                "A".to_string(),
            ))
            .expect("command should append");

        storage
            .append_log_entry(LogEntry::new_noop(Term::new(2)))
            .expect("no-op should append");
    }

    {
        let storage = FileStorage::<String>::open(&path)
            .expect("storage should recover");

        assert_eq!(
            storage.last_log_index().expect("last index should load"),
            LogIndex::new(2),
        );

        let command = storage
            .log_at(LogIndex::new(1))
            .expect("command lookup should succeed")
            .expect("command should exist");
        assert_eq!(
            command.command().expect("first entry should be a command"),
            "A",
        );

        let noop = storage
            .log_at(LogIndex::new(2))
            .expect("no-op lookup should succeed")
            .expect("no-op should exist");
        assert!(noop.is_noop());
        assert!(noop.command().is_none());
        assert_eq!(noop.request_id(), None);
    }

    cleanup_storage(&path);
}

#[test]
fn file_storage_reads_legacy_v2_log_files() {
    let path = test_storage_path("legacy-v2");
    fs::create_dir_all(&path)
        .expect("storage directory should be created");

    // Version 2 encoded: magic, count, term, request-ID marker, command.
    let mut bytes = b"RUSTYRAFT-LOG-2".to_vec();
    bytes.extend_from_slice(&1_u64.to_le_bytes());
    bytes.extend_from_slice(&1_u64.to_le_bytes());
    bytes.push(0);
    bytes.extend_from_slice(&1_u64.to_le_bytes());
    bytes.extend_from_slice(b"A");
    fs::write(path.join("log"), bytes)
        .expect("legacy log should be written");

    let storage = FileStorage::<String>::open(&path)
        .expect("legacy log format should still be readable");

    assert_eq!(
        storage.last_log_index().expect("last index should load"),
        LogIndex::new(1),
    );
    assert_eq!(
        storage
            .log_at(LogIndex::new(1))
            .expect("legacy command lookup should succeed")
            .expect("legacy command should exist")
            .command()
            .expect("legacy entry should contain a command"),
        "A",
    );

    cleanup_storage(&path);
}
