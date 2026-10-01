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
                .map(
                    |entry| entry.command
                ),
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
                .map(
                    |entry| entry.command
                ),
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
                .map(
                    |entry| entry.command
                ),
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