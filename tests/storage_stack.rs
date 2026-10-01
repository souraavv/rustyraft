use std::fs;
use std::path::PathBuf;
use std::time::{
    SystemTime,
    UNIX_EPOCH,
};

use rustyraft::raft::{
    LogEntry,
    LogIndex,
    RaftNode,
    Role,
    ServerId,
    Term,
};

use rustyraft::raft::rpc::{
    RequestVoteResponse,
};

use rustyraft::raft::state_machine::{
    NoopStateMachine,
};

use rustyraft::raft::storage::{
    CachedStorage,
    DurableStorage,
    FileStorage,
    InMemoryCache,
    InMemoryStorage,
    PersistentMetadata,
    RaftStorage,
};

type FileTestStorage =
    CachedStorage<
        FileStorage<String>,
        String,
        InMemoryCache<String>,
    >;

#[derive(Debug)]
struct StorageTestError;

struct FailingStorage<C> {
    durable: InMemoryStorage<C>,
}

impl<C> FailingStorage<C> {
    fn new(
        durable: InMemoryStorage<C>,
    ) -> Self {
        Self {
            durable,
        }
    }
}

impl<C> DurableStorage<C>
    for FailingStorage<C>
{
    type Error = StorageTestError;

    fn load_metadata(
        &self,
    ) -> Result<
        PersistentMetadata,
        Self::Error,
    > {
        <InMemoryStorage<C> as DurableStorage<C>>
            ::load_metadata(
                &self.durable,
            )
            .map_err(
                |_| StorageTestError
            )
    }

    fn log(
        &self,
    ) -> &rustyraft::raft::RaftLog<C> {
        <InMemoryStorage<C> as DurableStorage<C>>
            ::log(
                &self.durable,
            )
    }

    fn save_metadata(
        &mut self,
        _metadata: PersistentMetadata,
    ) -> Result<(), Self::Error> {
        Err(StorageTestError)
    }

    fn last_log_index(
        &self,
    ) -> Result<
        LogIndex,
        Self::Error,
    > {
        <InMemoryStorage<C> as DurableStorage<C>>
            ::last_log_index(
                &self.durable,
            )
            .map_err(
                |_| StorageTestError
            )
    }

    fn term_at(
        &self,
        index: LogIndex,
    ) -> Result<
        Option<Term>,
        Self::Error,
    > {
        <InMemoryStorage<C> as DurableStorage<C>>
            ::term_at(
                &self.durable,
                index,
            )
            .map_err(
                |_| StorageTestError
            )
    }

    fn log_at(
        &self,
        index: LogIndex,
    ) -> Result<
        Option<LogEntry<C>>,
        Self::Error,
    >
    where
        C: Clone,
    {
        <InMemoryStorage<C> as DurableStorage<C>>
            ::log_at(
                &self.durable,
                index,
            )
            .map_err(
                |_| StorageTestError
            )
    }

    fn entries_from(
        &self,
        start_index: LogIndex,
    ) -> Result<
        Vec<LogEntry<C>>,
        Self::Error,
    >
    where
        C: Clone,
    {
        <InMemoryStorage<C> as DurableStorage<C>>
            ::entries_from(
                &self.durable,
                start_index,
            )
            .map_err(
                |_| StorageTestError
            )
    }

    fn append_log_entry(
        &mut self,
        _entry: LogEntry<C>,
    ) -> Result<
        LogIndex,
        Self::Error,
    > {
        Err(StorageTestError)
    }

    fn truncate_log_from(
        &mut self,
        _index: LogIndex,
    ) -> Result<(), Self::Error> {
        Err(StorageTestError)
    }
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
    let _ =
        fs::remove_dir_all(
            path,
        );
}

/// Verifies CachedStorage hydrates and persists through FileStorage.
#[test]
fn cached_file_storage_survives_restart() {
    let path =
        test_storage_path(
            "cached-file-restart",
        );

    let server_id =
        ServerId::new(1);

    {
        let file_storage =
            FileStorage::<String>::open(
                &path,
            )
            .expect(
                "file storage should open",
            );

        let mut storage =
            CachedStorage::new(
                file_storage,
                InMemoryCache::new(),
            )
            .expect(
                "cached storage should initialize",
            );

        storage
            .save_metadata(
                PersistentMetadata::new(
                    Term::new(3),
                    Some(server_id),
                ),
            )
            .expect(
                "metadata should save",
            );

        storage
            .append_log_entry(
                LogEntry::new(
                    Term::new(3),
                    String::from("A"),
                ),
            )
            .expect(
                "entry A should append",
            );
    }

    {
        let file_storage =
            FileStorage::<String>::open(
                &path,
            )
            .expect(
                "file storage should reopen",
            );

        let mut storage =
            CachedStorage::new(
                file_storage,
                InMemoryCache::new(),
            )
            .expect(
                "cached storage should recover",
            );

        let metadata =
            storage
                .load_metadata()
                .expect(
                    "metadata should recover",
                );

        assert_eq!(
            metadata.current_term(),
            Term::new(3),
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
                    "entry should load",
                )
                .map(
                    |entry| entry.command
                ),
            Some(
                String::from("A")
            ),
        );

        storage
            .append_log_entry(
                LogEntry::new(
                    Term::new(4),
                    String::from("B"),
                ),
            )
            .expect(
                "entry B should append",
            );
    }

    {
        let file_storage =
            FileStorage::<String>::open(
                &path,
            )
            .expect(
                "file storage should reopen",
            );

        let storage =
            CachedStorage::new(
                file_storage,
                InMemoryCache::new(),
            )
            .expect(
                "cached storage should initialize",
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
    }

    cleanup_storage(
        &path,
    );
}

/// Verifies a failed durable write does not update the cache.
#[test]
fn cached_storage_does_not_update_cache_on_durable_failure() {
    let mut durable =
        InMemoryStorage::<String>::new();

    <InMemoryStorage<String> as DurableStorage<String>>
        ::save_metadata(
            &mut durable,
            PersistentMetadata::new(
                Term::new(1),
                None,
            ),
        )
        .expect(
            "metadata should initialize",
        );

    <InMemoryStorage<String> as DurableStorage<String>>
        ::append_log_entry(
            &mut durable,
            LogEntry::new(
                Term::new(1),
                String::from("A"),
            ),
        )
        .expect(
            "entry A should initialize",
        );

    let failing_storage =
        FailingStorage::new(
            durable,
        );

    let mut storage =
        CachedStorage::new(
            failing_storage,
            InMemoryCache::new(),
        )
        .expect(
            "cached storage should initialize",
        );

    let result =
        storage.append_log_entry(
            LogEntry::new(
                Term::new(2),
                String::from("B"),
            ),
        );

    assert!(
        result.is_err()
    );

    assert_eq!(
        storage
            .last_log_index()
            .expect(
                "existing log should remain",
            ),
        LogIndex::new(1),
    );

    assert_eq!(
        storage
            .log_at(
                LogIndex::new(1),
            )
            .expect(
                "existing entry should remain",
            )
            .map(
                |entry| entry.command
            ),
        Some(
            String::from("A")
        ),
    );

    assert!(
        storage
            .log_at(
                LogIndex::new(2),
            )
            .expect(
                "new entry lookup should succeed",
            )
            .is_none()
    );
}

/// Verifies RaftNode persists through the complete file storage stack.
#[test]
fn raft_node_survives_file_storage_restart() {
    let path =
        test_storage_path(
            "raft-node-file-restart",
        );

    let server_id =
        ServerId::new(1);

    let follower_id =
        ServerId::new(2);

    let first_storage =
        FileStorage::<String>::open(
            &path,
        )
        .expect(
            "file storage should open",
        );

    let first_storage =
        CachedStorage::new(
            first_storage,
            InMemoryCache::new(),
        )
        .expect(
            "cached storage should initialize",
        );

    let mut node =
        RaftNode::with_storage(
            server_id,
            first_storage,
            NoopStateMachine,
        );

    node.start_election();

    node.handle_request_vote_response(
        follower_id,
        RequestVoteResponse::granted(
            Term::new(1),
        ),
        &[
            server_id,
            follower_id,
        ],
    );

    assert_eq!(
        node.role(),
        Role::Leader,
    );

    assert_eq!(
        node.append_entry(
            String::from("A"),
        ),
        Some(
            LogIndex::new(1)
        ),
    );

    assert_eq!(
        node.append_entry(
            String::from("B"),
        ),
        Some(
            LogIndex::new(2)
        ),
    );

    let storage =
        node.into_storage();

    drop(
        storage
    );

    let second_storage =
        FileStorage::<String>::open(
            &path,
        )
        .expect(
            "file storage should reopen",
        );

    let second_storage =
        CachedStorage::new(
            second_storage,
            InMemoryCache::new(),
        )
        .expect(
            "cached storage should recover",
        );

    let restarted =
        RaftNode::from_storage(
            server_id,
            second_storage,
            NoopStateMachine,
        );

    assert_eq!(
        restarted.role(),
        Role::Follower,
    );

    assert_eq!(
        restarted.current_term(),
        Term::new(1),
    );

    assert_eq!(
        restarted.voted_for(),
        Some(server_id),
    );

    assert_eq!(
        restarted.log().last_index(),
        LogIndex::new(2),
    );

    assert_eq!(
        restarted
            .log_at(
                LogIndex::new(1),
            )
            .map(
                |entry| entry.command.clone()
            ),
        Some(
            String::from("A")
        ),
    );

    assert_eq!(
        restarted
            .log_at(
                LogIndex::new(2),
            )
            .map(
                |entry| entry.command.clone()
            ),
        Some(
            String::from("B")
        ),
    );

    cleanup_storage(
        &path,
    );
}