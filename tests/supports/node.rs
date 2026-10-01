use rustyraft::raft::{
    LogEntry,
    RaftNode,
    ServerId,
    Term,
};

use rustyraft::raft::state_machine::{
    NoopStateMachine,
    StateMachine,
};

use rustyraft::raft::storage::{
    CachedStorage,
    DurableStorage,
    InMemoryCache,
    InMemoryStorage,
    PersistentMetadata,
};

pub type TestStorage =
    CachedStorage<
        InMemoryStorage<String>,
        String,
        InMemoryCache<String>,
    >;

pub fn new_test_storage() -> TestStorage {
    CachedStorage::new(
        InMemoryStorage::new(),
        InMemoryCache::new(),
    )
    .expect("in-memory storage should initialize")
}

pub fn new_node(
    id: ServerId,
) -> RaftNode<String, TestStorage> {
    RaftNode::new(
        id,
        new_test_storage(),
    )
}

pub fn new_node_with_state_machine<S>(
    id: ServerId,
    state_machine: S,
) -> RaftNode<String, TestStorage, S>
where
    S: StateMachine<String>,
{
    RaftNode::with_storage(
        id,
        new_test_storage(),
        state_machine,
    )
}

pub fn restart_node(
    id: ServerId,
    storage: TestStorage,
) -> RaftNode<String, TestStorage> {
    RaftNode::from_storage(
        id,
        storage,
        NoopStateMachine,
    )
}

pub fn new_node_with_log(
    id: ServerId,
    term: Term,
    entries: Vec<LogEntry<String>>,
) -> RaftNode<String, TestStorage> {
    let mut storage = InMemoryStorage::new();

    <InMemoryStorage<String> as DurableStorage<String>>::save_metadata(
        &mut storage,
        PersistentMetadata::new(
            term,
            None,
        ),
    )
    .expect("in-memory storage should save metadata");

    for entry in entries {
        <InMemoryStorage<String> as DurableStorage<String>>::append_log_entry(
            &mut storage,
            entry,
        )
        .expect("in-memory storage should append log entry");
    }

    let storage = CachedStorage::new(
        storage,
        InMemoryCache::new(),
    )
    .expect("in-memory storage should initialize");

    RaftNode::new(
        id,
        storage,
    )
}
