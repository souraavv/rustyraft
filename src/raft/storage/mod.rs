//! Durable storage abstraction for the Raft Server
//!
//! The Raft protocol separates persistent state from volatile state
//!
//! Storage owns the persistent state and defines the durability boundary
//!
//! Persistent state
//!  - current_term
//!  - voted for
//!  - logs
//!
//! The storage implementation owns both the durable representation and, when
//! enabled, in-memory cache
//!
//! RaftNode should only interact with the storage API and should not
//! coordinate cache update itself.

use crate::raft::log::RaftLog;

use crate::raft::state::{
    LogIndex,
    ServerId,
    Term,
};

use crate::raft::LogEntry;


/// Persistent Raft Metadata
///
/// The metadata contains the state that must survive a server restart;
/// the current term and the candidate this server voted for in that term
///
/// We keep the field together because they form one logical persistent
/// state update.
#[derive(Debug, PartialEq, Eq)]
pub struct PersistentMetadata {
    current_term: Term,
    voted_for: Option<ServerId>,
}

impl PersistentMetadata {

    pub fn new(
        current_term: Term,
        voted_for: Option<ServerId>,
    ) -> Self {
        Self {
            current_term,
            voted_for,
        }
    }

    // ---------- getters ------------
    /// Returns the current term.
    pub fn current_term(&self) -> Term {
        self.current_term
    }

    /// Returns the server voted for in the current term.
    pub fn voted_for(&self) -> Option<ServerId> {
        self.voted_for
    }
}

/// Persistent storage used by Raft Server
///
/// A successful mutating operation means that the storage implementation
/// has persisted the change according to the durability contract
///
/// The trait doesn't expose a concrete file format or serialization mechanism.
/// Those belong to the storage implementation.
pub trait DurableStorage<C> {

    type Error;

    /// Loads the persistent term and vote information.
    fn load_metadata(&self) -> Result<PersistentMetadata, Self::Error>;

    /// Returns the in-memory log owned by the storage implementation.
    fn log(&self) -> &RaftLog<C>;

    /// Persists the current term and vote information together.
    fn save_metadata(
        &mut self,
        metadata: PersistentMetadata,
    ) -> Result<(), Self::Error>;

    fn last_log_index(&self) -> Result<LogIndex, Self::Error>;

    fn term_at(
        &self,
        index: LogIndex,
    ) -> Result<Option<Term>, Self::Error>;

    fn log_at(
        &self,
        index: LogIndex,
    ) -> Result<Option<LogEntry<C>>, Self::Error>
    where
        C: Clone;

    /// Return all entries starting at the given index.
    fn entries_from(
        &self,
        start_index: LogIndex,
    ) -> Result<Vec<LogEntry<C>>, Self::Error>
    where
        C: Clone;

    fn append_log_entry(
        &mut self,
        entry: LogEntry<C>,
    ) -> Result<LogIndex, Self::Error>;

    /// Remove logs from the given index onward.
    fn truncate_log_from(
        &mut self,
        index: LogIndex,
    ) -> Result<(), Self::Error>;
}

/// Optional Cache used by storage implementation
///
/// The cache is an optimization, not the source of truth
///
/// A cache implementation may return `None` when it has no cached value
///
/// The storage layer then reads from the durable backend
///
/// Log cache operation requires C: Clone because the durable log and
/// cache intentionally own separate copies of the commands.
pub trait StorageCache<C> {

    /// Returns cached metadata, when available.
    fn metadata(&self) -> Option<&PersistentMetadata>;

    /// Returns the cached log, when available.
    fn log(&self) -> Option<&RaftLog<C>>;

    /// Replaces the cached metadata.
    fn store_metadata(
        &mut self,
        metadata: &PersistentMetadata,
    );

    /// Replaces the cached log with a copy of the durable log.
    fn store_log(
        &mut self,
        log: &RaftLog<C>,
    )
    where
        C: Clone;

    /// Removes the cached log.
    fn invalidate_log(&mut self);
}

/// Storage boundary that combines a durable backend with an optional
/// cache
///
/// The storage layer owns both sides of persistent operation.
///
/// RaftNode never writes the cache or durable backend.
///
/// A successful write means the durable backend has succeeded before
/// the cache is updated to reflect that durable state.
pub trait RaftStorage<C> {

    type Error;

    fn load_metadata(
        &self,
    ) -> Result<PersistentMetadata, Self::Error>;

    // -------- persistent values getters -----
    fn current_term(&self) -> Result<Term, Self::Error>;

    fn voted_for(&self) -> Result<Option<ServerId>, Self::Error>;

    fn log(&self) -> &RaftLog<C>;

    // --------- helpers on log ----
    fn last_log_index(&self) -> Result<LogIndex, Self::Error>;

    fn term_at(
        &self,
        index: LogIndex,
    ) -> Result<Option<Term>, Self::Error>;

    fn log_at(
        &self,
        index: LogIndex,
    ) -> Result<Option<LogEntry<C>>, Self::Error>
    where
        C: Clone;

    fn append_log_entry(
        &mut self,
        entry: LogEntry<C>,
    ) -> Result<LogIndex, Self::Error>;

    fn truncate_log_from(
        &mut self,
        index: LogIndex,
    ) -> Result<(), Self::Error>;

    fn entries_from(
        &self,
        start_index: LogIndex,
    ) -> Result<Vec<LogEntry<C>>, Self::Error>
    where
        C: Clone;

    // ------ metadata persistence ------
    fn save_metadata(
        &mut self,
        metadata: PersistentMetadata,
    ) -> Result<(), Self::Error>;
}

mod cache;
mod file;
mod format;
mod memory;

pub use cache::{
    CachedStorage,
    InMemoryCache,
    NoCache,
};

pub use file::FileStorage;
pub use memory::InMemoryStorage;
