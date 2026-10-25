
use std::marker::PhantomData;

use crate::raft::log::RaftLog;
use crate::raft::state::{
    LogIndex,
    ServerId,
    Term,
};

use crate::raft::{LogEntry, LogEntryKind};

use super::{
    DurableStorage,
    PersistentMetadata,
    RaftStorage,
    StorageCache,
};

/// Cache implementation that deliberately bypasses caching.
///
/// This is useful when the durable backend already has an efficient
/// in-memory representation or when caching is not needed.
#[derive(Debug, Default)]
pub struct NoCache;

impl<C> StorageCache<C> for NoCache {

    fn metadata(&self) -> Option<&PersistentMetadata> {
        None
    }

    fn log(&self) -> Option<&RaftLog<C>> {
        None
    }

    fn store_metadata(
        &mut self,
        _metadata: &PersistentMetadata,
    ) {
    }

    fn store_log(
        &mut self,
        _log: &RaftLog<C>,
    )
    where
        C: Clone,
    {
    }

    fn invalidate_log(&mut self) {
    }
}

/// Simple in-memory cache.
///
/// This cache keeps a complete copy of the persistent metadata and log.
/// It is intentionally simple for the first cache implementation.
///
/// Later we can replace it with a bounded cache, segmented cache, or a
/// byte-oriented cache without changing the Raft protocol.
///
#[derive(Debug)]
pub struct InMemoryCache<C> {
    metadata: Option<PersistentMetadata>,
    log: Option<RaftLog<C>>,
}

impl<C> InMemoryCache<C> {
    /// Creates an empty cache.
    pub fn new() -> Self {
        Self {
            metadata: None,
            log: None,
        }
    }
}

impl<C> Default for InMemoryCache<C> {
    fn default() -> Self {
        Self::new()
    }
}

/// With our in memory cache we are having storage cache
/// which TBH doesn't even makes sense..
///
/// InMemoryCache is a assumed to be 'the durable storage'
/// later you will see that DurableStorage trait is also implemented by
/// InMemoryCache
impl<C> StorageCache<C> for InMemoryCache<C> {

    fn metadata(&self) -> Option<&PersistentMetadata> {
        self.metadata.as_ref()
    }

    fn log(&self) -> Option<&RaftLog<C>> {
        self.log.as_ref()
    }

    fn store_metadata(
        &mut self,
        metadata: &PersistentMetadata,
    ) {
        self.metadata = Some(
            PersistentMetadata::new(
                metadata.current_term(),
                metadata.voted_for(),
            ),
        );
    }

    fn store_log(
        &mut self,
        log: &RaftLog<C>,
    )
    where
        C: Clone,
    {
        let mut cached_log = RaftLog::new();

        for log_entry in log.iter() {
            // Preserve command metadata and protocol no-op entries.
            let cached_entry = match &log_entry.kind {
                LogEntryKind::Command {
                    command,
                    request_id: Some(request_id),
                } => LogEntry::new_client_command(
                    log_entry.term,
                    request_id.clone(),
                    command.clone(),
                ),
                LogEntryKind::Command {
                    command,
                    request_id: None,
                } => LogEntry::new(
                    log_entry.term,
                    command.clone(),
                ),
                LogEntryKind::Noop => LogEntry::new_noop(log_entry.term),
            };

            cached_log.append(cached_entry);
        }

        self.log = Some(cached_log);
    }

    fn invalidate_log(&mut self) {
        self.log = None;
    }
}

/// Storage boundary that combines a durable backend with an optional
/// cache
///
/// The storage layer owns both sides of persistent operation.
///
/// RaftNode never writes the cache or durable backend.
///
/// A write is considered successful only after the durable backend
/// succeeds. The cache is then updated to reflect that durable state.
///
/// PhantomData is used - struct is logically parameterized by C, even
/// though it does not actually store a C.
#[derive(Debug)]
pub struct CachedStorage<D, C, K> {
    durable: D,
    cache: K,
    // zero-sized marker - stores nothing at runtime
    // Phantom data acts like they store T (PhantomData<T>)
    // fn() -> C is trick saying that a function which returns C, but
    // still we are not holding any C here .. the variable is of type
    // function pointer (e.g., let f: fn() -> String = make_string)
    // PhantomData<C>  = pretend the struct actually owns a C
    // where as Phantom<fn() -> C> means struct type dependent on C, but
    // it doesn't own a C
    _marker: PhantomData<fn() -> C>,
}

impl<D, C, K> CachedStorage<D, C, K>
where
    D: DurableStorage<C>,
    K: StorageCache<C>,
    C: Clone,
{
    pub fn new(
        durable: D,
        mut cache: K,
    ) -> Result<Self, D::Error> {
        let metadata = durable.load_metadata()?;

        // cache is mutated here thus mutable reference
        cache.store_metadata(&metadata);
        cache.store_log(durable.log());

        Ok(Self {
            durable,
            cache,
            _marker: PhantomData,
        })
    }
}

// implementing a trait of RaftStorage (which a RaftNode expects) - API to
// be exposed by this Cached Storage
// Cached Storage is designed to pick any durable storage (D) which supports
// caching explicitly (K)
// the underlying storage also is generic of the type of Logs we want to
// store which is C
// Same we may have errors coming from the Durable Storage type i.e.,
// D::Error thus this also becomes generic; the error returned by the cached
// storage will also be based on the type implemented by the Durable Storage
//
impl<D, C, K> RaftStorage<C> for CachedStorage<D, C, K>
where
    D: DurableStorage<C>,
    K: StorageCache<C>,
    C: Clone,
{
    // D::Error means the error type is whatever type the specific durable
    // backend defines - this way we keep this generic
    type Error = D::Error;

    // We will first go and try to access the cache
    // if something is present we wrap that to the result and simply
    // return, but if it is None, then we go for the durable storage
    // but note that durable storage can also fail, thus we try and
    // hence the return is Result<T, Error> where T = PersistentMetadata
    // which is consumed directly by the RaftNode and the error is based
    // on the type of durable storage
    fn load_metadata(
        &self,
    ) -> Result<PersistentMetadata, Self::Error> {
        if let Some(metadata) = self.cache.metadata() {
            return Ok(
                PersistentMetadata::new(
                    metadata.current_term(),
                    metadata.voted_for(),
                ),
            );
        }

        self.durable.load_metadata()
    }

    // Frequently we access the current term, which is sent during
    // AppendEntries and vote requests.
    // We first try cached metadata and fall back to durable metadata.
    fn current_term(&self) -> Result<Term, Self::Error> {
        if let Some(metadata) = self.cache.metadata() {
            return Ok(metadata.current_term());
        }

        // ?. - return error immediately
        // If it is Ok, then extract PersistentMetadata
        Ok(self.durable.load_metadata()?.current_term())
    }

    // same as current_term
    fn voted_for(&self) -> Result<Option<ServerId>, Self::Error> {
        if let Some(metadata) = self.cache.metadata() {
            return Ok(metadata.voted_for());
        }

        Ok(self.durable.load_metadata()?.voted_for())
    }

    fn log(&self) -> &RaftLog<C> {
        if let Some(log) = self.cache.log() {
            return log;
        }

        self.durable.log()
    }

    fn last_log_index(
        &self,
    ) -> Result<LogIndex, Self::Error> {
        if let Some(log) = self.cache.log() {
            return Ok(log.last_index());
        }

        self.durable.last_log_index()
    }

    fn term_at(
        &self,
        index: LogIndex,
    ) -> Result<Option<Term>, Self::Error> {
        if let Some(log) = self.cache.log() {
            return Ok(log.term_at(index));
        }

        self.durable.term_at(index)
    }

    fn entries_from(
        &self,
        start_index: LogIndex,
    ) -> Result<Vec<LogEntry<C>>, Self::Error>
    where
        C: Clone,
    {
        if let Some(log) = self.cache.log() {
            let mut result_entries = Vec::new();
            let mut index = start_index;

            while let Some(entry) = log.get(index) {
                result_entries.push(entry.clone());
                index = index.next();
            }

            return Ok(result_entries);
        }

        self.durable.entries_from(start_index)
    }

    fn save_metadata(
        &mut self,
        metadata: PersistentMetadata,
    ) -> Result<(), Self::Error> {
        self.durable.save_metadata(
            PersistentMetadata::new(
                metadata.current_term(),
                metadata.voted_for(),
            ),
        )?;

        self.cache.store_metadata(&metadata);

        Ok(())
    }

    fn append_log_entry(
        &mut self,
        entry: LogEntry<C>,
    ) -> Result<LogIndex, Self::Error> {
        let index = self.durable.append_log_entry(entry)?;

        self.cache.store_log(self.durable.log());

        Ok(index)
    }

    fn truncate_log_from(
        &mut self,
        index: LogIndex,
    ) -> Result<(), Self::Error> {
        self.durable.truncate_log_from(index)?;

        self.cache.store_log(self.durable.log());

        Ok(())
    }

    fn log_at(
        &self,
        index: LogIndex,
    ) -> Result<Option<LogEntry<C>>, Self::Error>
    where
        C: Clone,
    {
        if let Some(log) = self.cache.log() {
            return Ok(log.get(index).cloned());
        }

        self.durable.log_at(index)
    }
}
