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
//! The storage implementation owns both the durable repsentation and, when
//! enabled, in-memory cache 
//! 
//! RaftNode should only interfact with the storage API and shoud not
//! coordinate cache update itself.

use std::convert::Infallible;
use std::marker::PhantomData;

use crate::raft::log::RaftLog;

use crate::raft::state::{
    PersistentState,
    LogIndex,
    ServerId,
    Term,
};

use crate::raft::LogEntry;

use std::fs::{
    self,
    File,
};

use std::io::{
    self,
    Read,
    Write,
};

use std::path::{
    Path,
    PathBuf,
};

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
/// A succesful mutating operation means that the storage implementation 
/// has persisted the change accordingly to the durability contract
/// 
/// The trait doesn't expose a concrete file format or serialization mechanism
/// Those belongs to the storage implementation

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

    /// Return all entry starting at 
    fn entries_from(
        &self, 
        start_index: LogIndex,
    ) -> Result<Vec<LogEntry<C>>, Self::Error>
    where 
        C: Clone;

    fn append_log_entry(
        &mut self, 
        entry: LogEntry<C>
    ) -> Result<LogIndex, Self::Error>;

    /// remove logs from 
    fn truncate_log_from(
        &mut self, 
        index: LogIndex,
    ) -> Result<(), Self::Error>;

}

/// Optional Cache used by storage implemenation 
/// 
/// The cache is an optimization, not the source of truth 
/// 
/// A cache implemenation may return `None` when it has no cached value
/// 
/// The storage layer then reads from the durable backend
/// 
/// Log cache operation requires C:Clone because the durable log and 
/// cache intentionally owns separate copy of the commands
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
/// later you will see that DurableStorage trai is also implemented by 
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
    )
    {
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
        C: Clone
    {
        let mut cached_log = RaftLog::new();

        for log_entry in log.iter() {
            cached_log.append(
                LogEntry::new(
                    log_entry.term,
                    log_entry.command.clone(),
                ),
            );
        }   

        self.log = Some(cached_log)
    }

    fn invalidate_log(&mut self) {
        self.log = None;
    }
}


/// Storage boudary that combines a durable backend with an optional 
/// cache
/// 
/// The storage layer ownes both sides of persistent operation.
/// 
/// RaftNode never write the cache or durable backend
/// 
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
    fn last_log_index(
        &self,
    ) -> Result<LogIndex, Self::Error>;

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

    // ------metadata persistence ------
    fn save_metadata(
        &mut self, 
        metadata: PersistentMetadata,
    ) -> Result<(), Self::Error>;


}

/// Storage boudary that combines a durable backend with an optional
/// cache
///
/// The storage layer ownes both sides of persistent operation.
///
/// RaftNode never write the cache or durable backend
///
/// A write is considered successfully only after the durable backend
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
    // where as Phantom<fn() -> C> means truct type dependent on C, but
    // it doesn't owns a C
    _marker: PhantomData<fn() -> C>,
}

impl <D, C, K> CachedStorage<D, C, K>
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
// Cached Storage is desing to pick any durable storage (D) which support 
// caching explicity (K)
// the underlying storage also is generic of the type of Logs we want to 
// store which is C
// Same we may we have errors coming from the Durable Storage type i.e., 
// D::Error thus this also become generic the error thrown by the Cache
// storage will also be based on the type implemented by the Durable Storage
// 
impl<D, C, K> RaftStorage<C> 
    for CachedStorage<D, C, K>
where
    D: DurableStorage<C>, 
    K: StorageCache<C>,
    C: Clone,
{
    // D::Error means the error type is whatever type the specific durable
    // backend defines - this way we keep this generic
    type Error = D::Error;

    // We will first go and try to access the cache
    // if Some thing is present we wrap that to the result as Ok  and simply 
    // returns, but if it is None, then we will go for the durable storage
    // but note that a durable storage can also fails thus we try and
    // hence the return is Result<T, Error> where T = PersistentMetadata
    // which is consumed directly by the RaftNode and again the error is based
    // on the type of durable storage
    fn load_metadata(
        &self,
    ) -> Result<PersistentMetadata, Self::Error>
    {
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

    // Frequenty we want to access the current term, this is what we send
    // during appendEntries and requesting votes
    // we will try to fetch this via cache metadata which expose method
    // but if not then we wil try to get it via the metadata
    fn current_term(&self) -> Result<Term, Self::Error> {
        if let Some(metadata) = self.cache.metadata() {
            return Ok(metadata.current_term());
        }

        // ?. - return error immediately
        // If it is Ok, then extract PersistentMetadata
        Ok(self.durable.load_metadata()?.current_term())
    }

    // same as current term
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
    ) -> Result<LogIndex, Self::Error>
    {
        if let Some(log) = self.cache.log() {
            return Ok(log.last_index());
        }
        self.durable.last_log_index()
    }

    fn term_at(
        &self,
        index: LogIndex,
    ) -> Result<Option<Term>, Self::Error>
    {
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

            return Ok(result_entries)
        }

        self.durable.entries_from(start_index)
    }

    fn save_metadata(
        &mut self, 
        metadata: PersistentMetadata,
    ) -> Result<(), Self::Error>
    {
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
    ) -> Result<LogIndex, Self::Error>
    {
        let index = self.durable.append_log_entry(entry)?;

        self.cache.store_log(self.durable.log());

        Ok(index)
    }

    fn truncate_log_from(
        &mut self, 
        index: LogIndex,
    ) -> Result<(), Self::Error>
    {
        self.durable.truncate_log_from(index)?;

        self.cache.store_log(self.durable.log());

        Ok(())
    }

    fn log_at(
        &self,
        index: LogIndex,
    ) -> Result<Option<LogEntry<C>>, Self::Error>
    where 
        C: Clone
    {
        if let Some(log) = self.cache.log() {
            return Ok(log.get(index).cloned());
        }

        self.durable.log_at(index)
    }

}

/// In-memory implementation of Raft storage contract
/// 
/// Simple
#[derive(Debug)]
pub struct InMemoryStorage<C> {
    metadata: PersistentMetadata,
    log: RaftLog<C>,
}

impl<C> InMemoryStorage<C> {

    pub fn new() -> Self {
        Self {
            metadata: PersistentMetadata::new(
                Term::ZERO, 
                None,
            ),
            log: RaftLog::new(),
        }
    }

    // from persistent state to the storage
    pub fn from_persistent_state(
        persistent: PersistentState<RaftLog<C>>,
    ) -> Self {
        Self {
            metadata: PersistentMetadata::new(
                persistent.current_term,
                persistent.voted_for,
            ),
            log: persistent.log,
        }
    }

    // from storage to instate
    pub fn into_persistent_state(
        self,
    ) -> PersistentState<RaftLog<C>> {
        PersistentState::new(
            self.metadata.current_term(),
            self.metadata.voted_for(),
            self.log,
        )
    }
}

impl<C> Default for InMemoryStorage<C> {
    fn default() -> Self {
        Self::new()
    }
}

/// This is vauge method but i'm keeping - for some reason I'm believing
/// that in memory is durable .. well
/// 
impl<C> DurableStorage<C> for InMemoryStorage<C> {
    type Error = Infallible;

    fn load_metadata(&self) -> Result<PersistentMetadata, Self::Error> {
        Ok(PersistentMetadata::new(
            self.metadata.current_term(),
            self.metadata.voted_for(),
        ))
    }

    fn save_metadata(
        &mut self, 
        metadata: PersistentMetadata,
    ) -> Result<(), Self::Error>
    {
        self.metadata = metadata;
        Ok(())
    }

    fn last_log_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.log.last_index())
    }

    fn term_at(
        &self, 
        index: LogIndex,
    ) -> Result<Option<Term>, Self::Error>
    {
        Ok(self.log.term_at(index))
    }

    fn log_at(
        &self,
        index: LogIndex,
    ) -> Result<Option<LogEntry<C>>, Self::Error>
    where 
        C: Clone
    {
        Ok(self.log.get(index).cloned())
    }

    fn entries_from(
        &self, 
        start_index: LogIndex,
    ) -> Result<Vec<LogEntry<C>>, Self::Error>
    where 
        C: Clone
    {
        let mut entries = Vec::new();
        let mut index = start_index;

        while let Some(entry) = self.log.get(index) {
            entries.push(entry.clone());
            index = index.next();
        }

        Ok(entries)
    }

    fn append_log_entry(
        &mut self, 
        entry: LogEntry<C>
    ) -> Result<LogIndex, Self::Error>
    {
        Ok(self.log.append(entry))
    }

    fn truncate_log_from(
        &mut self, 
        index: LogIndex,
    ) -> Result<(), Self::Error>
    {
        self.log.truncate_from(index);
        Ok(())
    }

    fn log(&self) -> &RaftLog<C> {
        &self.log
    }
}

impl<C> RaftStorage<C> for InMemoryStorage<C> {

    type Error = Infallible;

    fn load_metadata(
        &self,
    ) -> Result<PersistentMetadata, Self::Error> {
        <Self as DurableStorage<C>>::load_metadata(self)
    }

    fn current_term(&self) -> Result<Term, Self::Error> {
        Ok(self.metadata.current_term())
    }

    fn voted_for(&self) -> Result<Option<ServerId>, Self::Error> {
        Ok(self.metadata.voted_for())
    }

    fn log(&self) -> &RaftLog<C> {
        &self.log
    }

    fn last_log_index(
        &self,
    ) -> Result<LogIndex, Self::Error> {
        <Self as DurableStorage<C>>::last_log_index(self)
    }

    fn term_at(
        &self,
        index: LogIndex,
    ) -> Result<Option<Term>, Self::Error> {
        <Self as DurableStorage<C>>::term_at(self, index)
    }

    fn log_at(
        &self,
        index: LogIndex,
    ) -> Result<Option<LogEntry<C>>, Self::Error>
    where
        C: Clone,
    {
        <Self as DurableStorage<C>>::log_at(
            self,
            index,
        )
    }

    fn entries_from(
        &self,
        start_index: LogIndex,
    ) -> Result<Vec<LogEntry<C>>, Self::Error>
    where
        C: Clone,
    {
        <Self as DurableStorage<C>>::entries_from(
            self,
            start_index,
        )
    }

    fn save_metadata(
        &mut self,
        metadata: PersistentMetadata,
    ) -> Result<(), Self::Error> {
        <Self as DurableStorage<C>>::save_metadata(
            self,
            metadata,
        )
    }

    fn append_log_entry(
        &mut self,
        entry: LogEntry<C>,
    ) -> Result<LogIndex, Self::Error> {
        <Self as DurableStorage<C>>::append_log_entry(
            self,
            entry,
        )
    }

    fn truncate_log_from(
        &mut self,
        index: LogIndex,
    ) -> Result<(), Self::Error> {
        <Self as DurableStorage<C>>::truncate_log_from(
            self,
            index,
        )
    }
}


/// File-backed durable storage for Raft.
///
/// The storage keeps the recovered metadata and log in memory so normal
/// Raft reads do not require disk I/O. Mutating operations persist the
/// new state first and update the in-memory representation only after
/// the durable write succeeds.
///
/// Commands are stored as UTF-8 strings in this first implementation.
/// A future codec abstraction can support arbitrary command types.

#[derive(Debug)]
pub struct FileStorage<C> {
    path: PathBuf,
    metadata: PersistentMetadata,
    log: RaftLog<C>,
}

impl<C> FileStorage<C>
where
    C: From<String>,
{
    // String literals are embedded in to the binary, thus 
    // &'static str means a reference to a string which has entire programs
    // lifetime ('static -> represent lifetime). Static surivive entier programs
    // runtime
    const METADATA_FILE: &'static str = "metadata";
    const LOG_FILE: &'static str = "log";

    const METADATA_MAGIC: &'static [u8] =
        b"RUSTYRAFT-META-1";

    const LOG_MAGIC: &'static [u8] =
        b"RUSTYRAFT-LOG-1";

    pub fn open<P>(
        path: P,
    ) -> Result<Self, io::Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().to_path_buf();

        fs::create_dir_all(&path)?;

        let metadata_path =
            path.join(Self::METADATA_FILE);

        let log_path =
            path.join(Self::LOG_FILE);

        let metadata =
            if metadata_path.exists() {
                Self::read_metadata(
                    &metadata_path,
                )?
            } else {
                PersistentMetadata::new(
                    Term::ZERO,
                    None,
                )
            };

        let log =
            if log_path.exists() {
                Self::read_log(
                    &log_path,
                )?
            } else {
                RaftLog::new()
            };

        Ok(Self {
            path,
            metadata,
            log,
        })
    }

    fn metadata_path(
        &self,
    ) -> PathBuf {
        self.path.join(
            Self::METADATA_FILE,
        )
    }

    fn log_path(
        &self,
    ) -> PathBuf {
        self.path.join(
            Self::LOG_FILE,
        )
    }

    fn temporary_path(
        path: &Path,
    ) -> PathBuf {
        let file_name =
            path.file_name()
                .expect(
                    "storage path should have a filename",
                );

        path.with_file_name(
            format!(
                "{}.tmp",
                file_name.to_string_lossy()
            ),
        )
    }

    fn write_metadata(
        path: &Path,
        metadata: &PersistentMetadata,
    ) -> Result<(), io::Error> {
        let temporary =
            Self::temporary_path(path);

        let mut file =
            File::create(&temporary)?;

        file.write_all(
            Self::METADATA_MAGIC,
        )?;

        file.write_all(
            &metadata
                .current_term()
                .value()
                .to_le_bytes(),
        )?;

        match metadata.voted_for() {
            Some(server_id) => {
                file.write_all(&[1])?;

                file.write_all(
                    &server_id
                        .value()
                        .to_le_bytes(),
                )?;
            }

            None => {
                file.write_all(&[0])?;
            }
        }

        file.sync_all()?;

        fs::rename(
            &temporary,
            path,
        )?;

        Self::sync_directory(
            path.parent()
                .expect(
                    "metadata path should have a parent",
                ),
        )
    }

    fn read_metadata(
        path: &Path,
    ) -> Result<
        PersistentMetadata,
        io::Error,
    > {
        let mut file =
            File::open(path)?;

        let mut magic =
            vec![0u8; Self::METADATA_MAGIC.len()];

        file.read_exact(
            &mut magic,
        )?;

        if magic
            != Self::METADATA_MAGIC
        {
            return Err(
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid Raft metadata file",
                ),
            );
        }

        let term =
            Self::read_u64(
                &mut file,
            )?;

        let mut vote_marker =
            [0u8; 1];

        file.read_exact(
            &mut vote_marker,
        )?;

        let voted_for =
            match vote_marker[0] {
                0 => None,

                1 => Some(
                    ServerId::new(
                        Self::read_u64(
                            &mut file,
                        )?,
                    ),
                ),

                _ => {
                    return Err(
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid voted_for marker",
                        ),
                    );
                }
            };

        Ok(
            PersistentMetadata::new(
                Term::new(term),
                voted_for,
            ),
        )
    }

    /// T: AsRef<str> means:
    // Give me any type T that knows how to give me a reference to a str.
    // e.g., "name", String::from("hello")
    // T as ref -> &str
    fn write_log(
        path: &Path,
        log: &RaftLog<C>,
    ) -> Result<(), io::Error>
    where
        C: AsRef<str>,
    {
        let temporary =
            Self::temporary_path(path);

        let mut file =
            File::create(&temporary)?;

        file.write_all(
            Self::LOG_MAGIC,
        )?;

        file.write_all(
            &(log.len() as u64)
                .to_le_bytes(),
        )?;

        for entry in log.iter() {
            let command =
                entry.command.as_ref();

            let bytes =
                command.as_bytes();

            file.write_all(
                &entry
                    .term
                    .value()
                    .to_le_bytes(),
            )?;

            file.write_all(
                &(bytes.len() as u64)
                    .to_le_bytes(),
            )?;

            file.write_all(
                bytes,
            )?;
        }

        file.sync_all()?;

        fs::rename(
            &temporary,
            path,
        )?;

        Self::sync_directory(
            path.parent()
                .expect(
                    "log path should have a parent",
                ),
        )
    }

    fn read_log(
        path: &Path,
    ) -> Result<
        RaftLog<C>,
        io::Error,
    > {
        let mut file =
            File::open(path)?;

        let mut magic =
            vec![0u8; Self::LOG_MAGIC.len()];

        file.read_exact(
            &mut magic,
        )?;

        if magic
            != Self::LOG_MAGIC
        {
            return Err(
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid Raft log file",
                ),
            );
        }

        let entry_count =
            Self::read_u64(
                &mut file,
            )?;

        let mut log =
            RaftLog::new();

        for _ in 0..entry_count {
            let term =
                Term::new(
                    Self::read_u64(
                        &mut file,
                    )?,
                );

            let command_length =
                Self::read_u64(
                    &mut file,
                )?;

            let command_length =
                usize::try_from(
                    command_length,
                )
                .map_err(
                    |_| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "command is too large",
                        )
                    },
                )?;

            let mut bytes =
                vec![0u8; command_length];

            file.read_exact(
                &mut bytes,
            )?;

            let command =
                String::from_utf8(
                    bytes,
                )
                .map_err(
                    |_| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "log command is not valid UTF-8",
                        )
                    },
                )?;

            log.append(
                LogEntry::new(
                    term,
                    C::from(command),
                ),
            );
        }

        Ok(log)
    }

    fn read_u64(
        file: &mut File,
    ) -> Result<u64, io::Error> {
        let mut bytes =
            [0u8; 8];

        file.read_exact(
            &mut bytes,
        )?;

        Ok(
            u64::from_le_bytes(
                bytes,
            )
        )
    }

    fn sync_directory(
        path: &Path,
    ) -> Result<(), io::Error> {
        let directory =
            File::open(path)?;

        directory.sync_all()
    }
}

impl<C> DurableStorage<C>
    for FileStorage<C>
where
    C: From<String> + AsRef<str>,
{
    type Error = io::Error;

    fn load_metadata(
        &self,
    ) -> Result<
        PersistentMetadata,
        Self::Error,
    > {
        Ok(
            PersistentMetadata::new(
                self.metadata.current_term(),
                self.metadata.voted_for(),
            )
        )
    }

    fn log(
        &self,
    ) -> &RaftLog<C> {
        &self.log
    }

    fn save_metadata(
        &mut self,
        metadata: PersistentMetadata,
    ) -> Result<(), Self::Error> {
        Self::write_metadata(
            &self.metadata_path(),
            &metadata,
        )?;

        self.metadata =
            metadata;

        Ok(())
    }

    fn last_log_index(
        &self,
    ) -> Result<
        LogIndex,
        Self::Error,
    > {
        Ok(
            self.log.last_index()
        )
    }

    fn term_at(
        &self,
        index: LogIndex,
    ) -> Result<
        Option<Term>,
        Self::Error,
    > {
        Ok(
            self.log.term_at(index)
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
        Ok(
            self.log
                .get(index)
                .cloned()
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
        let mut entries =
            Vec::new();

        let mut index =
            start_index;

        while let Some(entry) =
            self.log.get(index)
        {
            entries.push(
                entry.clone()
            );

            index =
                index.next();
        }

        Ok(entries)
    }

    fn append_log_entry(
        &mut self,
        entry: LogEntry<C>,
    ) -> Result<
        LogIndex,
        Self::Error,
    > {
        let index =
            self.log.last_index()
                .next();

        let mut temporary_log =
            RaftLog::new();

        for existing in
            self.log.iter()
        {
            temporary_log.append(
                LogEntry::new(
                    existing.term,
                    existing.command.as_ref()
                        .to_string()
                        .into(),
                ),
            );
        }

        temporary_log.append(
            entry,
        );

        Self::write_log(
            &self.log_path(),
            &temporary_log,
        )?;

        self.log =
            temporary_log;

        Ok(index)
    }

    fn truncate_log_from(
        &mut self,
        index: LogIndex,
    ) -> Result<(), Self::Error> {
        let mut truncated_log =
            RaftLog::new();

        let mut current_index =
            LogIndex::new(1);

        while let Some(entry) =
            self.log.get(current_index)
        {
            if current_index >= index {
                break;
            }

            truncated_log.append(
                LogEntry::new(
                    entry.term,
                    entry.command.as_ref()
                        .to_string()
                        .into(),
                ),
            );

            current_index =
                current_index.next();
        }

        Self::write_log(
            &self.log_path(),
            &truncated_log,
        )?;

        self.log =
            truncated_log;

        Ok(())
    }
}