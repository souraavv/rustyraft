//! Durable storage abstraction for the Raft Server
//! 
//! The Raft protocol separates persistent state from volatile state
//! 

use std::convert::Infallible;

use crate::raft::log::RaftLog;

use crate::raft::state::PersistentState;
use crate::raft::{
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
/// A succesful mutating operation means that the storage implementation 
/// has persisted the change accordingly to the durability contract
/// 
/// The trait doesn't expose a concrete file format or serialization mechanism
/// Those belongs to the storage implementation

pub trait RaftStorage<C> {

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

    fn log_entry(
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

impl<C> RaftStorage<C> for InMemoryStorage<C> {
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

    fn log_entry(
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



