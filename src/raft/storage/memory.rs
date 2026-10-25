
use std::convert::Infallible;

use crate::raft::log::RaftLog;

use crate::raft::state::{
    PersistentState,
    LogIndex,
    ServerId,
    Term,
};

use crate::raft::LogEntry;

use super::{
    DurableStorage,
    PersistentMetadata,
    RaftStorage,
};

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

    fn load_metadata(
        &self,
    ) -> Result<PersistentMetadata, Self::Error> {
        Ok(
            PersistentMetadata::new(
                self.metadata.current_term(),
                self.metadata.voted_for(),
            ),
        )
    }

    fn save_metadata(
        &mut self,
        metadata: PersistentMetadata,
    ) -> Result<(), Self::Error> {
        self.metadata = metadata;

        Ok(())
    }

    fn last_log_index(
        &self,
    ) -> Result<LogIndex, Self::Error> {
        Ok(self.log.last_index())
    }

    fn term_at(
        &self,
        index: LogIndex,
    ) -> Result<Option<Term>, Self::Error> {
        Ok(self.log.term_at(index))
    }

    fn log_at(
        &self,
        index: LogIndex,
    ) -> Result<Option<LogEntry<C>>, Self::Error>
    where
        C: Clone,
    {
        Ok(self.log.get(index).cloned())
    }

    fn entries_from(
        &self,
        start_index: LogIndex,
    ) -> Result<Vec<LogEntry<C>>, Self::Error>
    where
        C: Clone,
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
        entry: LogEntry<C>,
    ) -> Result<LogIndex, Self::Error> {
        Ok(self.log.append(entry))
    }

    fn truncate_log_from(
        &mut self,
        index: LogIndex,
    ) -> Result<(), Self::Error> {
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
        <Self as DurableStorage<C>>::term_at(
            self,
            index,
        )
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
