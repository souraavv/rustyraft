//! Raft log implementation
//!
//! The RAFT log is an ordered sequence of entries
//! Each entry contains:
//!     - the term in which entry was created
//!     - the command for the replicated state machine
//!
//! Raft log indexes are one-based
//!
//! We are not implementing persistence here deliberately. We willd do that
//! in the storage layer
//!  

use crate::raft::state::{LogIndex, Term};

/// Each entry has the term in which it was added + Generic Command
#[derive(Debug, Clone, Copy)]
pub struct LogEntry<C> {
    pub term: Term,
    pub command: C,
}

impl<C> LogEntry<C> {
    pub fn new(term: Term, command: C) -> Self {
        Self { term, command }
    }
}

/// We have collection of such log entry
#[derive(Debug)]
pub struct RaftLog<C> {
    entries: Vec<LogEntry<C>>,
}

impl<C> RaftLog<C> {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// LogIndex::ZERO represents the position before the first entry,
    /// so an empty log has last_index() == ZERO
    pub fn last_index(&self) -> LogIndex {
        LogIndex::new(self.entries.len() as u64)
    }

    /// Returns the term of last log entry
    ///
    /// REturn none when the log is empty
    pub fn last_term(&self) -> Option<Term> {
        self.entries.last().map(|entry| entry.term)
    }

    /// Returns the log entry at the given RAFT index
    ///
    /// LogIndex::ZERO does not refer to an actual entry and we will return None
    /// in that case
    pub fn get(&self, index: LogIndex) -> Option<&LogEntry<C>> {
        let index = index.value();

        if index == 0 {
            return None;
        }

        self.entries.get((index - 1) as usize)
    }

    pub fn get_mut(&mut self, index: LogIndex) -> Option<&mut LogEntry<C>> {
        let index = index.value();

        if index == 0 {
            return None;
        }

        self.entries.get_mut((index - 1) as usize)
    }

    /// None when the index does not refer to an existing log entry
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        self.get(index).map(|entry| entry.term)
    }

    /// Append an entry to the end of the Logs
    pub fn append(&mut self, entry: LogEntry<C>) -> LogIndex {
        self.entries.push(entry);
        self.last_index()
    }

    /// Remove an entry at index and all the entries after it
    ///
    /// This operation is required when follower discovers conflicting
    /// log entires during AppendEntries processing
    pub fn truncate_from(&mut self, index: LogIndex) {
        let index = index.value();

        if index == 0 {
            self.entries.clear();
            return;
        }

        let position = (index - 1) as usize;

        if position < self.entries.len() {
            self.entries.truncate(position);
        }
    }

    /// Returns if the log entries contained at give index with a given term
    pub fn matches(&self, index: LogIndex, term: Term) -> bool {
        self.term_at(index) == Some(term)
    }

    /// Abstraction for the concrete iterator - we are just trying to gaurantee
    /// iterator will have (i.e., impl Iterator) anything which can implement
    /// Iterator is applicable - we care of behavior and not actual type
    /// We dont' want to make the entries public, thus we are giving a
    /// controlled way for iteration
    ///
    /// trait Iterator { type Item; fn next(&mut self) -> Option<Self::Item>; }
    pub fn iter(&self) -> impl Iterator<Item = &LogEntry<C>> {
        self.entries.iter()
    }
}

impl<C> Default for RaftLog<C> {
    fn default() -> Self {
        Self::new()
    }
}
