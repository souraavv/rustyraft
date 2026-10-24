
use std::fs::{
    self,
    File,
};

use std::io::{
    self,
};

use std::path::{
    Path,
    PathBuf,
};

use crate::raft::log::RaftLog;
use crate::raft::state::{
    LogIndex,
    Term,
};

use crate::raft::LogEntry;

use super::{
    format,
    DurableStorage,
    PersistentMetadata,
};

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
    const METADATA_FILE: &'static str = "metadata";
    const LOG_FILE: &'static str = "log";

    pub fn open<P>(
        path: P,
    ) -> Result<Self, io::Error>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref().to_path_buf();

        fs::create_dir_all(&path)?;

        let metadata_path = path.join(Self::METADATA_FILE);
        let log_path = path.join(Self::LOG_FILE);

        let metadata = if metadata_path.exists() {
            Self::read_metadata(&metadata_path)?
        } else {
            PersistentMetadata::new(
                Term::ZERO,
                None,
            )
        };

        let log = if log_path.exists() {
            Self::read_log(&log_path)?
        } else {
            RaftLog::new()
        };

        Ok(Self {
            path,
            metadata,
            log,
        })
    }

    fn metadata_path(&self) -> PathBuf {
        self.path.join(Self::METADATA_FILE)
    }

    fn log_path(&self) -> PathBuf {
        self.path.join(Self::LOG_FILE)
    }

    fn temporary_path(path: &Path) -> PathBuf {
        let file_name = path.file_name()
            .expect("storage path should have a filename");

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
        let temporary = Self::temporary_path(path);
        let mut file = File::create(&temporary)?;

        format::write_metadata(
            &mut file,
            metadata,
        )?;

        file.sync_all()?;

        fs::rename(
            &temporary,
            path,
        )?;

        Self::sync_directory(
            path.parent()
                .expect("metadata path should have a parent"),
        )
    }

    pub fn read_metadata(
        path: &Path,
    ) -> Result<PersistentMetadata, io::Error> {
        let mut file = File::open(path)?;

        format::read_metadata(&mut file)
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
        let temporary = Self::temporary_path(path);
        let mut file = File::create(&temporary)?;

        format::write_log(
            &mut file,
            log,
        )?;

        file.sync_all()?;

        fs::rename(
            &temporary,
            path,
        )?;

        Self::sync_directory(
            path.parent()
                .expect("log path should have a parent"),
        )
    }

    fn read_log(
        path: &Path,
    ) -> Result<RaftLog<C>, io::Error> {
        let mut file = File::open(path)?;

        format::read_log(&mut file)
    }

    fn sync_directory(
        path: &Path,
    ) -> Result<(), io::Error> {
        let directory = File::open(path)?;

        directory.sync_all()
    }

    fn copy_entry(
        entry: &LogEntry<C>,
    ) -> LogEntry<C>
    where
        C: AsRef<str>,
    {
        let command: C = entry.command.as_ref()
            .to_string()
            .into();

        match entry.request_id() {
            Some(request_id) => LogEntry::new_client_command(
                entry.term,
                request_id.to_owned(),
                command,
            ),
            None => LogEntry::new(
                entry.term,
                command,
            ),
        }
    }
}

impl<C> DurableStorage<C> for FileStorage<C>
where
    C: From<String> + AsRef<str>,
{
    type Error = io::Error;

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

    fn log(&self) -> &RaftLog<C> {
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
        let index = self.log.last_index().next();

        let mut temporary_log = RaftLog::new();

        for existing in self.log.iter() {
            temporary_log.append(Self::copy_entry(existing));
        }

        temporary_log.append(entry);

        Self::write_log(
            &self.log_path(),
            &temporary_log,
        )?;

        self.log = temporary_log;

        Ok(index)
    }

    fn truncate_log_from(
        &mut self,
        index: LogIndex,
    ) -> Result<(), Self::Error> {
        let mut truncated_log = RaftLog::new();
        let mut current_index = LogIndex::new(1);

        while let Some(entry) = self.log.get(current_index) {
            if current_index >= index {
                break;
            }

            truncated_log.append(Self::copy_entry(entry));

            current_index = current_index.next();
        }

        Self::write_log(
            &self.log_path(),
            &truncated_log,
        )?;

        self.log = truncated_log;

        Ok(())
    }
}
