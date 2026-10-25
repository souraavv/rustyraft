
use std::convert::TryFrom;
use std::io::{
    self,
    Read,
    Write,
};

use crate::raft::log::RaftLog;
use crate::raft::state::{
    ServerId,
    Term,
};

use crate::raft::{LogEntry, LogEntryKind};

use super::PersistentMetadata;

const METADATA_MAGIC: &[u8] = b"RUSTYRAFT-META-1";
const LOG_MAGIC_V2: &[u8] = b"RUSTYRAFT-LOG-2";
const LOG_MAGIC_V3: &[u8] = b"RUSTYRAFT-LOG-3";

pub(super) fn write_metadata<W: Write>(
    writer: &mut W,
    metadata: &PersistentMetadata,
) -> Result<(), io::Error> {
    writer.write_all(METADATA_MAGIC)?;

    writer.write_all(
        &metadata.current_term().value().to_le_bytes(),
    )?;

    match metadata.voted_for() {
        Some(server_id) => {
            writer.write_all(&[1])?;

            writer.write_all(
                &server_id.value().to_le_bytes(),
            )?;
        }
        None => {
            writer.write_all(&[0])?;
        }
    }

    Ok(())
}

pub(super) fn read_metadata<R: Read>(
    reader: &mut R,
) -> Result<PersistentMetadata, io::Error> {
    let mut magic = vec![0_u8; METADATA_MAGIC.len()];
    reader.read_exact(&mut magic)?;

    if magic != METADATA_MAGIC {
        return Err(invalid_data("invalid Raft metadata file"));
    }

    let term = Term::new(read_u64(reader)?);

    let mut vote_marker = [0_u8; 1];
    reader.read_exact(&mut vote_marker)?;

    let voted_for = match vote_marker[0] {
        0 => None,
        1 => Some(ServerId::new(read_u64(reader)?)),
        _ => {
            return Err(invalid_data("invalid voted_for marker"));
        }
    };

    Ok(PersistentMetadata::new(term, voted_for))
}

pub(super) fn write_log<C, W>(
    writer: &mut W,
    log: &RaftLog<C>,
) -> Result<(), io::Error>
where
    C: AsRef<str>,
    W: Write,
{
    // Version 3 represents command and no-op entries explicitly.
    writer.write_all(LOG_MAGIC_V3)?;
    writer.write_all(&(log.len() as u64).to_le_bytes())?;

    for entry in log.iter() {
        writer.write_all(&entry.term.value().to_le_bytes())?;
        match &entry.kind {
            LogEntryKind::Command { command, request_id } => {
                writer.write_all(&[0])?;
                match request_id {
                    Some(request_id) => {
                        writer.write_all(&[1])?;
                        write_bytes(writer, request_id.as_bytes())?;
                    }
                    None => writer.write_all(&[0])?,
                }
                write_bytes(writer, command.as_ref().as_bytes())?;
            }
            LogEntryKind::Noop => writer.write_all(&[1])?,
        }
    }

    Ok(())
}

pub(super) fn read_log<C, R>(
    reader: &mut R,
) -> Result<RaftLog<C>, io::Error>
where
    C: From<String>,
    R: Read,
{
    let mut magic = vec![0_u8; LOG_MAGIC_V3.len()];
    reader.read_exact(&mut magic)?;

    let is_v2 = magic == LOG_MAGIC_V2;
    if !is_v2 && magic != LOG_MAGIC_V3 {
        return Err(invalid_data("invalid Raft log file"));
    }

    let entry_count = read_u64(reader)?;
    let mut log = RaftLog::new();

    for _ in 0..entry_count {
        let term = Term::new(read_u64(reader)?);
        let entry = if is_v2 {
            // Version 2 always stores a command after request-ID metadata.
            let request_id = read_request_id(reader)?;
            let command = C::from(read_string(reader, "log command")?);
            match request_id {
                Some(request_id) => {
                    LogEntry::new_client_command(term, request_id, command)
                }
                None => LogEntry::new(term, command),
            }
        } else {
            let mut kind_marker = [0_u8; 1];
            reader.read_exact(&mut kind_marker)?;
            match kind_marker[0] {
                0 => {
                    let request_id = read_request_id(reader)?;
                    let command = C::from(
                        read_string(reader, "log command")?,
                    );
                    match request_id {
                        Some(request_id) => {
                            LogEntry::new_client_command(
                                term, request_id, command,
                            )
                        }
                        None => LogEntry::new(term, command),
                    }
                }
                1 => LogEntry::new_noop(term),
                _ => return Err(invalid_data("invalid log entry kind")),
            }
        };
        log.append(entry);
    }

    Ok(log)
}

fn read_request_id<R: Read>(
    reader: &mut R,
) -> Result<Option<String>, io::Error> {
    let mut marker = [0_u8; 1];
    reader.read_exact(&mut marker)?;
    match marker[0] {
        0 => Ok(None),
        1 => Ok(Some(read_string(reader, "request ID")?)),
        _ => Err(invalid_data("invalid request ID marker")),
    }
}

fn write_bytes<W: Write>(
    writer: &mut W,
    bytes: &[u8],
) -> Result<(), io::Error> {
    writer.write_all(&(bytes.len() as u64).to_le_bytes())?;
    writer.write_all(bytes)
}

fn read_string<R: Read>(
    reader: &mut R,
    field_name: &'static str,
) -> Result<String, io::Error> {
    let length = usize::try_from(read_u64(reader)?)
        .map_err(|_| invalid_data("field length is too large"))?;

    let mut bytes = vec![0_u8; length];
    reader.read_exact(&mut bytes)?;

    String::from_utf8(bytes).map_err(|_| {
        match field_name {
            "request ID" => {
                invalid_data("request ID is not valid UTF-8")
            }
            _ => {
                invalid_data("log command is not valid UTF-8")
            }
        }
    })
}

fn read_u64<R: Read>(
    reader: &mut R,
) -> Result<u64, io::Error> {
    let mut bytes = [0_u8; 8];
    reader.read_exact(&mut bytes)?;

    Ok(u64::from_le_bytes(bytes))
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        message,
    )
}
