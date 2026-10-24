
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

use crate::raft::LogEntry;

use super::PersistentMetadata;

const METADATA_MAGIC: &[u8] = b"RUSTYRAFT-META-1";
const LOG_MAGIC: &[u8] = b"RUSTYRAFT-LOG-2";

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
    // Version 2 stores an optional request ID for each entry.
    writer.write_all(LOG_MAGIC)?;
    writer.write_all(&(log.len() as u64).to_le_bytes())?;

    for entry in log.iter() {
        writer.write_all(
            &entry.term.value().to_le_bytes(),
        )?;

        match entry.request_id() {
            Some(request_id) => {
                writer.write_all(&[1])?;
                write_bytes(writer, request_id.as_bytes())?;
            }
            None => {
                writer.write_all(&[0])?;
            }
        }

        // Store the application command.
        write_bytes(
            writer,
            entry.command.as_ref().as_bytes(),
        )?;
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
    let mut magic = vec![0_u8; LOG_MAGIC.len()];
    reader.read_exact(&mut magic)?;

    if magic != LOG_MAGIC {
        return Err(invalid_data(
            "unsupported or invalid Raft log format",
        ));
    }

    let entry_count = read_u64(reader)?;
    let mut log = RaftLog::new();

    for _ in 0..entry_count {
        let term = Term::new(read_u64(reader)?);

        let mut request_id_marker = [0_u8; 1];
        reader.read_exact(&mut request_id_marker)?;

        let request_id = match request_id_marker[0] {
            0 => None,
            1 => Some(read_string(reader, "request ID")?),
            _ => {
                return Err(invalid_data("invalid request ID marker"));
            }
        };

        let command = read_string(reader, "log command")?;
        let command = C::from(command);

        let entry = match request_id {
            Some(request_id) => LogEntry::new_client_command(
                term,
                request_id,
                command,
            ),
            None => LogEntry::new(
                term,
                command,
            ),
        };

        log.append(entry);
    }

    Ok(log)
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
