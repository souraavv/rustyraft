//! Encoding and decoding for the TCP wire protocol.
//!
//! This module converts network protocol values to and from bytes.
//! TCP framing is handled by the connection layer.

use serde::de::DeserializeOwned;
use serde::Serialize;

use super::error::TcpTransportError;

/// Encodes a value into bytes.
///
/// The serialized bytes represent the payload of a single
/// network frame. The frame length itself is not encoded here.
pub fn encode<T>(
    value: &T,
) -> Result<Vec<u8>, TcpTransportError>
where
    T: Serialize,
{
    // bincode::config::standard() defines the default layout rules
    // - Little endian, integer encoding (variant - small number small byte)
    bincode::serde::encode_to_vec(
        value,
        bincode::config::standard(),
    )
    .map_err(|error: bincode::error::EncodeError| {
        // This is a serialization error
        TcpTransportError::Serialization(
            error.to_string(),
        )
    })
}

/// Decodes a value from bytes.
///
/// The provided bytes must contain exactly one serialized
/// protocol value. Trailing bytes are rejected.
pub fn decode<T>(
    bytes: &[u8],
) -> Result<T, TcpTransportError>
where
    T: DeserializeOwned,
{
    let (value, bytes_read) =
        bincode::serde::decode_from_slice(
            bytes,
            bincode::config::standard(),
        )
        .map_err(|error: bincode::error::DecodeError| {
            // deserialization error
            TcpTransportError::Deserialization(
                error.to_string(),
            )
        })?;

    if bytes_read != bytes.len() {
        return Err(
            TcpTransportError::InvalidFrame(format!(
                "decoded {} bytes out of {}",
                bytes_read,
                bytes.len(),
            )),
        );
    }

    Ok(value)
}