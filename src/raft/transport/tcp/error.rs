
use std::fmt;

use tracing_subscriber::fmt::FormattedFields;

#[derive(Debug)]
pub enum TcpTransportError {
    /// The TCP connection could not established
    ConnectionFailed(String),
    ConnectionClosed,
    InvalidFrame(String),
    InvalidHandshake(String),
    UnsupportedProtocolVersion(u16),
    Serialization(String),
    Deserialization(String),

    FrameTooLarge {
        size: usize, 
        max_size: usize,
    },

    /// An unexpected peer was encountered.
    UnknownPeer(String),
    /// An underlying I/O operation failed.
    Io(std::io::Error),
}

impl fmt::Display for TcpTransportError {
    fn fmt(
        &self,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Self::ConnectionFailed(message) => {
                write!(
                    formatter,
                    "TCP connection failed: {}",
                    message
                )
            }

            Self::ConnectionClosed => {
                write!(formatter, "TCP connection closed")
            }

            Self::InvalidFrame(message) => {
                write!(
                    formatter,
                    "invalid TCP frame: {}",
                    message
                )
            }

            Self::InvalidHandshake(message) => {
                write!(
                    formatter,
                    "invalid TCP handshake: {}",
                    message
                )
            }

            Self::UnsupportedProtocolVersion(version) => {
                write!(
                    formatter,
                    "unsupported protocol version: {}",
                    version
                )
            }

            Self::Serialization(message) => {
                write!(
                    formatter,
                    "message serialization failed: {}",
                    message
                )
            }

            Self::Deserialization(message) => {
                write!(
                    formatter,
                    "message deserialization failed: {}",
                    message
                )
            }

            Self::FrameTooLarge {
                size,
                max_size,
            } => {
                write!(
                    formatter,
                    "TCP frame is too large: {} bytes \
                     (maximum {} bytes)",
                    size,
                    max_size
                )
            }

            Self::UnknownPeer(message) => {
                write!(
                    formatter,
                    "unknown peer: {}",
                    message
                )
            }

            Self::Io(error) => {
                write!(formatter, "TCP I/O error: {}", error)
            }
        }
    }
}

/// Make TcpTransportError implement Rust's standard Error trait.
impl std::error::Error for TcpTransportError {}

/// teaching how to convert a std::io::Error into a TcpTransportError.
/// This helps when we are using shorthands e.g., ?
impl From<std::io::Error> for TcpTransportError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}