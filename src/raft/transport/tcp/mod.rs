//! TCP transport implementation.
//!
//! This module provides the production transport used for
//! communication between RustyRaft nodes.

mod codec;
mod connection;
mod connection_manager;
mod error;
mod listener;
mod protocol;
mod transport;

pub use connection_manager::PeerAddress;
pub use error::TcpTransportError;
pub use transport::TcpTransport;