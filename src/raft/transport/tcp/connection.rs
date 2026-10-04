//! A single TCP connection to a Raft peer.
//!
//! This module owns the lifecycle and I/O of one TCP connection.
//! It does not know about Raft protocol decisions or cluster
//! membership.
//!
//! The connection is responsible for:
//! - performing the initial handshake,
//! - reading framed messages,
//! - writing framed messages,
//! - enforcing the maximum frame size.
//!
//! Connection ownership, reconnection, retry and peer management
//! are handled by `connection_manager.rs`.

use tokio::io::{
    AsyncRead,
    AsyncReadExt,
    AsyncWrite,
    AsyncWriteExt,
};
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream,
};

use super::codec::{decode, encode};
use super::error::TcpTransportError;
use super::protocol::{Handshake, PROTOCOL_VERSION};

use crate::raft::state::ServerId;
use crate::raft::transport::message::RaftMessage;

/// Maximum size of a normal Raft message frame.
///
/// Snapshot transfer will eventually use a separate streaming
/// mechanism rather than allowing arbitrarily large normal
/// Raft messages.
pub const MAX_FRAME_SIZE: usize =
    8 * 1024 * 1024;

/// Number of bytes used to encode the frame length.
///
/// The length is encoded as a four-byte big-endian unsigned
/// integer followed by the serialized payload.
const FRAME_LENGTH_SIZE: usize = 4;

/// Represents one established TCP connection to a peer.
pub struct TcpConnection {
    /// The local Raft identity.
    local_server_id: ServerId,

    /// The remote peer's Raft identity.
    peer_server_id: ServerId,

    /// The underlying TCP stream.
    stream: TcpStream,
}

impl TcpConnection {

    // ------------------------------------
    // ------------ public APIs -----------
    // ------------------------------------

    // send, receive and shutdown

    /// Sends one Raft message over the TCP stream.
    ///
    /// This method is used while the connection owns the complete
    /// TCP stream. After the stream is split, the writer task uses
    /// `write_frame()` directly with the owned write half.
    pub async fn send<C>(
        &mut self,
        message: RaftMessage<C>,
    ) -> Result<(), TcpTransportError>
    where
        C: serde::Serialize,
    {
        Self::write_frame(
            &mut self.stream,
            message,
        )
        .await
    }

    /// Receives one Raft message from the TCP stream.
    ///
    /// This method is used while the connection owns the complete
    /// TCP stream. After the stream is split, the reader task uses
    /// `read_frame()` directly with the owned read half.
    pub async fn receive<C>(
        &mut self,
    ) -> Result<RaftMessage<C>, TcpTransportError>
    where
        C: serde::de::DeserializeOwned,
    {
        Self::read_frame(
            &mut self.stream,
        )
        .await
    }

    /// Shuts down the TCP connection.
    pub async fn shutdown(
        &mut self,
    ) -> Result<(), TcpTransportError> {
        self.stream.shutdown().await?;

        Ok(())
    }

    // ----------------------------------------------------------
    // -------- Inbound and outbound traffic initializer --------
    // ----------------------------------------------------------

    // establish and accept

    // Inbound taken care by listener via `accept`,
    // outbound is initialized by the manager which will land up to establish
    // TcpConnection over TcpStream created by the manager
    // (See `Result<Self, ..>`)

    /// Creates a connection from an already-established TCP
    /// stream.
    ///
    /// The TCP handshake is performed before the connection is
    /// returned to the caller.
    /// Remember this stream will be created by the connection manager for us
    pub async fn establish(
        mut stream: TcpStream,
        local_server_id: ServerId,
        expected_peer_id: ServerId,
    ) -> Result<Self, TcpTransportError> {

        // I'm attempting to connect to the peer
        // first create the handshake
        let handshake =
            Handshake::new(local_server_id);

        // Write the handshake frame to the stream
        Self::write_frame(
            &mut stream,
            handshake,
        )
        .await?;

        let peer_handshake: Handshake =
            Self::read_frame(
                &mut stream,
            )
            .await?;

        // Validate the handshake
        Self::validate_handshake(
            &peer_handshake,
        )?;

        // Make sure the peer is the one we intended
        // to connect to.
        if peer_handshake.server_id
            != expected_peer_id
        {
            return Err(
                TcpTransportError::UnknownPeer(
                    format!(
                        "expected peer {:?}, received {:?}",
                        expected_peer_id,
                        peer_handshake.server_id,
                    ),
                ),
            );
        }

        Ok(Self {
            local_server_id,
            peer_server_id:
                peer_handshake.server_id,
            stream,
        })
    }

    /// Creates a connection from an accepted TCP stream.
    ///
    /// The inbound peer sends its handshake first. We validate
    /// that handshake and then send our own handshake in response.
    pub async fn accept(
        mut stream: TcpStream,
        local_server_id: ServerId,
    ) -> Result<Self, TcpTransportError> {

        // Looking for the handshake send by some peer
        let peer_handshake: Handshake =
            Self::read_frame(
                &mut stream,
            )
            .await?;

        // validate the handshake
        Self::validate_handshake(
            &peer_handshake,
        )?;

        // prepare 2way handshake
        let handshake =
            Handshake::new(local_server_id);

        Self::write_frame(
            &mut stream,
            handshake,
        )
        .await?;

        Ok(Self {
            local_server_id,
            peer_server_id:
                peer_handshake.server_id,
            stream,
        })
    }

    /// Splits the established TCP connection into independent
    /// read and write halves.
    ///
    /// The read half can be owned by a reader task and the write
    /// half can be owned by a writer task.
    ///
    /// One TCP connection is still used. Splitting only allows
    /// reading and writing to happen independently.
    pub fn split(
        self,
    ) -> (
        ServerId,
        ServerId,
        OwnedReadHalf,
        OwnedWriteHalf,
    ) {
        let (
            read_half,
            write_half,
        ) = self.stream.into_split();

        (
            self.local_server_id,
            self.peer_server_id,
            read_half,
            write_half,
        )
    }

    // ---------------------------------------------------
    // -------------- Private helpers --------------------
    // ---------------------------------------------------

    // Read frame, write frame, validate handshake, read exact

    /// Reads one length-prefixed frame from an async reader.
    pub (crate) async fn read_frame<R, T>(
        stream: &mut R,
    ) -> Result<T, TcpTransportError>
    where
        R: AsyncRead + Unpin,
        T: serde::de::DeserializeOwned,
    {
        let mut length_bytes =
            [0u8; FRAME_LENGTH_SIZE];

        // `AsyncRead` means that R supports asynchronous reading.
        //
        // `Unpin` means that R is safe to move in memory while it
        // is being used through an async operation. Some async
        // types must remain at a fixed memory address after being
        // pinned, but TcpStream and its owned read half do not have
        // that restriction.
        //
        // `read_exact()` requires this `Unpin` bound because the
        // async read operation may suspend at `.await` and resume
        // later.
        //
        // We do not need to manually pin TcpStream here because
        // TcpStream implements Unpin.
        //
        // `AsyncReadExt` provides the convenient `read_exact()`
        // method on top of the `AsyncRead` trait.
        // 
        // First read the length bytes (i.e., default to 4)
        // - Read 4 bytes which will tell you the message length into the
        // array length_bytes
        Self::read_exact(
            stream,
            &mut length_bytes,
        )
        .await?;

        // simplest decode
        // convert those bytes into the u32 and cast that as usize (later we
        // will use this to declare the array/vector size of payload)
        let frame_length =
            u32::from_be_bytes(length_bytes)
                as usize;

        // ensure if frame_length is within the limits
        if frame_length > MAX_FRAME_SIZE {
            return Err(
                TcpTransportError::FrameTooLarge {
                    size: frame_length,
                    max_size: MAX_FRAME_SIZE,
                },
            );
        }

        // now read the payload, which is frame_length
        let mut payload =
            vec![0u8; frame_length];

        // Now read the payload (we will use TcpStream::read_exact method)
        // which will fill the buffer (semantics of read_exact)
        Self::read_exact(
            stream,
            &mut payload,
        )
        .await?;

        // finally decode the readed payload
        decode(&payload)
    }

    /// Reads exactly the requested number of bytes.
    ///
    /// TCP does not guarantee that one read operation returns all
    /// requested bytes, so `read_exact()` keeps reading until the
    /// requested buffer is full or the connection fails.
    async fn read_exact<R>(
        stream: &mut R,
        buffer: &mut [u8],
    ) -> Result<(), TcpTransportError>
    where
        R: AsyncRead + Unpin,
    {
        match stream.read_exact(buffer).await {
            Ok(_) => Ok(()),

            Err(error)
                if error.kind()
                    == std::io::ErrorKind::UnexpectedEof =>
            {
                Err(
                    TcpTransportError::ConnectionClosed
                )
            }

            Err(error) => {
                Err(TcpTransportError::Io(error))
            }
        }
    }

    /// Writes one length-prefixed frame.
    ///
    /// Frame format:
    ///
    /// ```text
    /// +----------------------+----------------------+
    /// | 4-byte frame length  | serialized payload   |
    /// +----------------------+----------------------+
    /// ```
    ///
    /// The frame length does not include the four-byte length
    /// prefix itself.
    pub (crate) async fn write_frame<W, T>(
        stream: &mut W,
        value: T,
    ) -> Result<(), TcpTransportError>
    where
        W: AsyncWrite + Unpin,
        T: serde::Serialize,
    {
        // Encode the payload which I've to write to th stream
        let encoded_payload =
            encode(&value)?;

        if encoded_payload.len()
            > MAX_FRAME_SIZE
        {
            return Err(
                TcpTransportError::FrameTooLarge {
                    size: encoded_payload.len(),
                    max_size: MAX_FRAME_SIZE,
                },
            );
        }

        let frame_length =
            u32::try_from(
                encoded_payload.len(),
            )
            .map_err(|_| {
                TcpTransportError::FrameTooLarge {
                    size: encoded_payload.len(),
                    max_size: MAX_FRAME_SIZE,
                }
            })?;

        stream.write_all(
            &frame_length.to_be_bytes(),
        ).await?;

        stream.write_all(
            &encoded_payload,
        ).await?;

        stream.flush().await?;

        Ok(())
    }

    /// Validates the protocol-level part of the handshake.
    ///
    /// The peer identity is validated separately:
    ///
    /// - outbound connections compare it with the expected peer,
    /// - inbound connections are validated by the connection
    ///   manager against the configured peer list.
    fn validate_handshake(
        handshake: &Handshake,
    ) -> Result<(), TcpTransportError> {

        // Case 1. Protocol version not matched
        if handshake.protocol_version
            != PROTOCOL_VERSION
        {
            return Err(
                TcpTransportError::UnsupportedProtocolVersion(
                    handshake.protocol_version,
                ),
            );
        }

        Ok(())
    }

    // ---------- Getters ----------

    /// Returns the local server identity.
    pub fn local_server_id(
        &self,
    ) -> ServerId {
        self.local_server_id
    }

    /// Returns the remote peer identity.
    pub fn peer_server_id(
        &self,
    ) -> ServerId {
        self.peer_server_id
    }
}