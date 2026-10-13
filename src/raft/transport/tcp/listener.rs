//! TCP listener for incoming Raft peer connections.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::raft::state::ServerId;

use super::connection::TcpConnection;
use super::error::TcpTransportError;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

pub struct TcpListenerService {
    /// Identity of the local Raft server.
    local_server_id: ServerId,

    /// The TCP listener.
    listener: TcpListener,
}

impl TcpListenerService {
    /// Creates a TCP listener bound to the given address.
    pub async fn bind(
        local_server_id: ServerId,
        address: SocketAddr,
    ) -> Result<Self, TcpTransportError> {
        let listener = TcpListener::bind(address).await?;

        Ok(Self {
            local_server_id,
            listener,
        })
    }

    pub fn local_addr(
        &self,
    ) -> Result<SocketAddr, TcpTransportError> {
        Ok(self.listener.local_addr()?)
    }

    /// Accepts TCP connections independently of the transport.
    ///
    /// Each connection completes its handshake in a separate task.
    /// Established connections are published through a bounded channel.
    /// The connection manager remains responsible for registration.
    pub fn spawn(
        self,
        established_sender: mpsc::Sender<TcpConnection>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {

                // Step 1. Ask listener to accept the incoming
                // client connection
                let (stream, address) =
                    match self.listener.accept().await {
                        Ok(accepted) => accepted,
                        Err(error) => {
                            tracing::warn!(
                                ?error,
                                "Failed to accept TCP connection"
                            );
                            continue;
                        }
                    };

                let local_server_id = self.local_server_id;
                let sender = established_sender.clone();

                // Step2. Create the TcpConnection based on the stream
                tokio::spawn(async move {
                    // Attempt the handshake in a give timeout
                    // if done else boom
                    let result = timeout(
                        HANDSHAKE_TIMEOUT,
                        TcpConnection::accept(
                            stream,
                            local_server_id,
                        ),
                    )
                    .await;

                    match result {
                        Ok(Ok(connection)) => {
                            if sender.send(connection).await.is_err() {
                                tracing::debug!(
                                    %address,
                                    "Transport closed before connection registration"
                                );
                            }
                        }
                        Ok(Err(error)) => {
                            tracing::warn!(
                                %address,
                                ?error,
                                "TCP handshake failed"
                            );
                        }
                        Err(_) => {
                            tracing::warn!(
                                %address,
                                "TCP handshake timed out"
                            );
                        }
                    }
                });
            }
        })
    }
}