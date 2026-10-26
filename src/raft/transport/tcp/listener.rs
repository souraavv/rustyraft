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

// Delay before retrying accept after a listener error. The delay doubles
// after consecutive failures and is capped to avoid a tight retry loop.
const INITIAL_ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(100);
const MAX_ACCEPT_RETRY_DELAY: Duration = Duration::from_secs(5);
const MAX_ACCEPT_RETRY_BACKOFF_EXPONENT: u32 = 6;

/// Calculates a bounded delay before retrying a failed accept.
fn accept_retry_delay(consecutive_errors: u32) -> Duration {
    let exponent =
        consecutive_errors.min(MAX_ACCEPT_RETRY_BACKOFF_EXPONENT);
    let multiplier = 2u32.pow(exponent);

    (INITIAL_ACCEPT_RETRY_DELAY * multiplier).min(MAX_ACCEPT_RETRY_DELAY)
}

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
            let mut consecutive_accept_errors = 0u32;

            loop {
                // Step 1. Ask listener to accept the incoming
                // client connection
                let (stream, address) =
                    match self.listener.accept().await {
                        Ok(accepted) => {
                            consecutive_accept_errors = 0;
                            accepted
                        }
                        Err(error) => {
                            let retry_delay =
                                accept_retry_delay(consecutive_accept_errors);
                            consecutive_accept_errors =
                                consecutive_accept_errors.saturating_add(1);

                            tracing::warn!(
                                ?error,
                                ?retry_delay,
                                consecutive_errors =
                                    consecutive_accept_errors,
                                "Failed to accept TCP connection; retrying with backoff"
                            );

                            // Aborting this listener task during shutdown
                            // also cancels the pending retry sleep.
                            tokio::time::sleep(retry_delay).await;
                            continue;
                        }
                    };

                let local_server_id = self.local_server_id;
                let sender = established_sender.clone();

                // Step2. Create the TcpConnection based on the stream
                // Notice we again spawned here.. because we just want our
                // listener to be block free. As soon as listener got a client
                // connection and  have the stream, we will let to the 
                // handshake part again in the non blocking way
                tokio::spawn(async move {

                    // Attempt the handshake in a give timeout
                    let result = timeout(
                        HANDSHAKE_TIMEOUT,
                        TcpConnection::accept(
                            stream,
                            local_server_id,
                        ),
                    )
                    .await;

                    // Post handshake the result is either an error in 
                    // hanshake or Ok.
                    // If it is Ok
                    match result {
                        Ok(Ok(connection)) => {
                            if sender.send(connection).await.is_err() {
                                tracing::debug!(
                                    %address,
                                    "Closed before connection registration"
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

#[cfg(test)]
mod tests {
    use super::{
        accept_retry_delay,
        MAX_ACCEPT_RETRY_DELAY,
    };
    use std::time::Duration;

    #[test]
    fn accept_retry_delay_doubles_after_consecutive_errors() {
        assert_eq!(
            accept_retry_delay(0),
            Duration::from_millis(100)
        );
        assert_eq!(
            accept_retry_delay(1),
            Duration::from_millis(200)
        );
        assert_eq!(
            accept_retry_delay(2),
            Duration::from_millis(400)
        );
        assert_eq!(
            accept_retry_delay(5),
            Duration::from_millis(3200)
        );
    }

    #[test]
    fn accept_retry_delay_stays_within_the_configured_cap() {
        assert_eq!(
            accept_retry_delay(6),
            MAX_ACCEPT_RETRY_DELAY
        );
        assert_eq!(
            accept_retry_delay(u32::MAX),
            MAX_ACCEPT_RETRY_DELAY
        );
    }
}
