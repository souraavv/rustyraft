use std::net::SocketAddr;
use std::time::Duration;

use rustyraft::raft::client::ClientHandle;
use rustyraft::raft::ServerId;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

const HTTP_RETRY_DELAY: Duration = Duration::from_secs(1);
const HTTP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

// Keeps the HTTP server running and restarts it if it stops or fails.
pub struct HttpServer {
    address: SocketAddr,
    shutdown_tx: watch::Sender<bool>,
    leader_status_rx: watch::Receiver<Option<ServerId>>,
    client_handle: ClientHandle<String>,
    handle: JoinHandle<()>,
}

impl HttpServer {
    pub fn start(
        address: SocketAddr,
        listener: TcpListener,
        leader_status_rx: watch::Receiver<Option<ServerId>>,
        client_handle: ClientHandle<String>,
    ) -> Self {
        // watch is a Tokio channel for sharing the latest values;
        // true means shutdown.
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // We are handing over the receiver to the supervisor.
        let handle = tokio::spawn(
            supervise_http_server(
                address,
                Some(listener),
                shutdown_rx,
                leader_status_rx.clone(),
                client_handle.clone(),
            ),
        );

        Self {
            address,
            shutdown_tx,
            leader_status_rx,
            client_handle,
            handle,
        }
    }

    pub fn task(&mut self) -> &mut JoinHandle<()> {
        &mut self.handle
    }

    pub fn restart(&mut self) {
        let address = self.address;
        let shutdown_rx = self.shutdown_tx.subscribe();

        self.handle = tokio::spawn(
            supervise_http_server(
                address,
                None,
                shutdown_rx,
                self.leader_status_rx.clone(),
                self.client_handle.clone(),
            ),
        );
    }

    pub async fn shutdown(&mut self) {
        tracing::info!("Requesting HTTP server shutdown");

        // send_replace updates the stored value even when no receiver
        // currently exists.
        let _ = self.shutdown_tx.send_replace(true);

        // Wait for graceful shutdown, but don't wait indefinitely.
        match timeout(
            HTTP_SHUTDOWN_TIMEOUT,
            &mut self.handle,
        )
        .await
        {
            Ok(Ok(())) => {
                tracing::info!(
                    "HTTP server shut down gracefully"
                );
            }
            Ok(Err(error)) => {
                tracing::warn!(
                    %error,
                    "HTTP supervisor failed during shutdown"
                );
            }
            Err(_) => {
                tracing::warn!(
                    "HTTP shutdown timed out; aborting task"
                );

                self.handle.abort();

                // Wait for the aborted task to finish.
                let _ = (&mut self.handle).await;
            }
        }
    }
}

// Keeps the HTTP server running and restarts it if it stops or fails.
async fn supervise_http_server(
    address: SocketAddr,
    initial_listener: Option<TcpListener>,
    shutdown: watch::Receiver<bool>,
    leader_status_rx: watch::Receiver<Option<ServerId>>,
    client_handle: ClientHandle<String>,
) {
    let mut listener = initial_listener;

    loop {
        // Exit early if shutdown has been requested.
        // We don't want to restart the HTTP server during shutdown.
        if *shutdown.borrow() {
            tracing::info!("HTTP supervisor stopping");
            return;
        }

        let listener_to_serve = match listener.take() {
            Some(listener) => listener,

            None => {
                let bind_result = tokio::select! {
                    result = TcpListener::bind(address) => {
                        Some(result)
                    }
                    _ = wait_for_shutdown(shutdown.clone()) => {
                        None
                    }
                };

                match bind_result {
                    Some(Ok(listener)) => listener,

                    Some(Err(error)) => {
                        tracing::error!(
                            %address,
                            %error,
                            "Failed to bind HTTP server; retrying"
                        );

                        tokio::select! {
                            _ = sleep(HTTP_RETRY_DELAY) => {}
                            _ = wait_for_shutdown(shutdown.clone()) => {
                                return;
                            }
                        }

                        continue;
                    }

                    None => return,
                }
            }
        };

        tracing::info!(
            %address,
            "Starting HTTP server"
        );

        // Axum polls the shutdown future alongside the running server.
        // When shutdown is requested, it stops accepting new connections.
        let result = axum::serve(
            listener_to_serve,
            crate::api::create_router(
                leader_status_rx.clone(),
                client_handle.clone(),
            ),
        )
        .with_graceful_shutdown(
            wait_for_shutdown(shutdown.clone()),
        )
        .await;

        // Don't restart the server when shutdown was requested.
        if *shutdown.borrow() {
            tracing::info!(
                %address,
                "HTTP server stopped"
            );
            return;
        }

        match result {
            Ok(()) => {
                tracing::warn!(
                    %address,
                    "HTTP server exited; restarting"
                );
            }
            Err(error) => {
                tracing::error!(
                    %address,
                    %error,
                    "HTTP server failed; restarting"
                );
            }
        }

        // The supervisor waits before retrying. If shutdown occurs
        // during the delay, it exits instead of starting another server.
        tokio::select! {
            _ = sleep(HTTP_RETRY_DELAY) => {}
            _ = wait_for_shutdown(shutdown.clone()) => {
                return;
            }
        }
    }
}

async fn wait_for_shutdown(
    mut shutdown: watch::Receiver<bool>,
) {
    if *shutdown.borrow() {
        return;
    }

    // This completes when a new value is sent through the watch channel.
    let _ = shutdown.changed().await;
}