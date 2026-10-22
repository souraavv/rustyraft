
use std::net::SocketAddr;
use std::time::Duration;

use rustyraft::raft::ServerId;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

const HTTP_RETRY_DELAY: Duration = Duration::from_secs(1);
const HTTP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

// Keeps the HTTP server running and restart if it stops or fails.
pub struct HttpServer {
    address: SocketAddr,
    shutdown_tx: watch::Sender<bool>,
    leader_status_rx: watch::Receiver<Option<ServerId>>,
    handle: JoinHandle<()>,
}

impl HttpServer {
    pub fn start(
        address: SocketAddr,
        listener: TcpListener,
        leader_status_rx: watch::Receiver<Option<ServerId>>,
    ) -> Self {
        // watch is a tokio channel - for sharing the latest values;
        // true means shutdown
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // We are handing over the reciever to the supervisor
        let handle = tokio::spawn(
            supervise_http_server(
                address,
                Some(listener),
                shutdown_rx,
                leader_status_rx.clone(),
            ),
        );

        Self {
            address,
            shutdown_tx,
            leader_status_rx,
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
            ),
        );
    }

    pub async fn shutdown(&mut self) {
        tracing::info!("Requesting HTTP server shutdown");

        // sending a shutdown true
        // send_replace() method is more safe than send() because 
        // even when no receiver currently exists it update the stored value
        // unlike send() which return an error if no receiver
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
                // waiting using temporary borrow
                let _ = (&mut self.handle).await;
            }
        }
    }
}

// Keeps the HTTP server running and restart if it stops or fails.
async fn supervise_http_server(
    address: SocketAddr,
    initial_listener: Option<TcpListener>,
    shutdown: watch::Receiver<bool>,
    leader_status_rx: watch::Receiver<Option<ServerId>>,
) {
    let mut listener = initial_listener;

    loop {
        // Safe code practice - exit early if you see shutdown event
        // We don't supervisor to restart the HTTP server when it fails 
        // if shutdown is triggered

        // borrow because we don't want to take the value out from the channel
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

        // Axum will poll the shudown future alongside its server working
        // When it become true we will stop accepting new connections
        let result = axum::serve(
            listener_to_serve,
            crate::api::create_router(leader_status_rx.clone()),
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

        // Two concurrent branches - one finish drop the other future handle
        // basically cancel that.

        // super visor is an infinite loop  
        // branch1. wait for next restart.. (live branch of sleep)
        // Branch 2. Shudown event - this serves different purpose, server
        // is already having graceful shudown handler and that return 
        // if shudown become true, here this select is in the case when
        // we dont' have a server and we are trying to create one.. 
        // while we are sleeping if shudown event occur no need to enter the
        // next iteration of loop
        tokio::select! {
            _ = sleep(HTTP_RETRY_DELAY) => {}
            _ = wait_for_shutdown(shutdown.clone()) => {
                // exit the supervisr
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

    // This await completes when a new value is sent through the watch channel.
    // The receiver is notified of the update, even if the new value is 
    // the same.
    let _ = shutdown.changed().await;
}



#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shutdown_stops_server_and_releases_port() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("should bind test listener");

        let address = listener
            .local_addr()
            .expect("should get listener address");

        let (_leader_tx, leader_status_rx) =
            watch::channel::<Option<ServerId>>(None);

        let mut server =
            HttpServer::start(address, listener, leader_status_rx);

        // Wait until the HTTP server is accepting connections.
        let deadline = tokio::time::Instant::now()
            + Duration::from_secs(2);

        loop {
            if let Ok(stream) = tokio::net::TcpStream::connect(address).await {
                drop(stream);
                break;
            }

            assert!(
                tokio::time::Instant::now() < deadline,
                "HTTP server did not start in time"
            );

            sleep(Duration::from_millis(10)).await;
        }

        server.shutdown().await;

        assert!(
            server.task().is_finished(),
            "HTTP supervisor should have stopped"
        );

        let rebound_listener = TcpListener::bind(address)
            .await
            .expect("port should be available after shutdown");

        drop(rebound_listener);
    }

    #[tokio::test]
    async fn shutdown_interrupts_bind_retry() {
        // Keep the address occupied so the supervisor's bind fails.
        let occupied_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("should bind occupied listener");

        let address = occupied_listener
            .local_addr()
            .expect("should get listener address");

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let (_leader_tx, leader_status_rx) =
            watch::channel::<Option<ServerId>>(None);

        let supervisor = tokio::spawn(supervise_http_server(
            address,
            None,
            shutdown_rx,
            leader_status_rx,
        ));

        // Allow the supervisor to encounter the bind failure.
        sleep(Duration::from_millis(50)).await;

        shutdown_tx.send_replace(true);

        timeout(Duration::from_millis(500), supervisor)
            .await
            .expect("supervisor should stop without waiting for retry")
            .expect("supervisor should not panic");

        drop(occupied_listener);
    }
}
