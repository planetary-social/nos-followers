use anyhow::{Context, Result};
use nostr_sdk::prelude::*;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::Sender;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{error, info};

pub async fn start_tcp_importer(
    task_tracker: TaskTracker,
    tcp_port: u16,
    event_tx: Sender<Box<Event>>,
    cancellation_token: CancellationToken,
    max_connections: usize,
) -> Result<()> {
    let address = SocketAddr::from(([0, 0, 0, 0], tcp_port));
    let listener = TcpListener::bind(&address)
        .await
        .context(format!("Error opening TCP listener on port {tcp_port}"))?;

    info!("Listening for TCP connections on {}", address);

    // Semaphore with the maximum number of allowed concurrent connections
    let semaphore = Arc::new(Semaphore::new(max_connections));

    task_tracker.spawn(async move {
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("TCP importer task cancelled");
                    break;
                }

                result = listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            if is_local_address(&addr) {
                                let tx = event_tx.clone();
                                let cancel_token = cancellation_token.clone();
                                let semaphore_clone = semaphore.clone();
                                let permit = semaphore_clone.acquire_owned().await;

                                match permit {
                                    Ok(permit) => {
                                        tokio::spawn(handle_connection_with_permit(stream, tx, cancel_token, permit));
                                    }
                                    Err(e) => {
                                        error!("Failed to acquire semaphore permit: {}", e);
                                    }
                                }
                            } else {
                                info!("Ignoring connection from non-local address: {}", addr);
                            }
                        }
                            Err(e) => {
                                error!("Failed to accept connection: {}", e);
                            }
                        }
                }
            }
        }

        info!("Shutting down TCP importer.");
    });

    Ok(())
}

fn is_local_address(addr: &SocketAddr) -> bool {
    match addr.ip() {
        IpAddr::V4(ipv4) => ipv4.is_loopback(), // Checks if in 127.0.0.0/8
        IpAddr::V6(ipv6) => ipv6.is_loopback(), // Checks if ::1
    }
}

async fn handle_connection_with_permit(
    stream: TcpStream,
    tx: Sender<Box<Event>>,
    cancellation_token: CancellationToken,
    _permit: OwnedSemaphorePermit,
) {
    handle_connection(stream, tx, cancellation_token).await;
    // Here the _permit is dropped
}

// Handle the incoming connection and read jsonl contact events
async fn handle_connection(
    stream: TcpStream,
    event_tx: Sender<Box<Event>>,
    cancellation_token: CancellationToken,
) {
    let reader = BufReader::new(stream);
    let mut lines = reader.lines();

    while let Ok(Some(line)) = lines.next_line().await {
        if cancellation_token.is_cancelled() {
            info!("Connection task cancelled");
            break;
        }

        match Event::from_json(&line) {
            Ok(event) => {
                if let Err(e) = event_tx.send(Box::new(event)) {
                    error!("Failed to send event: {}", e);
                }
            }
            Err(e) => {
                error!("Failed to deserialize Event: {}", e);
            }
        }
    }
}
