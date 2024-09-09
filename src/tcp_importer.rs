use anyhow::{Context, Result};
use nostr_sdk::prelude::*;
use std::net::SocketAddr;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::net::TcpListener;
use tokio::sync::broadcast::Sender;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{error, info};

pub async fn start_tcp_importer(
    task_tracker: TaskTracker,
    tcp_port: u16,
    event_tx: Sender<Box<Event>>,
    cancellation_token: CancellationToken,
) -> Result<()> {
    let address = SocketAddr::from(([0, 0, 0, 0], tcp_port));
    let listener = TcpListener::bind(&address)
        .await
        .context(format!("Error opening TCP listener on port {tcp_port}"))?;

    info!("Listening for tcp connections on {}", address);

    task_tracker.spawn(async move {
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("TCP importer task cancelled");
                    break;
                }

                Ok((stream, _)) = listener.accept() => {
                    let tx = event_tx.clone();
                    let cancel_token = cancellation_token.clone();

                    tokio::spawn(handle_connection(stream, tx, cancel_token));
                }
            }
        }

        info!("Shutting down TCP importer.");
    });

    Ok(())
}

// Handle the incoming connection and read jsonl contact events
async fn handle_connection(
    stream: tokio::net::TcpStream,
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
