use anyhow::{bail, Result};
use nostr_sdk::prelude::*;
use signal::unix::{signal, SignalKind};
use std::time::Duration;
use tokio::signal;
use tokio::sync::broadcast::Sender;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

pub fn create_client() -> Client {
    let opts = Options::new()
        .skip_disconnected_relays(true)
        .wait_for_send(false)
        .connection_timeout(Some(Duration::from_secs(5)))
        .send_timeout(Some(Duration::from_secs(5)))
        .wait_for_subscription(true);

    ClientBuilder::new().opts(opts).build()
}

pub async fn start_nostr_subscription(
    nostr_client: Client,
    relays: Vec<String>,
    filters: Vec<Filter>,
    event_tx: Sender<Box<Event>>,
    cancellation_token: CancellationToken,
) -> Result<()> {
    for relay in relays {
        info!("Connecting to relay: {}", relay);
        if let Err(e) = nostr_client.add_relay(relay).await {
            bail!("Failed to add relay: {}", e);
        }
    }

    let token_clone = cancellation_token.clone();
    tokio::spawn(async move {
        if let Err(e) = cancel_on_stop_signals(token_clone).await {
            error!("Failed to listen stop signals: {}", e);
        }
    });

    start_subscription(
        &nostr_client,
        &filters,
        event_tx.clone(),
        cancellation_token.clone(),
    )
    .await?;

    info!("Subscription ended");

    Ok(())
}

async fn start_subscription(
    client: &Client,
    filters: &Vec<Filter>,
    event_tx: Sender<Box<Event>>,
    cancellation_token: CancellationToken,
) -> Result<()> {
    client.connect().await;

    let token_clone = cancellation_token.clone();
    let client_clone = client.clone();
    tokio::spawn(async move {
        token_clone.cancelled().await;
        debug!("Cancelling relay subscription worker");
        client_clone.unsubscribe_all().await;
        if let Err(e) = client_clone.shutdown().await {
            error!("Failed to shutdown client: {}", e);
        }
    });

    info!("Subscribing to {:?}", &filters);

    client.subscribe(filters.clone(), None).await?;

    client
        .handle_notifications(|notification| async {
            if cancellation_token.is_cancelled() {
                return Ok(true);
            }

            if let RelayPoolNotification::Event { event, .. } = notification {
                debug!("Received event: {}", event.id);
                if let Err(e) = event_tx.send(event) {
                    error!("Failed to send nostr event: {}", e);
                    cancellation_token.cancel();
                    return Ok(true);
                }
            }

            // True would exit from the loop
            Ok(false)
        })
        .await?;

    Ok(())
}

// Listen to ctrl-c, terminate and cancellation token
async fn cancel_on_stop_signals(cancellation_token: CancellationToken) -> Result<()> {
    #[cfg(unix)]
    let terminate = async {
        signal(SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = cancellation_token.cancelled() => {
            info!("Starting graceful termination, from cancellation token");
        },
        _ = signal::ctrl_c() => {
            info!("Starting graceful termination, from ctrl-c");
        },
        _ = terminate => {
            info!("Starting graceful termination, from terminate signal");
        },
    }

    cancellation_token.cancel();

    info!("Waiting 3 seconds before exiting");
    tokio::time::sleep(Duration::from_secs(3)).await;

    Ok(())
}
