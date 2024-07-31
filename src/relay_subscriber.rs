use anyhow::{bail, Result};
use futures::future::join_all;
use nostr_sdk::prelude::*;
use std::time::Duration;
use tokio::signal;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info};

pub async fn start_nostr_subscription(
    relays: &[String],
    filters: Vec<Filter>,
    event_tx: Sender<Box<Event>>,
    cancellation_token: CancellationToken,
) -> Result<()> {
    let opts = Options::new()
        .skip_disconnected_relays(true)
        .wait_for_send(false)
        .connection_timeout(Some(Duration::from_secs(5)))
        .send_timeout(Some(Duration::from_secs(5)))
        .wait_for_subscription(true);

    let nostr_client = ClientBuilder::new().opts(opts).build();

    for relay in relays {
        if let Err(e) = nostr_client.add_relay(relay).await {
            bail!("Failed to add relay: {}", e);
        }
    }

    let token_clone = cancellation_token.clone();
    tokio::spawn(async move {
        if let Err(e) = listen_stop_signals(token_clone).await {
            error!("Failed to listen stop signals: {}", e);
        }
    });

    let tracker = TaskTracker::new();

    while all_disconnected(&nostr_client).await && !cancellation_token.is_cancelled() {
        start_subscription(
            &nostr_client,
            &filters,
            event_tx.clone(),
            cancellation_token.clone(),
        )
        .await?;

        if !cancellation_token.is_cancelled() {
            info!("Subscription ended, waiting 5 seconds before reconnecting");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    tracker.wait().await;
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

            if let RelayPoolNotification::Event {
                event: contacts_event,
                ..
            } = notification
            {
                debug!("Received event: {}", contacts_event.id);
                event_tx.send(contacts_event).await?;
            }

            // True would exit from the loop
            Ok(false)
        })
        .await?;

    Ok(())
}

// Listen to ctrl-c, terminate and cancellation token
async fn listen_stop_signals(cancellation_token: CancellationToken) -> Result<()> {
    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
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

// Check if all relays disconnected
async fn all_disconnected(client: &Client) -> bool {
    let relays = client.pool().relays().await;
    let futures: Vec<_> = relays.values().map(|relay| relay.is_connected()).collect();
    let results = join_all(futures).await;
    results.iter().all(|&is_connected| !is_connected)
}
