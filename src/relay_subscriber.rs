use anyhow::{bail, Result};
use async_trait::async_trait;
use nostr_sdk::prelude::*;
use std::marker::Send;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::Sender;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, error, info};

#[async_trait]
pub trait GetEventsOf: Send + Sync {
    async fn get_events_of(
        &self,
        filters: Vec<Filter>,
        timeout: Option<Duration>,
    ) -> Result<Vec<Event>, Error>;
}

#[async_trait]
impl GetEventsOf for Client {
    async fn get_events_of(
        &self,
        filters: Vec<Filter>,
        timeout: Option<Duration>,
    ) -> Result<Vec<Event>, Error> {
        self.get_events_of(filters, timeout).await
    }
}

pub fn create_client() -> Client {
    let opts = Options::default()
        .skip_disconnected_relays(true)
        .wait_for_send(false)
        .connection_timeout(Some(Duration::from_secs(5)))
        .send_timeout(Some(Duration::from_secs(5)))
        .wait_for_subscription(true);

    ClientBuilder::default().opts(opts).build()
}

pub fn start_nostr_subscription(
    task_tracker: TaskTracker,
    nostr_client: Arc<Client>,
    relays: Vec<String>,
    filters: Vec<Filter>,
    event_tx: Sender<Box<Event>>,
    cancellation_token: CancellationToken,
) {
    task_tracker.spawn(async move {
        for relay in relays {
            info!("Connecting to relay: {}", relay);
            if let Err(e) = nostr_client.add_relay(relay).await {
                bail!("Failed to add relay: {}", e);
            }
        }

        start_subscription(
            &nostr_client,
            &filters,
            event_tx.clone(),
            cancellation_token.clone(),
        )
        .await?;

        info!("Subscription ended");

        Ok(())
    });
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

    if let Err(e) = client.subscribe(filters.clone(), None).await {
        error!("Failed to subscribe: {}", e);
        cancellation_token.cancel();
        bail!("Failed to subscribe: {}", e);
    }

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
