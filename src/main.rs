mod config;
mod domain;
mod follows_differ;
mod migrations;
mod relay_subscriber;
mod repo;
mod send_with_checks;
mod worker_pool;

use crate::config::Config;
use anyhow::Result;
use cached::proc_macro::cached;
use cached::TimedSizedCache;
use follows_differ::FollowChange;
use follows_differ::FollowsDiffer;
use migrations::apply_migrations;
use neo4rs::Graph;
use nostr_sdk::prelude::*;
use relay_subscriber::{create_client, start_nostr_subscription};
use repo::Repo;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use worker_pool::WorkerPool;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    info!("Starting followers server");
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let config = Config::new("config")?;
    let uri = config.get_by_key::<String>("NEO4J_URI")?;
    let user = config.get_by_key::<String>("NEO4J_USER")?;
    let password = config.get_by_key::<String>("NEO4J_PASSWORD")?;
    info!("Connecting to Neo4j at {}", uri);
    let graph = Graph::new(uri, user, password).await?;
    apply_migrations(&graph).await?;

    let (event_tx, event_rx) = mpsc::channel::<Box<Event>>(100);

    let repo = Repo::new(graph);
    let (follow_change_tx, mut follow_change_rx) = mpsc::channel::<FollowChange>(10000);
    let follows_differ = FollowsDiffer::new(repo, follow_change_tx);
    let cancellation_token = CancellationToken::new();

    let worker_tracker =
        WorkerPool::start(8, event_rx, cancellation_token.clone(), follows_differ)?;

    let nostr_client = Arc::new(create_client());

    let nostr_client_clone = nostr_client.clone();
    let follow_change_task = tokio::spawn(async move {
        while let Some(follow_change) = follow_change_rx.recv().await {
            match follow_change {
                FollowChange::Followed {
                    at,
                    follower,
                    followee,
                } => {
                    let (friendly_follower, friendly_followee) = tokio::join!(
                        fetch_friendly_id(&nostr_client_clone, &follower),
                        fetch_friendly_id(&nostr_client_clone, &followee)
                    );

                    debug!(
                        "Followed: {}({}) -> {}({}) at {}",
                        follower,
                        friendly_follower,
                        followee,
                        friendly_followee,
                        at.to_human_datetime()
                    );
                    // TODO: Send to pubsub
                }
                FollowChange::Unfollowed {
                    at,
                    follower,
                    followee,
                } => {
                    let (friendly_follower, friendly_followee) = tokio::join!(
                        fetch_friendly_id(&nostr_client_clone, &follower),
                        fetch_friendly_id(&nostr_client_clone, &followee)
                    );

                    debug!(
                        "Unfollowed: {}({}) -> {}({}) at {}",
                        follower,
                        friendly_follower,
                        followee,
                        friendly_followee,
                        at.to_human_datetime()
                    )
                    // TODO: Send to pubsub
                }
            }
        }
    });

    let relay = config.get_by_key::<String>("relay")?;
    let five_minutes_ago = Timestamp::now() - 60 * 5;
    let filters = vec![Filter::new()
        .since(five_minutes_ago)
        .kind(Kind::ContactList)];

    start_nostr_subscription(
        nostr_client,
        &[relay],
        filters,
        event_tx,
        cancellation_token.clone(),
    )
    .await?;
    worker_tracker.wait().await;
    follow_change_task.await?;

    Ok(())
}

// Try to return an identifier that is not the public key.
// Define a cached async function with a 5-minute expiration
// We cache 1000000 entries and each entry expires after 50 minutes
#[cached(
    ty = "TimedSizedCache<String, String>",
    create = "{ TimedSizedCache::with_size_and_lifespan(1000000, 3000) }",
    convert = r#"{ public_key.to_hex() }"#
)]
async fn fetch_friendly_id(client: &Client, public_key: &PublicKey) -> String {
    let npub_or_pubkey = match public_key.to_bech32() {
        Ok(npub) => npub,
        Err(_) => return public_key.to_hex(),
    };

    let Some(metadata) = client.metadata(*public_key).await.ok() else {
        return npub_or_pubkey;
    };

    let name_or_npub_or_pubkey = metadata.name.unwrap_or(npub_or_pubkey);

    if let Some(nip05_value) = metadata.nip05 {
        let Ok(verified) = nip05::verify(public_key, &nip05_value, None).await else {
            return name_or_npub_or_pubkey;
        };

        if !verified {
            return name_or_npub_or_pubkey;
        }

        return nip05_value;
    }

    name_or_npub_or_pubkey
}
