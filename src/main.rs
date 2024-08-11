mod config;
mod domain;
mod fetch_friendly_id;
mod follows_differ;
mod migrations;
mod relay_subscriber;
mod repo;
mod send_with_checks;
mod worker_pool;

use crate::config::Config;
use crate::fetch_friendly_id::fetch_friendly_id;
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
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use worker_pool::WorkerPool;

#[tokio::main]
async fn main() -> Result<()> {
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

    let repo = Arc::new(Repo::new(graph));
    let (follow_change_tx, mut follow_change_rx) = mpsc::channel::<FollowChange>(10000);
    let follows_differ = FollowsDiffer::new(repo.clone(), follow_change_tx.clone());
    let cancellation_token = CancellationToken::new();

    let worker_tracker =
        WorkerPool::start(8, event_rx, cancellation_token.clone(), follows_differ)?;

    let nostr_client = Arc::new(create_client());

    let nostr_client_clone = nostr_client.clone();
    let follow_change_task = tokio::spawn(async move {
        let mut verify = true;
        let mut fetch_nip05 = true;

        while let Some(follow_change) = follow_change_rx.recv().await {
            let current_seconds = match SystemTime::now().duration_since(UNIX_EPOCH) {
                Ok(duration) => duration.as_secs(),
                Err(e) => {
                    error!("Failed to get current time: {}", e);
                    continue;
                }
            };

            // Every 5 seconds we check if the channel buffer is almost full
            if current_seconds % 5 == 0 {
                let capacity = follow_change_tx.capacity();
                let max_capacity = follow_change_tx.max_capacity();

                // We stop verification for nip05 if the channel is 80% full
                verify = capacity > max_capacity / 5;

                // We stop fetching nip05 if the channel is 90% full. So in the
                // worse scenario we just convert the pubkey to npub and send
                // that
                fetch_nip05 = capacity > max_capacity / 10;
            }

            match follow_change {
                FollowChange::Followed {
                    at,
                    follower,
                    followee,
                } => {
                    let (friendly_follower, friendly_followee) = tokio::join!(
                        handle_friendly_id(
                            &repo,
                            &nostr_client_clone,
                            &follower,
                            verify,
                            fetch_nip05
                        ),
                        handle_friendly_id(
                            &repo,
                            &nostr_client_clone,
                            &followee,
                            verify,
                            fetch_nip05
                        ),
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
                        handle_friendly_id(
                            &repo,
                            &nostr_client_clone,
                            &follower,
                            verify,
                            fetch_nip05
                        ),
                        handle_friendly_id(
                            &repo,
                            &nostr_client_clone,
                            &followee,
                            verify,
                            fetch_nip05
                        ),
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

        info!("Follow change task ended");
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
// We cache 1_000_000 entries and each entry expires after 50 minutes
#[cached(
    ty = "TimedSizedCache<[u8; 32], String>",
    create = "{ TimedSizedCache::with_size_and_lifespan(1_000_000, 60 * 50) }",
    convert = r#"{ public_key.to_bytes() }"#
)]
async fn handle_friendly_id(
    repo: &Arc<Repo>,
    nostr_client: &Client,
    public_key: &PublicKey,
    verify: bool,
    fetch_nip05: bool,
) -> String {
    let friendly_id = fetch_friendly_id(nostr_client, public_key, verify, fetch_nip05).await;

    let pubkey = *public_key;
    let repo_follower = repo.clone();
    let friendly_follower_clone = friendly_id.clone();
    tokio::spawn(async move {
        if let Err(e) = repo_follower
            .add_friendly_id(&pubkey, &friendly_follower_clone)
            .await
        {
            error!("Failed to add friendly ID for follower: {}", e);
        }
    });

    friendly_id
}
