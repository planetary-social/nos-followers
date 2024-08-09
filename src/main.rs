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
    let follows_differ = FollowsDiffer::new(repo, follow_change_tx.clone());
    let cancellation_token = CancellationToken::new();

    let worker_tracker =
        WorkerPool::start(8, event_rx, cancellation_token.clone(), follows_differ)?;

    let nostr_client = Arc::new(create_client());

    let nostr_client_clone = nostr_client.clone();
    let follow_change_task = tokio::spawn(async move {
        while let Some(follow_change) = follow_change_rx.recv().await {
            // We stop verification for nip05 if the channel is 80% full
            let verify = follow_change_tx.capacity() > follow_change_tx.max_capacity() / 5;

            match follow_change {
                FollowChange::Followed {
                    at,
                    follower,
                    followee,
                } => {
                    let (friendly_follower, friendly_followee) = tokio::join!(
                        fetch_friendly_id(&nostr_client_clone, &follower, verify),
                        fetch_friendly_id(&nostr_client_clone, &followee, verify)
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
                        fetch_friendly_id(&nostr_client_clone, &follower, verify),
                        fetch_friendly_id(&nostr_client_clone, &followee, verify)
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
