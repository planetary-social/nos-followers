mod config;
mod domain;
mod fetch_friendly_id;
mod follows_differ;
mod handle_follow_changes;
mod migrations;
mod relay_subscriber;
mod repo;
mod send_with_checks;
mod worker_pool;

use crate::config::Config;
use crate::follows_differ::FollowChange;
use follows_differ::FollowsDiffer;
use handle_follow_changes::handle_follow_changes;
use migrations::apply_migrations;
use neo4rs::Graph;
use nostr_sdk::prelude::*;
use relay_subscriber::{create_client, start_nostr_subscription};
use repo::Repo;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use worker_pool::WorkerPool;

#[tokio::main]
async fn main() -> Result<()> {
    info!("Follower server started");
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let config = Config::new("config")?;
    let relay = config.get_by_key::<String>("relay")?;
    let uri = config.get_by_key::<String>("NEO4J_URI")?;
    let user = config.get_by_key::<String>("NEO4J_USER")?;
    let password = config.get_by_key::<String>("NEO4J_PASSWORD")?;

    info!("Initializing repository at {}", uri);
    let graph = Graph::new(uri, user, password).await?;
    apply_migrations(&graph).await?;
    let repo = Arc::new(Repo::new(graph));

    info!("Initializing workers for follower list diff calculation");
    let (follow_change_sender, follow_change_receiver) = mpsc::channel::<FollowChange>(100000);
    let follows_differ_worker = FollowsDiffer::new(repo.clone(), follow_change_sender.clone());
    let cancellation_token = CancellationToken::new();
    let (event_sender, event_receiver) = mpsc::channel::<Box<Event>>(100);
    let worker_pool_handle = WorkerPool::start(
        8,
        event_receiver,
        cancellation_token.clone(),
        follows_differ_worker,
    )?;

    info!("Starting follower change processing task");
    let shared_nostr_client = Arc::new(create_client());
    let follow_change_handler_task = handle_follow_changes(
        shared_nostr_client.clone(),
        repo.clone(),
        follow_change_sender,
        follow_change_receiver,
    );

    info!("Subscribing to kind 3 events");
    let five_minutes_ago = Timestamp::now() - 60 * 5;
    let filters = vec![Filter::new()
        .since(five_minutes_ago)
        .kind(Kind::ContactList)];

    start_nostr_subscription(
        shared_nostr_client,
        &[relay],
        filters,
        event_sender,
        cancellation_token.clone(),
    )
    .await?;

    info!("Terminating");
    worker_pool_handle.wait().await;
    follow_change_handler_task.await?;

    Ok(())
}
