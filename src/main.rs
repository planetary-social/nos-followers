mod config;
mod domain;
mod fetch_friendly_id;
mod follow_change_handler;
mod follows_differ;
mod google_publisher;
mod migrations;
mod relay_subscriber;
mod repo;
mod send_with_checks;
mod worker_pool;

use crate::config::Config;
use crate::domain::follow_change::FollowChange;
use follow_change_handler::FollowChangeHandler;
use follows_differ::FollowsDiffer;
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

    // TODO: Use struct to hold configuration
    let config = Config::new("config")?;
    let relay = config.get_by_key::<String>("relay")?;
    let uri = config.get_by_key::<String>("NEO4J_URI")?;
    let user = config.get_by_key::<String>("NEO4J_USER")?;
    let password = config.get_by_key::<String>("NEO4J_PASSWORD")?;
    let event_channnel_size = config.get_by_key::<usize>("event_channel_size")?;
    let event_workers = config.get_by_key::<usize>("event_workers")?;
    let follow_change_channel_size = config.get_by_key::<usize>("follow_change_channel_size")?;
    let follow_change_workers = config.get_by_key::<usize>("follow_change_workers")?;
    let worker_timeout_secs = config.get_by_key::<u64>("worker_timeout_secs")?;

    info!("Initializing repository at {}", uri);
    let graph = Graph::new(uri, user, password).await?;
    apply_migrations(&graph).await?;
    let repo = Arc::new(Repo::new(graph));

    info!("Initializing workers for follower list diff calculation");
    let (follow_change_sender, follow_change_receiver) =
        mpsc::channel::<FollowChange>(follow_change_channel_size);
    let follows_differ_worker = FollowsDiffer::new(repo.clone(), follow_change_sender);
    let cancellation_token = CancellationToken::new();
    let (event_sender, event_receiver) = mpsc::channel::<Box<Event>>(event_channnel_size);
    let event_worker_pool_handle = WorkerPool::start(
        event_workers,
        worker_timeout_secs,
        event_receiver,
        cancellation_token.clone(),
        follows_differ_worker,
    )?;

    info!("Starting follower change processing task");
    let shared_nostr_client = create_client();
    let follow_change_handler = FollowChangeHandler::new(
        repo.clone(),
        shared_nostr_client.clone(),
        cancellation_token.clone(),
    )
    .await?;

    let follow_change_handler_task = WorkerPool::start(
        follow_change_workers,
        worker_timeout_secs,
        follow_change_receiver,
        cancellation_token.clone(),
        follow_change_handler,
    )?;

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

    info!("Finished Nostr subscription");

    event_worker_pool_handle.wait().await;
    info!("Finished Nostr event worker pool");

    follow_change_handler_task.wait().await;
    info!("Finished follow change worker pool");

    info!("Follower server stopped");
    Ok(())
}
