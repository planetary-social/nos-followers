mod config;
mod domain;
mod follow_change_handler;
mod follows_differ;
mod google_publisher;
mod google_pubsub_client;
mod http_server;
mod migrations;
mod refresh_friendly_id;
mod relay_subscriber;
mod repo;
mod unique_follow_changes;
mod worker_pool;

use config::{Config, Settings};
use domain::follow_change::FollowChange;
use follow_change_handler::FollowChangeHandler;
use follows_differ::FollowsDiffer;
use http_server::HttpServer;
use migrations::apply_migrations;
use neo4rs::Graph;
use nostr_sdk::prelude::*;
use relay_subscriber::{create_client, start_nostr_subscription};
use repo::Repo;

use core::panic;
use std::sync::Arc;
use tokio::sync::broadcast;
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

    // Load the configuration
    let config = Config::new("config")?;
    let settings = config.get::<Settings>()?;

    info!("Initializing repository at {}", settings.neo4j_uri);
    let graph = Graph::new(
        &settings.neo4j_uri,
        &settings.neo4j_user,
        &settings.neo4j_password,
    )
    .await?;
    apply_migrations(&graph).await?;
    let repo = Arc::new(Repo::new(graph));

    info!("Initializing workers for follower list diff calculation");
    let shared_nostr_client = Arc::new(create_client());
    let (follow_change_sender, _) =
        broadcast::channel::<FollowChange>(settings.follow_change_channel_size);
    let follows_differ_worker = FollowsDiffer::new(
        repo.clone(),
        shared_nostr_client.clone(),
        follow_change_sender.clone(),
    );
    let cancellation_token = CancellationToken::new();
    let (event_sender, event_receiver) =
        broadcast::channel::<Box<Event>>(settings.event_channel_size);
    let event_worker_pool_handle = WorkerPool::start(
        settings.event_workers,
        settings.worker_timeout_secs,
        event_receiver,
        cancellation_token.clone(),
        follows_differ_worker,
    )?;

    info!("Starting follower change processing task");
    let follow_change_handler = FollowChangeHandler::new(
        repo.clone(),
        shared_nostr_client.clone(),
        cancellation_token.clone(),
        &settings,
    )
    .await?;

    let follow_change_handler_task = WorkerPool::start(
        settings.follow_change_workers,
        settings.worker_timeout_secs * 2,
        follow_change_sender.subscribe(),
        cancellation_token.clone(),
        follow_change_handler,
    )?;

    info!("Subscribing to kind 3 events");
    let five_minutes_ago = Timestamp::now() - 60 * 5;
    let filters = vec![Filter::new()
        .since(five_minutes_ago)
        .kind(Kind::ContactList)];

    let nostr_sub = start_nostr_subscription(
        shared_nostr_client.clone(),
        [settings.relay].into(),
        filters,
        event_sender,
        cancellation_token.clone(),
    );

    let http_server = HttpServer::run(cancellation_token.clone());

    tokio::select! {
        _ = nostr_sub => info!("Nostr subscription ended"),
        _ = http_server => info!("HTTP server ended"),
        _ = cancellation_token.cancelled() => info!("Cancellation token cancelled"),
    }

    info!("Finished Nostr subscription");

    event_worker_pool_handle.wait().await;
    info!("Finished Nostr event worker pool");

    follow_change_handler_task.wait().await;
    info!("Finished follow change worker pool");

    info!("Follower server stopped");
    Ok(())
}
