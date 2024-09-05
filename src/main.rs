mod account_info;
mod config;
mod domain;
mod follow_change_handler;
mod google_pubsub_client;
mod http_server;
mod metrics;
mod migrations;
mod publisher;
mod rate_counter;
mod relay_subscriber;
mod repo;
mod worker_pool;

use config::{Config, Settings};
use core::panic;
use domain::{FollowChange, FollowsDiffer};
use follow_change_handler::FollowChangeHandler;
use http_server::HttpServer;
use migrations::apply_migrations;
use neo4rs::Graph;
use nostr_sdk::prelude::*;
use relay_subscriber::{create_client, start_nostr_subscription};
use repo::Repo;
use rustls::crypto::ring;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use worker_pool::WorkerPool;

#[tokio::main]
async fn main() -> Result<()> {
    info!("Follower server started");

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    ring::default_provider()
        .install_default()
        .expect("Failed to install ring crypto provider");

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
    let event_worker_pool_task_tracker = WorkerPool::start(
        "Differ",
        settings.diff_workers,
        settings.worker_timeout_secs,
        event_receiver,
        cancellation_token.clone(),
        follows_differ_worker,
    )?;

    info!("Starting follower change processing task");
    let follow_change_task_tracker = FollowChangeHandler::new(
        repo.clone(),
        shared_nostr_client.clone(),
        cancellation_token.clone(),
        &settings,
    )
    .await?;

    let follow_change_handle = WorkerPool::start(
        "FollowChangeHandler",
        settings.follow_change_workers,
        settings.worker_timeout_secs * 2,
        follow_change_sender.subscribe(),
        cancellation_token.clone(),
        follow_change_task_tracker,
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
        event_sender.clone(),
        cancellation_token.clone(),
    );

    let http_server = HttpServer::run(cancellation_token.clone());

    tokio::select! {
        result = nostr_sub => if let Err(e) = result {
            error!("Nostr subscription encountered an error: {:?}", e);
        },
        result = http_server => if let Err(e) = result {
            error!("HTTP server encountered an error: {:?}", e);
        },
        _ = follow_change_handle.wait() => {},
        _ = event_worker_pool_task_tracker.wait() => {},
        _ = cancellation_token.cancelled() => info!("Cancellation token cancelled"),
    }

    info!("Follower server stopped");
    Ok(())
}
