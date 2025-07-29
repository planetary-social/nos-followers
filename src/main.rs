use anyhow::{Context, Result};
use neo4rs::Graph;
use nos_followers::{
    config::{Config, Settings},
    domain::{FollowChange, FollowsDiffer},
    follow_change_handler::FollowChangeHandler,
    http_server::HttpServer,
    migrations::apply_migrations,
    recommendation_queue::RecommendationQueue,
    relay_subscriber::{create_client, start_nostr_subscription},
    repo::{Repo, RepoTrait},
    scheduler::start_scheduler,
    tcp_importer::start_tcp_importer,
    vanish_subscriber::{start_vanish_subscriber, RedisClient},
    worker_pool::WorkerPool,
};
use nostr_sdk::prelude::*;
use rustls::crypto::ring;
use signal::unix::{signal, SignalKind};
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::signal;
use tokio::sync::broadcast;
use tokio::time::Duration;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{error, info};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    ring::default_provider()
        .install_default()
        .expect("Failed to install ring crypto provider");

    info!("Follower server started");

    let config = Config::new("config")?;
    let settings = config.get::<Settings>()?;

    if let Err(e) = start_server(settings).await {
        error!("Failed to start the server: {:#}", e);
    }

    Ok(())
}

async fn start_server(settings: Settings) -> Result<()> {
    info!("Initializing repository at {}", settings.neo4j_uri);
    let graph = Graph::new(
        &settings.neo4j_uri,
        &settings.neo4j_user,
        &settings.neo4j_password,
    )
    .await
    .context("Failed to connect to Neo4j")?;

    apply_migrations(&graph)
        .await
        .context("Failed applying migrations")?;
    let repo = Arc::new(Repo::new(graph));

    repo.log_neo4j_details().await?;
    let shared_nostr_client = Arc::new(create_client());
    let task_tracker = TaskTracker::new();
    let cancellation_token = CancellationToken::new();

    // Create recommendation queue and cache
    info!("Initializing recommendation queue");
    let recommendation_cache = moka::future::Cache::builder()
        .time_to_live(std::time::Duration::from_secs(86400)) // 1 day
        .max_capacity(4000)
        .build();

    let (recommendation_queue, recommendation_receiver) = RecommendationQueue::new(
        repo.clone(),
        recommendation_cache,
        1000, // channel size
    );
    let recommendation_queue = Arc::new(recommendation_queue);

    // Start recommendation queue worker
    recommendation_queue.start_worker(
        task_tracker.clone(),
        recommendation_receiver,
        cancellation_token.clone(),
    );

    // Leave the http server at the top so the health endpoint is available quickly
    info!("Starting HTTP server at port {}", settings.http_port);
    HttpServer::start(
        task_tracker.clone(),
        &settings,
        repo.clone(),
        shared_nostr_client.clone(),
        recommendation_queue,
        cancellation_token.clone(),
    )?;

    info!("Initializing workers for follower list diff calculation");
    let (follow_change_sender, _) =
        broadcast::channel::<Box<FollowChange>>(settings.follow_change_channel_size.get());
    let follows_differ_worker = FollowsDiffer::new(
        repo.clone(),
        shared_nostr_client.clone(),
        follow_change_sender.clone(),
    );

    let (event_sender, event_receiver) =
        broadcast::channel::<Box<Event>>(settings.event_channel_size.get());
    WorkerPool::start(
        task_tracker.clone(),
        "Differ",
        settings.diff_workers,
        settings.worker_timeout_secs,
        event_receiver,
        cancellation_token.clone(),
        follows_differ_worker,
    );

    info!("Starting follower change processing task");
    let follow_change_worker = FollowChangeHandler::new(
        repo.clone(),
        shared_nostr_client.clone(),
        cancellation_token.clone(),
        &settings,
    )
    .await
    .context("Failed starting the follow change processing task")?;

    WorkerPool::start(
        task_tracker.clone(),
        "FollowChangeHandler",
        settings.follow_change_workers,
        NonZeroUsize::new(settings.worker_timeout_secs.get() * 2).unwrap(),
        follow_change_sender.subscribe(),
        cancellation_token.clone(),
        follow_change_worker,
    );

    info!("Subscribing to kind 3 events");
    let five_minutes_ago = Timestamp::now() - 60 * 5;
    let filters = vec![Filter::new()
        .since(five_minutes_ago)
        .kind(Kind::ContactList)];

    start_nostr_subscription(
        task_tracker.clone(),
        shared_nostr_client.clone(),
        [settings.relay.clone()].into(),
        filters,
        event_sender.clone(),
        cancellation_token.clone(),
    );

    info!("Starting tcp listener for jsonl contact streams");
    start_tcp_importer(
        task_tracker.clone(),
        settings.tcp_importer_port,
        event_sender,
        cancellation_token.clone(),
        5,
    )
    .await?;

    info!("Starting scheduler");
    start_scheduler(
        task_tracker.clone(),
        repo.clone(),
        cancellation_token.clone(),
        &settings,
    )
    .await
    .context("Failed starting the scheduler")?;

    // TODO: Now that we have redis we would use it to restore pending
    // notifications between restarts and integrate it with cached crate
    let redis_client = RedisClient::new(&settings.redis_url);

    info!("Starting vanish subscriber");
    start_vanish_subscriber(
        task_tracker.clone(),
        redis_client,
        repo.clone(),
        cancellation_token.clone(),
    );

    tokio::spawn(async move {
        if let Err(e) = cancel_on_stop_signals(cancellation_token).await {
            error!("Failed to listen stop signals: {}", e);
        }
    });

    info!("Server is ready");
    task_tracker.close();
    task_tracker.wait().await;

    info!("Follower server stopped");
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
