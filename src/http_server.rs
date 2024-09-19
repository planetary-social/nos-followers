mod handlers;
mod router;
mod trust_policy;

use crate::{
    config::Settings,
    relay_subscriber::GetEventsOf,
    repo::{Recommendation, RepoTrait},
};
use anyhow::{Context, Result};
use axum::Router;
use moka::future::Cache;
use router::create_router;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{error, info};

pub struct AppState<T, U>
where
    T: RepoTrait,
    U: GetEventsOf,
{
    pub repo: Arc<T>,
    pub nostr_client: Arc<U>,
    pub recommendation_cache: Cache<String, Vec<Recommendation>>,
    pub trust_cache: Cache<String, bool>,
}

impl<T, U> AppState<T, U>
where
    T: RepoTrait + 'static,
    U: GetEventsOf + 'static,
{
    pub fn new(repo: Arc<T>, nostr_client: Arc<U>) -> Self {
        let recommendation_cache = Cache::builder()
            .time_to_live(Duration::from_secs(86400)) // 1 day
            .max_capacity(4000)
            .build();

        let trust_cache = Cache::builder()
            .time_to_live(Duration::from_secs(86400)) // 1 day
            .max_capacity(4000)
            .build();

        Self {
            repo,
            nostr_client,
            recommendation_cache,
            trust_cache,
        }
    }
}

pub struct HttpServer;
impl HttpServer {
    pub fn start<T, U>(
        task_tracker: TaskTracker,
        settings: &Settings,
        repo: Arc<T>,
        nostr_client: Arc<U>,
        cancellation_token: CancellationToken,
    ) -> Result<()>
    where
        T: RepoTrait + 'static,
        U: GetEventsOf + 'static,
    {
        let state = Arc::new(AppState::new(repo, nostr_client));
        let router = create_router(state, settings)?;

        start_http_server(task_tracker, settings.http_port, router, cancellation_token);

        Ok(())
    }
}

fn start_http_server(
    task_tracker: TaskTracker,
    http_port: u16,
    router: Router,
    cancellation_token: CancellationToken,
) {
    task_tracker.spawn(async move {
        let addr = SocketAddr::from(([0, 0, 0, 0], http_port));
        let Ok(listener) = tokio::net::TcpListener::bind(addr).await else {
            error!("Failed to bind to address: {}", addr);
            cancellation_token.cancel();
            return;
        };

        let token_clone = cancellation_token.clone();
        let server_future = tokio::spawn(async {
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(shutdown_hook(token_clone))
            .await
            .context("Failed to start HTTP server")
        });

        await_shutdown(cancellation_token, server_future).await;
    });
}

async fn await_shutdown(
    cancellation_token: CancellationToken,
    server_future: tokio::task::JoinHandle<Result<()>>,
) {
    cancellation_token.cancelled().await;
    info!("Shutdown signal received.");
    match timeout(Duration::from_secs(5), server_future).await {
        Ok(_) => info!("HTTP service exited successfully."),
        Err(e) => info!("HTTP service exited after timeout: {}", e),
    }
}

async fn shutdown_hook(cancellation_token: CancellationToken) {
    cancellation_token.cancelled().await;
    info!("Exiting the process");
}
