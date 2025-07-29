mod handlers;
mod router;
mod trust_policy;

use crate::{
    config::Settings,
    recommendation_queue::RecommendationQueue,
    relay_subscriber::GetEventsOf,
    repo::{Recommendation, RepoTrait},
};
use anyhow::{Context, Result};
use axum::Router;
use axum_server::Handle;
use moka::future::Cache;
use router::create_router;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::info;

pub struct AppState<T, U>
where
    T: RepoTrait,
    U: GetEventsOf,
{
    pub repo: Arc<T>,
    pub nostr_client: Arc<U>,
    pub recommendation_cache: Cache<String, Vec<Recommendation>>,
    pub trust_cache: Cache<String, bool>,
    pub recommendation_queue: Arc<RecommendationQueue<T>>,
}

impl<T, U> AppState<T, U>
where
    T: RepoTrait + 'static,
    U: GetEventsOf + 'static,
{
    pub fn new(
        repo: Arc<T>,
        nostr_client: Arc<U>,
        recommendation_queue: Arc<RecommendationQueue<T>>,
    ) -> Self {
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
            recommendation_queue,
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
        recommendation_queue: Arc<RecommendationQueue<T>>,
        cancellation_token: CancellationToken,
    ) -> Result<()>
    where
        T: RepoTrait + 'static,
        U: GetEventsOf + 'static,
    {
        let state = Arc::new(AppState::new(repo, nostr_client, recommendation_queue));
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
        let handle = Handle::new();
        tokio::spawn(await_shutdown(cancellation_token, handle.clone()));
        axum_server::bind(addr)
            .handle(handle)
            .serve(router.into_make_service_with_connect_info::<SocketAddr>())
            .await
            .context("Failed to start HTTP server")
    });
}

async fn await_shutdown(cancellation_token: CancellationToken, handle: Handle) {
    cancellation_token.cancelled().await;
    info!("Shuting down.");
    handle.graceful_shutdown(Some(Duration::from_secs(30)));
}
