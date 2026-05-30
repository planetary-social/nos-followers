use super::open_ranking;
use super::AppState;
use crate::config::Settings;
use crate::http_server::handlers::{
    cached_check_if_trusted, cached_get_recommendations, check_recommendation_status,
    serve_root_page,
};
use crate::metrics::setup_metrics;
use crate::relay_subscriber::GetEventsOf;
use crate::repo::{RepoError, RepoTrait};
use axum::{
    body::Body,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use hyper::http::{header, HeaderValue, Method, Request};
use std::{sync::Arc, time::Duration};
use thiserror::Error;
use tower_governor::{
    governor::GovernorConfigBuilder, key_extractor::SmartIpKeyExtractor, GovernorLayer,
};
use tower_http::{
    cors::{Any, CorsLayer},
    timeout::TimeoutLayer,
    trace::{DefaultMakeSpan, DefaultOnFailure, DefaultOnResponse, TraceLayer},
    LatencyUnit,
};
use tracing::{error, Level};

pub fn create_router<T, U>(
    state: Arc<AppState<T, U>>,
    settings: &Settings,
) -> Result<Router, ApiError>
where
    T: RepoTrait + 'static,
    U: GetEventsOf + 'static,
{
    let tracing_layer = TraceLayer::new_for_http()
        .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
        .on_response(
            DefaultOnResponse::new()
                .level(Level::INFO)
                .latency_unit(LatencyUnit::Millis),
        )
        .on_failure(DefaultOnFailure::new().level(Level::ERROR));

    let metrics_handle = setup_metrics().map_err(|e| ApiError::InternalServerError(e.into()))?;
    let rate_limit_config = Arc::new(
        GovernorConfigBuilder::default()
            .key_extractor(SmartIpKeyExtractor)
            .per_second(1000) // Let's start big, we reduce it after metrics
            .burst_size(2000)
            .use_headers()
            .finish()
            .unwrap(),
    );

    let cors = CorsLayer::new()
        .allow_methods([
            Method::GET,
            Method::PUT,
            Method::POST,
            Method::PATCH,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers(Any)
        .allow_origin(Any)
        .max_age(Duration::from_secs(86400));

    let cache_duration = settings.http_cache_seconds;

    // OpenRanking endpoints live at the top level (not under /api/v1) per the
    // protocol; they share AppState with the rest of the server. The capability
    // document lives here too so it carries the same CORS layer as the endpoints
    // it advertises (browsers fetch it cross-origin during discovery).
    let open_ranking_routes = Router::new()
        .route(
            open_ranking::CAPABILITY_PATH,
            get(open_ranking::serve_capability_document),
        )
        .route(
            open_ranking::RANK_ENDPOINT,
            post(open_ranking::serve_rank::<T, U>),
        )
        .route(
            open_ranking::REPUTATION_ENDPOINT,
            post(open_ranking::serve_reputation::<T, U>),
        )
        .route(
            open_ranking::FOLLOWERS_ENDPOINT,
            post(open_ranking::serve_followers::<T, U>),
        )
        .route(
            open_ranking::RECOMMEND_ENDPOINT,
            post(open_ranking::serve_recommend::<T, U>),
        )
        .layer(TimeoutLayer::new(Duration::from_secs(5)))
        .layer(GovernorLayer {
            config: rate_limit_config.clone(),
        })
        .layer(cors.clone())
        .with_state(state.clone());

    Ok(Router::new()
        .route("/", get(serve_root_page))
        .route("/metrics", get(|| async move { metrics_handle.render() }))
        .merge(open_ranking_routes)
        .nest(
            "/api/v1",
            Router::new()
                .route(
                    "/recommendations/:pubkey",
                    get(cached_get_recommendations::<T, U>),
                )
                .route(
                    "/recommendations/:pubkey/status",
                    get(check_recommendation_status::<T, U>),
                )
                .route("/trusted/:pubkey", get(cached_check_if_trusted::<T, U>))
                .layer(TimeoutLayer::new(Duration::from_secs(5)))
                .layer(GovernorLayer {
                    config: rate_limit_config.clone(),
                })
                .layer(cors)
                .route_layer(axum::middleware::from_fn(move |req, next| {
                    add_cache_header(req, next, cache_duration)
                }))
                .with_state(state),
        )
        .layer(tracing_layer))
}

async fn add_cache_header(
    req: Request<Body>,
    next: axum::middleware::Next,
    cache_duration: u32,
) -> impl IntoResponse {
    let mut response = next.run(req).await;

    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_str(&format!("public, max-age={}", cache_duration)).unwrap(),
    );
    response
}

#[derive(Error, Debug)]
pub enum ApiError {
    #[error("Invalid public key")]
    InvalidPublicKey(#[from] nostr_sdk::nostr::key::Error),
    #[error(transparent)]
    RepoError(#[from] RepoError),
    #[error(transparent)]
    AxumError(#[from] axum::Error),
    #[error("Internal server errorre")]
    InternalServerError(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, body) = match &self {
            ApiError::InvalidPublicKey(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            ApiError::RepoError(_) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
            ApiError::AxumError(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Axum error".to_string()),
            ApiError::InternalServerError(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, self.to_string())
            }
        };

        error!("Api error: {}", self);
        (status, body).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Settings;
    use crate::recommendation_queue::RecommendationQueue;
    use async_trait::async_trait;
    use axum::body::Body;
    use nostr_sdk::{Event, Filter};
    use tower::ServiceExt;

    /// Minimal repo: the capability route never touches it, so the trait's
    /// default (panicking) implementations are fine.
    #[derive(Default)]
    struct MockRepo;
    impl RepoTrait for MockRepo {}

    #[derive(Default)]
    struct MockNostrClient;

    #[async_trait]
    impl GetEventsOf for MockNostrClient {
        async fn get_events_of(
            &self,
            _filters: Vec<Filter>,
            _timeout: Option<Duration>,
        ) -> Result<Vec<Event>, nostr_sdk::client::Error> {
            Ok(vec![])
        }
    }

    fn test_settings() -> Settings {
        serde_json::from_value(serde_json::json!({
            "relay": "wss://relay.example",
            "neo4j_uri": "db:7687",
            "neo4j_user": "neo4j",
            "neo4j_password": "x",
            "event_channel_size": 1000,
            "diff_workers": 1,
            "follow_change_channel_size": 1000,
            "follow_change_workers": 1,
            "worker_timeout_secs": 15,
            "google_project_id": "p",
            "google_topic": "t",
            "flush_period_seconds": 60,
            "min_seconds_between_messages": 60,
            "tcp_importer_port": 3001,
            "http_port": 3000,
            "pagerank_cron_expression": "0 0 0 * * *",
            "http_cache_seconds": 86400,
            "burst": 10,
            "redis_url": "redis://localhost"
        }))
        .expect("test settings deserialize")
    }

    fn test_state() -> Arc<AppState<MockRepo, MockNostrClient>> {
        let repo = Arc::new(MockRepo);
        let result_cache = moka::future::Cache::builder().build();
        let (queue, _receiver) = RecommendationQueue::new(repo.clone(), result_cache, 16);
        Arc::new(AppState::new(repo, Arc::new(MockNostrClient), Arc::new(queue)))
    }

    /// Regression test for the discovery document being served outside the
    /// CORS-layered group: it must carry `Access-Control-Allow-Origin` like the
    /// endpoints it advertises. `setup_metrics` installs a process-global
    /// recorder, so this is the only test that builds the full router.
    #[tokio::test]
    async fn capability_document_route_is_cors_enabled() {
        let router = create_router(test_state(), &test_settings()).expect("router builds");

        let response = router
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri(open_ranking::CAPABILITY_PATH)
                    .header(header::ORIGIN, "https://example.com")
                    // SmartIpKeyExtractor needs a client IP; supply one so the
                    // rate-limit layer does not reject the test request.
                    .header("x-forwarded-for", "203.0.113.7")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .contains_key(header::ACCESS_CONTROL_ALLOW_ORIGIN),
            "discovery document must send CORS headers like the endpoints it advertises"
        );
    }
}
