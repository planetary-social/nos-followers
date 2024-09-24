use super::AppState;
use crate::config::Settings;
use crate::http_server::handlers::{
    cached_check_if_trusted, cached_get_recommendations, serve_root_page,
};
use crate::metrics::setup_metrics;
use crate::relay_subscriber::GetEventsOf;
use crate::repo::{RepoError, RepoTrait};
use axum::{body::Body, http::StatusCode, response::IntoResponse, routing::get, Router};
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
    let rate_limit_config = GovernorConfigBuilder::default()
        .key_extractor(SmartIpKeyExtractor)
        .per_second(1000) // Let's start big, we reduce it after metrics
        .burst_size(2000)
        .use_headers()
        .finish()
        .unwrap();

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

    Ok(Router::new()
        .route("/", get(serve_root_page))
        .route("/metrics", get(|| async move { metrics_handle.render() }))
        .nest(
            "/api/v1",
            Router::new()
                .route(
                    "/recommendations/:pubkey",
                    get(cached_get_recommendations::<T, U>),
                )
                .route("/trusted/:pubkey", get(cached_check_if_trusted::<T, U>))
                .layer(TimeoutLayer::new(Duration::from_secs(5)))
                .layer(GovernorLayer {
                    config: Arc::new(rate_limit_config),
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
