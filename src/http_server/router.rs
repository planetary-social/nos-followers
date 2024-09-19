use super::AppState;
use crate::metrics::setup_metrics;
use crate::repo::{Recommendation, RepoError, RepoTrait};
use anyhow::Result;
use axum::{
    extract::State, http::HeaderMap, http::StatusCode, response::Html, response::IntoResponse,
    routing::get, Json, Router,
};
use nostr_sdk::PublicKey;
use std::{sync::Arc, time::Duration};
use thiserror::Error;
use tower_http::{
    timeout::TimeoutLayer,
    trace::{DefaultMakeSpan, DefaultOnFailure, DefaultOnResponse, TraceLayer},
    LatencyUnit,
};
use tracing::{error, info, Level};

pub fn create_router<T>(state: Arc<AppState<T>>) -> Result<Router>
where
    T: RepoTrait + 'static, // 'static is needed because the router needs to be static
{
    let tracing_layer = TraceLayer::new_for_http()
        .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
        .on_response(
            DefaultOnResponse::new()
                .level(Level::INFO)
                .latency_unit(LatencyUnit::Millis),
        )
        .on_failure(DefaultOnFailure::new().level(Level::ERROR));

    let metrics_handle = setup_metrics()?;

    Ok(Router::new()
        .route("/", get(serve_root_page))
        .route("/metrics", get(|| async move { metrics_handle.render() }))
        .route(
            "/recommendations/:pubkey",
            get(cached_get_recommendations::<T>),
        )
        .route("/maybe_spammer/:pubkey", get(cached_maybe_spammer::<T>))
        .layer(tracing_layer)
        .layer(TimeoutLayer::new(Duration::from_secs(5)))
        .with_state(state))
}

async fn cached_get_recommendations<T>(
    State(state): State<Arc<AppState<T>>>,
    axum::extract::Path(pubkey): axum::extract::Path<String>,
) -> Result<Json<Vec<Recommendation>>, ApiError>
where
    T: RepoTrait,
{
    let public_key = PublicKey::from_hex(&pubkey).map_err(ApiError::InvalidPublicKey)?;
    if let Some(cached_recommendation_result) = state.recommendation_cache.get(&pubkey).await {
        return Ok(Json(cached_recommendation_result));
    }

    let recommendations = get_recommendations(&state.repo, public_key).await?;

    state
        .recommendation_cache
        .insert(pubkey, recommendations.clone())
        .await;

    Ok(Json(recommendations))
}

async fn get_recommendations<T>(
    repo: &Arc<T>,
    public_key: PublicKey,
) -> Result<Vec<Recommendation>, ApiError>
where
    T: RepoTrait,
{
    repo.get_recommendations(&public_key)
        .await
        .map_err(ApiError::from)
}

async fn cached_maybe_spammer<T>(
    State(state): State<Arc<AppState<T>>>, // Extract shared state with generic RepoTrait
    axum::extract::Path(pubkey): axum::extract::Path<String>, // Extract pubkey from the path
) -> Result<Json<bool>, ApiError>
where
    T: RepoTrait,
{
    let public_key = PublicKey::from_hex(&pubkey).map_err(ApiError::InvalidPublicKey)?;

    if let Some(cached_spammer_result) = state.spammer_cache.get(&pubkey).await {
        return Ok(Json(cached_spammer_result));
    }

    let is_spammer = maybe_spammer(&state.repo, public_key).await?;

    state.spammer_cache.insert(pubkey, is_spammer).await;

    Ok(Json(is_spammer))
}

async fn maybe_spammer<T>(repo: &Arc<T>, public_key: PublicKey) -> Result<bool, ApiError>
where
    T: RepoTrait,
{
    info!("Checking if {} is a spammer", public_key.to_hex());
    let pagerank = repo
        .get_pagerank(&public_key)
        .await
        .map_err(|_| ApiError::NotFound)?;

    info!("Pagerank for {}: {}", public_key.to_hex(), pagerank);

    Ok(pagerank < 0.2)
    // TODO don't return if it's too low, instead use other manual
    // checks, nos user, nip05, check network, more hints, and only then
    // give up
}

async fn serve_root_page(_headers: HeaderMap) -> impl IntoResponse {
    let body = r#"
        <html>
            <head>
                <title>Nos Followers Server</title>
            </head>
            <body>
                <h1>Healthy</h1>
            </body>
        </html>
    "#;

    Html(body)
}

#[derive(Error, Debug)]
pub enum ApiError {
    #[error("Invalid public key")]
    InvalidPublicKey(#[from] nostr_sdk::nostr::key::Error),
    #[error("Not found, try later")]
    NotFound,
    #[error(transparent)]
    RepoError(#[from] RepoError),
    #[error(transparent)]
    AxumError(#[from] axum::Error),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, body) = match &self {
            ApiError::InvalidPublicKey(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            ApiError::NotFound => (StatusCode::NOT_FOUND, self.to_string()),
            ApiError::RepoError(_) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
            ApiError::AxumError(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Axum error".to_string()),
        };

        error!("Api error: {}", self);
        (status, body).into_response()
    }
}
