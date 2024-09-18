use super::AppState;
use crate::metrics::setup_metrics;
use crate::repo::{Recommendation, RepoError, RepoTrait};
use anyhow::Result;
use axum::Json;
use axum::{
    extract::State, http::HeaderMap, http::StatusCode, response::Html, response::IntoResponse,
    routing::get, Router,
};
use nostr_sdk::PublicKey;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tower_http::LatencyUnit;
use tower_http::{timeout::TimeoutLayer, trace::DefaultOnFailure};
use tracing::Level;

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
            get(get_recommendations_handler::<T>),
        ) // Make handler generic
        .route("/maybe_spammer/:pubkey", get(maybe_spammer::<T>)) // Make handler generic
        .layer(tracing_layer)
        .layer(TimeoutLayer::new(Duration::from_secs(5)))
        .with_state(state)) // Attach state to the router
}

async fn get_recommendations_handler<T>(
    State(state): State<Arc<AppState<T>>>, // Extract shared state with generic RepoTrait
    axum::extract::Path(pubkey): axum::extract::Path<String>, // Extract pubkey from the path
) -> Result<Json<Vec<Recommendation>>, ApiError>
where
    T: RepoTrait,
{
    let public_key = PublicKey::from_hex(&pubkey).map_err(|_| ApiError::InvalidPublicKey)?;
    let recommendations = state
        .repo
        .get_recommendations(&public_key)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(recommendations))
}

async fn maybe_spammer<T>(
    State(state): State<Arc<AppState<T>>>, // Extract shared state with generic RepoTrait
    axum::extract::Path(pubkey): axum::extract::Path<String>, // Extract pubkey from the path
) -> Json<bool>
where
    T: RepoTrait,
{
    let Ok(public_key) = PublicKey::from_hex(&pubkey).map_err(|_| ApiError::InvalidPublicKey)
    else {
        return Json(false);
    };

    let Ok(pagerank) = state
        .repo
        .get_pagerank(&public_key)
        .await
        .map_err(ApiError::from)
    else {
        return Json(false);
    };

    Json(pagerank > 0.5)
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
    InvalidPublicKey,
    #[error(transparent)]
    RepoError(#[from] RepoError),
    #[error(transparent)]
    AxumError(#[from] axum::Error),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, body) = match self {
            ApiError::InvalidPublicKey => {
                (StatusCode::BAD_REQUEST, "Invalid public key".to_string())
            }
            ApiError::RepoError(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Repository error".to_string(),
            ),
            ApiError::AxumError(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Axum error".to_string()),
        };
        (status, body).into_response()
    }
}
