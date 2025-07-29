use super::router::ApiError;
use super::AppState;
use crate::recommendation_queue::RecommendationStatus;
use crate::relay_subscriber::GetEventsOf;
use crate::repo::{Recommendation, RepoTrait};
use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse, Response},
    Json,
};
use nostr_sdk::PublicKey;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;

#[derive(Debug, Deserialize)]
pub struct RecommendationParams {
    min_pagerank: Option<f64>,
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum RecommendationResponse {
    Completed(Vec<Recommendation>),
    Pending { status: String, message: String },
    Processing { status: String, message: String },
    Failed { status: String, error: String },
}

pub async fn cached_get_recommendations<T, U>(
    State(state): State<Arc<AppState<T, U>>>,
    axum::extract::Path(pubkey): axum::extract::Path<String>,
    Query(params): Query<RecommendationParams>,
) -> Result<Response, ApiError>
where
    T: RepoTrait,
    U: GetEventsOf,
{
    info!(
        "GET /api/v1/recommendations/{} - params: {:?}",
        pubkey, params
    );

    let public_key = PublicKey::from_hex(&pubkey).map_err(ApiError::InvalidPublicKey)?;

    // Request recommendations through the queue
    let status = state
        .recommendation_queue
        .request_recommendations(public_key, params.min_pagerank, params.limit)
        .await
        .map_err(|e| ApiError::InternalServerError(e.into()))?;

    match status {
        RecommendationStatus::Completed(recommendations) => {
            info!(
                "Returning {} completed recommendations for {}",
                recommendations.len(),
                pubkey
            );
            Ok((
                StatusCode::OK,
                Json(RecommendationResponse::Completed(recommendations)),
            )
                .into_response())
        }
        RecommendationStatus::Pending => {
            info!("Recommendation request pending for {}", pubkey);
            Ok((
                StatusCode::ACCEPTED,
                Json(RecommendationResponse::Pending {
                    status: "pending".to_string(),
                    message: "Recommendation calculation queued".to_string(),
                }),
            )
                .into_response())
        }
        RecommendationStatus::Processing => {
            info!("Recommendation request processing for {}", pubkey);
            Ok((
                StatusCode::ACCEPTED,
                Json(RecommendationResponse::Processing {
                    status: "processing".to_string(),
                    message: "Recommendation calculation in progress".to_string(),
                }),
            )
                .into_response())
        }
        RecommendationStatus::Failed(error) => {
            info!("Recommendation request failed for {}: {}", pubkey, error);
            Ok((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(RecommendationResponse::Failed {
                    status: "failed".to_string(),
                    error,
                }),
            )
                .into_response())
        }
    }
}

pub async fn cached_check_if_trusted<T, U>(
    State(state): State<Arc<AppState<T, U>>>,
    axum::extract::Path(pubkey): axum::extract::Path<String>,
) -> Result<Json<bool>, ApiError>
where
    T: RepoTrait,
    U: GetEventsOf,
{
    info!("GET /api/v1/trusted/{}", pubkey);

    let public_key = PublicKey::from_hex(&pubkey).map_err(ApiError::InvalidPublicKey)?;

    if let Some(cached_spammer_result) = state.trust_cache.get(&pubkey).await {
        info!(
            "Cache hit for trusted check: {} -> {}",
            pubkey, cached_spammer_result
        );
        return Ok(Json(cached_spammer_result));
    }

    info!("Cache miss for trusted check: {}", pubkey);
    let trusted = state.repo.check_if_trusted(&public_key).await?;

    state.trust_cache.insert(pubkey.clone(), trusted).await;

    info!("Trusted check result for {}: {}", pubkey, trusted);
    Ok(Json(trusted))
}

pub async fn check_recommendation_status<T, U>(
    State(state): State<Arc<AppState<T, U>>>,
    axum::extract::Path(pubkey): axum::extract::Path<String>,
    Query(params): Query<RecommendationParams>,
) -> Result<Response, ApiError>
where
    T: RepoTrait,
    U: GetEventsOf,
{
    info!(
        "GET /api/v1/recommendations/{}/status - params: {:?}",
        pubkey, params
    );

    // Create cache key that includes parameters
    let cache_key = format!(
        "{}:{}:{}",
        pubkey,
        params.min_pagerank.unwrap_or(0.3),
        params.limit.unwrap_or(50)
    );

    let status = state
        .recommendation_queue
        .get_status(&cache_key)
        .await
        .unwrap_or(RecommendationStatus::Pending);

    match status {
        RecommendationStatus::Completed(recommendations) => {
            info!(
                "Status check: {} completed recommendations for {}",
                recommendations.len(),
                pubkey
            );
            Ok((
                StatusCode::OK,
                Json(RecommendationResponse::Completed(recommendations)),
            )
                .into_response())
        }
        RecommendationStatus::Pending => {
            info!("Status check: recommendations pending for {}", pubkey);
            Ok((
                StatusCode::OK,
                Json(RecommendationResponse::Pending {
                    status: "pending".to_string(),
                    message: "Recommendation calculation queued".to_string(),
                }),
            )
                .into_response())
        }
        RecommendationStatus::Processing => {
            info!("Status check: recommendations processing for {}", pubkey);
            Ok((
                StatusCode::OK,
                Json(RecommendationResponse::Processing {
                    status: "processing".to_string(),
                    message: "Recommendation calculation in progress".to_string(),
                }),
            )
                .into_response())
        }
        RecommendationStatus::Failed(error) => {
            info!(
                "Status check: recommendations failed for {}: {}",
                pubkey, error
            );
            Ok((
                StatusCode::OK,
                Json(RecommendationResponse::Failed {
                    status: "failed".to_string(),
                    error,
                }),
            )
                .into_response())
        }
    }
}

pub async fn serve_root_page(_headers: HeaderMap) -> impl IntoResponse {
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
