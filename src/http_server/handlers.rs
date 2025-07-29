use super::router::ApiError;
use super::AppState;
use crate::relay_subscriber::GetEventsOf;
use crate::repo::{Recommendation, RepoTrait};
use axum::{
    extract::{Query, State},
    http::HeaderMap,
    response::Html,
    response::IntoResponse,
    Json,
};
use nostr_sdk::PublicKey;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct RecommendationParams {
    min_pagerank: Option<f64>,
    limit: Option<usize>,
}

pub async fn cached_get_recommendations<T, U>(
    State(state): State<Arc<AppState<T, U>>>,
    axum::extract::Path(pubkey): axum::extract::Path<String>,
    Query(params): Query<RecommendationParams>,
) -> Result<Json<Vec<Recommendation>>, ApiError>
where
    T: RepoTrait,
    U: GetEventsOf,
{
    let public_key = PublicKey::from_hex(&pubkey).map_err(ApiError::InvalidPublicKey)?;

    // Create cache key that includes parameters
    let cache_key = format!(
        "{}:{}:{}",
        pubkey,
        params.min_pagerank.unwrap_or(0.3),
        params.limit.unwrap_or(50)
    );

    if let Some(cached_recommendation_result) = state.recommendation_cache.get(&cache_key).await {
        return Ok(Json(cached_recommendation_result));
    }

    let recommendations =
        get_recommendations(&state.repo, public_key, params.min_pagerank, params.limit).await?;

    state
        .recommendation_cache
        .insert(cache_key, recommendations.clone())
        .await;

    Ok(Json(recommendations))
}

async fn get_recommendations<T>(
    repo: &Arc<T>,
    public_key: PublicKey,
    min_pagerank: Option<f64>,
    limit: Option<usize>,
) -> Result<Vec<Recommendation>, ApiError>
where
    T: RepoTrait,
{
    repo.get_recommendations(&public_key, min_pagerank, limit)
        .await
        .map_err(ApiError::from)
}

pub async fn cached_check_if_trusted<T, U>(
    State(state): State<Arc<AppState<T, U>>>,
    axum::extract::Path(pubkey): axum::extract::Path<String>,
) -> Result<Json<bool>, ApiError>
where
    T: RepoTrait,
    U: GetEventsOf,
{
    let public_key = PublicKey::from_hex(&pubkey).map_err(ApiError::InvalidPublicKey)?;

    if let Some(cached_spammer_result) = state.trust_cache.get(&pubkey).await {
        return Ok(Json(cached_spammer_result));
    }

    let trusted = state.repo.check_if_trusted(&public_key).await?;

    state.trust_cache.insert(pubkey, trusted).await;

    Ok(Json(trusted))
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
