use super::router::ApiError;
use super::AppState;
use crate::repo::{Recommendation, RepoTrait};
use axum::{extract::State, http::HeaderMap, response::Html, response::IntoResponse, Json};
use nostr_sdk::PublicKey;
use std::sync::Arc;
use tracing::info;

pub async fn cached_get_recommendations<T>(
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

pub async fn cached_maybe_spammer<T>(
    State(state): State<Arc<AppState<T>>>, // Extract shared state with generic RepoTrait
    axum::extract::Path(pubkey): axum::extract::Path<String>, // Extract pubkey from the path
) -> Result<Json<bool>, ApiError>
where
    T: RepoTrait,
{
    println!("cached_maybe_spammer");
    let public_key = PublicKey::from_hex(&pubkey).map_err(ApiError::InvalidPublicKey)?;
    println!("cached_maybe_spammer2");

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
