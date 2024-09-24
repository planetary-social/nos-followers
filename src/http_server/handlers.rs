use super::router::ApiError;
use super::trust_policy::is_trusted;
use super::AppState;
use crate::relay_subscriber::GetEventsOf;
use crate::repo::{Recommendation, RepoTrait};
use axum::{extract::State, http::HeaderMap, response::Html, response::IntoResponse, Json};
use nostr_sdk::PublicKey;
use std::sync::Arc;
use tracing::info;

pub async fn cached_get_recommendations<T, U>(
    State(state): State<Arc<AppState<T, U>>>,
    axum::extract::Path(pubkey): axum::extract::Path<String>,
) -> Result<Json<Vec<Recommendation>>, ApiError>
where
    T: RepoTrait,
    U: GetEventsOf,
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

    let trusted = check_if_trusted(&state.repo, &public_key).await?;

    state.trust_cache.insert(pubkey, trusted).await;

    Ok(Json(trusted))
}

/// This needs to be fast even if not cached, relay filters may rely on this
async fn check_if_trusted<T>(repo: &Arc<T>, public_key: &PublicKey) -> Result<bool, ApiError>
where
    T: RepoTrait,
{
    let maybe_account_info = repo.get_account_info(public_key).await?;

    info!(
        "Checking if {}is trusted. Account: {:?}",
        public_key, maybe_account_info
    );

    let found = maybe_account_info.is_some();
    let has_good_enough_followers =
        maybe_account_info.is_some_and(|a| a.pagerank.is_some_and(|pr| pr > 0.2));
    // TODO: This is a placeholder, we need to implement this from a nos user registration endpoint
    let is_nos_user = false;

    Ok(is_trusted(found, has_good_enough_followers, is_nos_user))
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
