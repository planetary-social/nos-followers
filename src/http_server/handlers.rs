use super::router::ApiError;
use super::trust_policy::is_trusted;
use super::AppState;
use crate::relay_subscriber::GetEventsOf;
use crate::repo::{Recommendation, RepoTrait};
use axum::{extract::State, http::HeaderMap, response::Html, response::IntoResponse, Json};
use chrono::Utc;
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
    let Some(account_info) = repo.get_account_info(public_key).await? else {
        return Err(ApiError::NotFound);
    };

    info!("Checking if account is trusted: {}", account_info);

    let followed_by_enough_people = account_info.follower_count.is_some_and(|c| c > 1);
    let following_enough_people = account_info.followee_count.is_some_and(|c| c > 1);
    let has_good_enough_followers = account_info.pagerank.is_some_and(|pr| pr > 0.2);
    let oldest_event_seen_at = account_info.created_at.unwrap_or(Utc::now());
    let latest_contact_list_at = account_info.last_contact_list_at.unwrap_or(Utc::now());
    let is_nos_user = false;

    Ok(is_trusted(
        has_good_enough_followers,
        followed_by_enough_people,
        following_enough_people,
        oldest_event_seen_at,
        latest_contact_list_at,
        is_nos_user,
    ))
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
