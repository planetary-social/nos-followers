//! OpenRanking protocol support (<https://github.com/Open-Ranking/protocol>).
//!
//! Exposes the subset of the protocol that maps onto our follow graph:
//! capability discovery (ORE-01) plus the `/rank`, `/reputation`, `/followers`
//! and `/recommend` endpoints. See OREs 00-06 for the full specification.

use super::AppState;
use crate::relay_subscriber::GetEventsOf;
use crate::repo::RepoTrait;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use nostr_sdk::PublicKey;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::error;

/// Endpoint paths exposed by this provider. Each is a key in the capability
/// document (ORE-01) and the route path for the corresponding handler.
pub(crate) const RANK_ENDPOINT: &str = "/rank";
pub(crate) const REPUTATION_ENDPOINT: &str = "/reputation";
pub(crate) const FOLLOWERS_ENDPOINT: &str = "/followers";
pub(crate) const RECOMMEND_ENDPOINT: &str = "/recommend";

/// Well-known location of the capability document (ORE-01).
pub(crate) const CAPABILITY_PATH: &str = "/.well-known/open-ranking";

/// An ORE-01 Algorithm Object: a ranking/search strategy offered for an endpoint.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct AlgorithmSpec {
    /// Opaque identifier (ORE-00: lowercase alphanumeric, hyphens and dots).
    pub id: &'static str,
    /// `true` when the algorithm is personalised and requires a `pov` pubkey.
    /// Omitted from the document when `false` (i.e. a global algorithm).
    #[serde(skip_serializing_if = "is_global")]
    pub pov: bool,
    /// Short, human-readable description. Plain UTF-8, no markup.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<&'static str>,
}

/// `skip_serializing_if` predicate: a global algorithm omits the `pov` field.
fn is_global(pov: &bool) -> bool {
    !*pov
}

/// The endpoints and algorithms this provider supports, in priority order.
/// The first algorithm listed for an endpoint is its default (ORE-01).
fn registry() -> Vec<(&'static str, Vec<AlgorithmSpec>)> {
    vec![
        (
            RANK_ENDPOINT,
            vec![AlgorithmSpec {
                id: "pagerank",
                pov: false,
                description: Some("Global PageRank over the Nostr follow graph. Higher is better."),
            }],
        ),
        (
            REPUTATION_ENDPOINT,
            vec![AlgorithmSpec {
                id: "pagerank",
                pov: false,
                description: Some("PageRank reputation with follower and following counts."),
            }],
        ),
        (
            FOLLOWERS_ENDPOINT,
            vec![AlgorithmSpec {
                id: "pagerank",
                pov: false,
                description: Some("Top followers ranked by global PageRank."),
            }],
        ),
        (
            RECOMMEND_ENDPOINT,
            vec![AlgorithmSpec {
                id: "for-you",
                pov: true,
                description: Some("Personalised follow recommendations from your social graph."),
            }],
        ),
    ]
}

/// Build the ORE-01 capability document: a JSON object keyed by endpoint path,
/// each value a non-empty array of Algorithm Objects.
pub(crate) fn capability_document() -> serde_json::Value {
    let mut document = serde_json::Map::new();
    for (endpoint, algorithms) in registry() {
        document.insert(
            endpoint.to_string(),
            serde_json::to_value(algorithms).expect("algorithm objects always serialize"),
        );
    }
    serde_json::Value::Object(document)
}

/// `GET /.well-known/open-ranking` (ORE-01). Always 200 with `application/json`.
pub(crate) async fn serve_capability_document() -> impl IntoResponse {
    axum::Json(capability_document())
}

/// Maximum number of pubkeys accepted by `/rank` in a single request (ORE-02).
const MAX_PUBKEYS: usize = 1000;

/// TTL hint (seconds) for PageRank-derived responses. PageRank is recomputed on
/// a daily cron, so an hour is a safe "still fresh" hint for clients.
const RANK_TTL_SECONDS: u64 = 3600;

/// Default and maximum `limit` for endpoints that select their own candidates
/// (`/followers`, `/recommend`).
const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 1000;

/// An ORE-02 Profile: a ranked Nostr identity. Higher `rank` is always better.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct Profile {
    /// 64-char lowercase hex pubkey (ORE-00).
    pub pubkey: String,
    pub rank: f64,
}

/// The shared `/rank`-shaped response: profiles sorted by rank desc + a TTL hint.
#[derive(Debug, Serialize)]
pub(crate) struct RankResponse {
    pub profiles: Vec<Profile>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl: Option<u64>,
}

/// Request body for `POST /rank` (ORE-02). Unknown fields are ignored for
/// forward compatibility; a `pov` sent to the global algorithm is ignored.
#[derive(Debug, Deserialize)]
pub(crate) struct RankRequest {
    #[serde(default)]
    pub pubkeys: Vec<String>,
    #[serde(default)]
    pub algorithm: Option<String>,
    #[serde(default)]
    pub limit: Option<i64>,
}

/// Errors returned by OpenRanking endpoints, mapped to their HTTP status codes.
/// The human-readable reason is also sent in the `X-Reason` header (ORE-00).
#[derive(Debug)]
pub(crate) enum OpenRankingError {
    /// 400: the request body is malformed or not valid JSON.
    MalformedBody(String),
    /// 413: the request exceeds the provider's size limit.
    PayloadTooLarge(String),
    /// 422: the request is well-formed but semantically invalid.
    Unprocessable(String),
    /// 500: an internal error (e.g. the graph database is unavailable).
    Internal,
}

impl OpenRankingError {
    pub fn malformed_body(reason: impl Into<String>) -> Self {
        Self::MalformedBody(reason.into())
    }

    pub fn payload_too_large(reason: impl Into<String>) -> Self {
        Self::PayloadTooLarge(reason.into())
    }

    pub fn unprocessable(reason: impl Into<String>) -> Self {
        Self::Unprocessable(reason.into())
    }

    pub fn status(&self) -> StatusCode {
        match self {
            Self::MalformedBody(_) => StatusCode::BAD_REQUEST,
            Self::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            Self::Unprocessable(_) => StatusCode::UNPROCESSABLE_ENTITY,
            Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn reason(&self) -> &str {
        match self {
            Self::MalformedBody(reason)
            | Self::PayloadTooLarge(reason)
            | Self::Unprocessable(reason) => reason,
            Self::Internal => "internal server error",
        }
    }
}

impl IntoResponse for OpenRankingError {
    fn into_response(self) -> Response {
        let status = self.status();
        let reason = self.reason().to_string();

        // ORE-00: surface the reason in the X-Reason header.
        let mut headers = axum::http::HeaderMap::new();
        if let Ok(value) = HeaderValue::from_str(&reason) {
            headers.insert("x-reason", value);
        }

        (
            status,
            headers,
            Json(serde_json::json!({ "error": reason })),
        )
            .into_response()
    }
}

/// Resolve the algorithm for an endpoint. With no request, the default is the
/// first algorithm listed (ORE-01); an unsupported id is a 422.
pub(crate) fn resolve_algorithm(
    endpoint: &str,
    requested: Option<&str>,
) -> Result<AlgorithmSpec, OpenRankingError> {
    let algorithms = registry()
        .into_iter()
        .find(|(path, _)| *path == endpoint)
        .map(|(_, algorithms)| algorithms)
        .expect("endpoint is registered");

    match requested {
        None => Ok(algorithms
            .into_iter()
            .next()
            .expect("each endpoint has at least one algorithm")),
        Some(id) => algorithms
            .into_iter()
            .find(|algorithm| algorithm.id == id)
            .ok_or_else(|| {
                OpenRankingError::unprocessable(format!(
                    "algorithm '{id}' is not supported by {endpoint}"
                ))
            }),
    }
}

/// Parse a JSON request body, mapping any deserialization failure to 400.
fn parse_json_body<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, OpenRankingError> {
    serde_json::from_slice(body)
        .map_err(|e| OpenRankingError::malformed_body(format!("invalid JSON body: {e}")))
}

/// Core `/rank` logic (ORE-02): rank the requested pubkeys by global PageRank.
pub(crate) async fn rank<T: RepoTrait>(
    repo: &T,
    request: RankRequest,
) -> Result<RankResponse, OpenRankingError> {
    // ORE-01: validate the requested algorithm (defaults to the first listed).
    resolve_algorithm(RANK_ENDPOINT, request.algorithm.as_deref())?;

    // ORE-02: pubkeys MUST contain at least one entry.
    if request.pubkeys.is_empty() {
        return Err(OpenRankingError::unprocessable(
            "pubkeys must contain at least one pubkey",
        ));
    }

    // ORE-02: providers MAY reject oversized requests with 413.
    if request.pubkeys.len() > MAX_PUBKEYS {
        return Err(OpenRankingError::payload_too_large(format!(
            "pubkeys exceeds the maximum of {MAX_PUBKEYS}"
        )));
    }

    // ORE-02: limit, when provided, MUST be a positive integer.
    if let Some(limit) = request.limit {
        if limit < 1 {
            return Err(OpenRankingError::unprocessable(
                "limit must be a positive integer",
            ));
        }
    }

    // ORE-02: the default limit is the number of requested pubkeys, and a larger
    // limit is silently clamped to that count.
    let default_limit = request.pubkeys.len();
    let limit = request
        .limit
        .map(|limit| limit as usize)
        .unwrap_or(default_limit)
        .min(default_limit);

    // Parse and de-duplicate the pubkeys, preserving request order.
    let mut seen = HashSet::new();
    let mut pubkeys = Vec::with_capacity(request.pubkeys.len());
    for raw in &request.pubkeys {
        let pubkey = PublicKey::from_hex(raw).map_err(|e| {
            OpenRankingError::malformed_body(format!("invalid pubkey '{raw}': {e}"))
        })?;
        if seen.insert(pubkey) {
            pubkeys.push(pubkey);
        }
    }

    let pageranks = repo.get_pageranks(&pubkeys).await.map_err(|e| {
        error!("/rank get_pageranks failed: {e}");
        OpenRankingError::Internal
    })?;

    // ORE-02: return a rank for every requested pubkey; unknown pubkeys rank 0.
    let mut profiles: Vec<Profile> = pubkeys
        .iter()
        .map(|pubkey| Profile {
            pubkey: pubkey.to_hex(),
            rank: pageranks.get(pubkey).copied().unwrap_or(0.0),
        })
        .collect();

    profiles.sort_by(|a, b| b.rank.partial_cmp(&a.rank).unwrap_or(Ordering::Equal));
    profiles.truncate(limit);

    Ok(RankResponse {
        profiles,
        ttl: Some(RANK_TTL_SECONDS),
    })
}

/// `POST /rank` (ORE-02).
pub(crate) async fn serve_rank<T, U>(
    State(state): State<Arc<AppState<T, U>>>,
    body: Bytes,
) -> Result<Json<RankResponse>, OpenRankingError>
where
    T: RepoTrait,
    U: GetEventsOf,
{
    let request: RankRequest = parse_json_body(&body)?;
    let response = rank(state.repo.as_ref(), request).await?;
    Ok(Json(response))
}

/// An ORE-06 `stats` object: non-negative integer counts from our index.
#[derive(Debug, Serialize)]
pub(crate) struct ReputationStats {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub followers: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub follows: Option<u64>,
}

/// Provider-specific `x` extensions for `/reputation` (ORE-06).
#[derive(Debug, Serialize)]
pub(crate) struct ReputationExtensions {
    /// Our boolean trust verdict (PageRank-based; see `AccountInfo::is_trusted`).
    pub trusted: bool,
}

/// Response body for `POST /reputation` (ORE-06). `pubkey` and `rank` are
/// always present; the rest are omitted when we have no data.
#[derive(Debug, Serialize)]
pub(crate) struct ReputationResponse {
    pub pubkey: String,
    pub rank: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<ReputationStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub x: Option<ReputationExtensions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl: Option<u64>,
}

/// Request body for `POST /reputation` (ORE-06). Unknown fields are ignored.
#[derive(Debug, Deserialize)]
pub(crate) struct ReputationRequest {
    #[serde(default)]
    pub pubkey: Option<String>,
    #[serde(default)]
    pub algorithm: Option<String>,
}

/// Parse a required pubkey field. A missing or invalid pubkey is a 422
/// (ORE-04/06), unlike a malformed pubkey inside a `/rank` array.
fn parse_required_pubkey(pubkey: Option<&str>) -> Result<PublicKey, OpenRankingError> {
    let pubkey =
        pubkey.ok_or_else(|| OpenRankingError::unprocessable("pubkey field is required"))?;
    PublicKey::from_hex(pubkey)
        .map_err(|e| OpenRankingError::unprocessable(format!("pubkey is invalid: {e}")))
}

/// Core `/reputation` logic (ORE-06): structured PageRank reputation for a pubkey.
pub(crate) async fn reputation<T: RepoTrait>(
    repo: &T,
    request: ReputationRequest,
) -> Result<ReputationResponse, OpenRankingError> {
    resolve_algorithm(REPUTATION_ENDPOINT, request.algorithm.as_deref())?;
    let pubkey = parse_required_pubkey(request.pubkey.as_deref())?;

    let account = repo.get_account_info(&pubkey).await.map_err(|e| {
        error!("/reputation get_account_info failed: {e}");
        OpenRankingError::Internal
    })?;

    let Some(account) = account else {
        // Unknown pubkey: rank is still required, but there are no stats to report.
        return Ok(ReputationResponse {
            pubkey: pubkey.to_hex(),
            rank: 0.0,
            stats: None,
            x: None,
            ttl: Some(RANK_TTL_SECONDS),
        });
    };

    let stats = if account.follower_count.is_some() || account.followee_count.is_some() {
        Some(ReputationStats {
            followers: account.follower_count,
            follows: account.followee_count,
        })
    } else {
        None
    };

    Ok(ReputationResponse {
        pubkey: pubkey.to_hex(),
        rank: account.pagerank.unwrap_or(0.0),
        stats,
        x: Some(ReputationExtensions {
            trusted: account.is_trusted(),
        }),
        ttl: Some(RANK_TTL_SECONDS),
    })
}

/// `POST /reputation` (ORE-06).
pub(crate) async fn serve_reputation<T, U>(
    State(state): State<Arc<AppState<T, U>>>,
    body: Bytes,
) -> Result<Json<ReputationResponse>, OpenRankingError>
where
    T: RepoTrait,
    U: GetEventsOf,
{
    let request: ReputationRequest = parse_json_body(&body)?;
    let response = reputation(state.repo.as_ref(), request).await?;
    Ok(Json(response))
}

/// Response body for `POST /followers` (ORE-04).
#[derive(Debug, Serialize)]
pub(crate) struct FollowersResponse {
    pub profiles: Vec<Profile>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl: Option<u64>,
}

/// Request body for `POST /followers` (ORE-04). Unknown fields are ignored.
#[derive(Debug, Deserialize)]
pub(crate) struct FollowersRequest {
    #[serde(default)]
    pub pubkey: Option<String>,
    #[serde(default)]
    pub algorithm: Option<String>,
    #[serde(default)]
    pub limit: Option<i64>,
}

/// Validate an optional `limit`, applying the provider default and maximum.
fn resolve_limit(limit: Option<i64>, default: usize) -> Result<usize, OpenRankingError> {
    match limit {
        None => Ok(default),
        Some(limit) if limit < 1 => Err(OpenRankingError::unprocessable(
            "limit must be a positive integer",
        )),
        Some(limit) if limit as usize > MAX_LIMIT => Err(OpenRankingError::unprocessable(format!(
            "limit exceeds the maximum of {MAX_LIMIT}"
        ))),
        Some(limit) => Ok(limit as usize),
    }
}

/// Core `/followers` logic (ORE-04): a pubkey's top followers by PageRank.
pub(crate) async fn followers<T: RepoTrait>(
    repo: &T,
    request: FollowersRequest,
) -> Result<FollowersResponse, OpenRankingError> {
    resolve_algorithm(FOLLOWERS_ENDPOINT, request.algorithm.as_deref())?;
    let pubkey = parse_required_pubkey(request.pubkey.as_deref())?;
    let limit = resolve_limit(request.limit, DEFAULT_LIMIT)?;

    let (top_followers, total) = repo.get_top_followers(&pubkey, limit).await.map_err(|e| {
        error!("/followers get_top_followers failed: {e}");
        OpenRankingError::Internal
    })?;

    let profiles = top_followers
        .into_iter()
        .map(|(pubkey, rank)| Profile {
            pubkey: pubkey.to_hex(),
            rank,
        })
        .collect();

    Ok(FollowersResponse {
        profiles,
        total,
        ttl: Some(RANK_TTL_SECONDS),
    })
}

/// `POST /followers` (ORE-04).
pub(crate) async fn serve_followers<T, U>(
    State(state): State<Arc<AppState<T, U>>>,
    body: Bytes,
) -> Result<Json<FollowersResponse>, OpenRankingError>
where
    T: RepoTrait,
    U: GetEventsOf,
{
    let request: FollowersRequest = parse_json_body(&body)?;
    let response = followers(state.repo.as_ref(), request).await?;
    Ok(Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::AccountInfo;
    use crate::repo::{RepoError, RepoTrait, TopFollowers};
    use axum::http::StatusCode;
    use nostr_sdk::{Keys, PublicKey};
    use std::collections::HashMap;

    fn fresh_pubkey() -> PublicKey {
        Keys::generate().public_key()
    }

    /// A repo stub that knows the PageRank of a fixed set of pubkeys and, like
    /// the real Cypher query, returns only the requested ones it knows about.
    #[derive(Default)]
    struct MockRepo {
        pageranks: HashMap<PublicKey, f64>,
        accounts: HashMap<PublicKey, AccountInfo>,
        /// Full follower set per target, sorted by rank desc (as the real query returns).
        top_followers: HashMap<PublicKey, Vec<(PublicKey, f64)>>,
        fail: bool,
    }

    impl RepoTrait for MockRepo {
        async fn get_pageranks(
            &self,
            pubkeys: &[PublicKey],
        ) -> Result<HashMap<PublicKey, f64>, RepoError> {
            if self.fail {
                return Err(RepoError::InvalidParameter("boom".into()));
            }
            Ok(pubkeys
                .iter()
                .filter_map(|pubkey| self.pageranks.get(pubkey).map(|rank| (*pubkey, *rank)))
                .collect())
        }

        async fn get_account_info(
            &self,
            pubkey: &PublicKey,
        ) -> Result<Option<AccountInfo>, RepoError> {
            if self.fail {
                return Err(RepoError::InvalidParameter("boom".into()));
            }
            Ok(self.accounts.get(pubkey).cloned())
        }

        async fn get_top_followers(
            &self,
            pubkey: &PublicKey,
            limit: usize,
        ) -> Result<TopFollowers, RepoError> {
            if self.fail {
                return Err(RepoError::InvalidParameter("boom".into()));
            }
            match self.top_followers.get(pubkey) {
                Some(all) => {
                    let total = all.len() as u64;
                    Ok((all.iter().take(limit).cloned().collect(), Some(total)))
                }
                None => Ok((Vec::new(), None)),
            }
        }
    }

    fn rank_request(pubkeys: &[PublicKey], limit: Option<i64>) -> RankRequest {
        RankRequest {
            pubkeys: pubkeys.iter().map(PublicKey::to_hex).collect(),
            algorithm: None,
            limit,
        }
    }

    // ---- capability document (ORE-01) ----

    #[test]
    fn capability_document_lists_each_endpoint_with_nonempty_algorithms() {
        let doc = capability_document();
        let obj = doc.as_object().expect("capability doc is a JSON object");
        for endpoint in [
            RANK_ENDPOINT,
            REPUTATION_ENDPOINT,
            FOLLOWERS_ENDPOINT,
            RECOMMEND_ENDPOINT,
        ] {
            let algorithms = obj
                .get(endpoint)
                .unwrap_or_else(|| panic!("endpoint {endpoint} present"))
                .as_array()
                .expect("algorithms is an array");
            assert!(!algorithms.is_empty(), "{endpoint} has >=1 algorithm");
            assert!(algorithms[0].get("id").is_some(), "algorithm has an id");
        }
    }

    #[test]
    fn recommend_default_algorithm_is_for_you_and_requires_pov() {
        let doc = capability_document();
        let algorithms = doc[RECOMMEND_ENDPOINT].as_array().unwrap();
        assert_eq!(algorithms[0]["id"], "for-you");
        assert_eq!(algorithms[0]["pov"], true);
    }

    #[test]
    fn global_algorithms_omit_the_pov_field() {
        let doc = capability_document();
        let rank = &doc[RANK_ENDPOINT].as_array().unwrap()[0];
        assert!(
            rank.get("pov").is_none(),
            "global algorithms must omit pov, got {rank}"
        );
    }

    // ---- algorithm selection + errors (ORE-00/01) ----

    #[test]
    fn resolve_algorithm_defaults_to_the_first_listed() {
        let algorithm = resolve_algorithm(RANK_ENDPOINT, None).expect("default resolves");
        assert_eq!(algorithm.id, "pagerank");
    }

    #[test]
    fn resolve_algorithm_rejects_unknown_algorithm_with_422() {
        let error = resolve_algorithm(RANK_ENDPOINT, Some("does-not-exist"))
            .expect_err("unknown algorithm is rejected");
        assert_eq!(error.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[test]
    fn error_status_codes_match_the_spec() {
        assert_eq!(
            OpenRankingError::malformed_body("x").status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            OpenRankingError::payload_too_large("x").status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
        assert_eq!(
            OpenRankingError::unprocessable("x").status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
    }

    #[test]
    fn parse_json_body_rejects_malformed_json_with_400() {
        let error = parse_json_body::<RankRequest>(b"not json").expect_err("malformed rejected");
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    }

    // ---- /rank (ORE-02) ----

    #[tokio::test]
    async fn rank_rejects_empty_pubkeys_with_422() {
        let repo = MockRepo::default();
        let error = rank(&repo, rank_request(&[], None))
            .await
            .expect_err("empty pubkeys rejected");
        assert_eq!(error.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn rank_rejects_more_than_1000_pubkeys_with_413() {
        let repo = MockRepo::default();
        let pubkeys: Vec<PublicKey> = (0..1001).map(|_| fresh_pubkey()).collect();
        let error = rank(&repo, rank_request(&pubkeys, None))
            .await
            .expect_err("oversized request rejected");
        assert_eq!(error.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn rank_rejects_non_positive_limit_with_422() {
        let repo = MockRepo::default();
        let error = rank(&repo, rank_request(&[fresh_pubkey()], Some(0)))
            .await
            .expect_err("non-positive limit rejected");
        assert_eq!(error.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn rank_rejects_unknown_algorithm_with_422() {
        let repo = MockRepo::default();
        let mut request = rank_request(&[fresh_pubkey()], None);
        request.algorithm = Some("does-not-exist".into());
        let error = rank(&repo, request)
            .await
            .expect_err("unknown algorithm rejected");
        assert_eq!(error.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn rank_returns_a_rank_for_every_pubkey_sorted_desc_with_unknowns_at_zero() {
        let known = fresh_pubkey();
        let other = fresh_pubkey();
        let unknown = fresh_pubkey();
        let repo = MockRepo {
            pageranks: HashMap::from([(known, 0.9), (other, 0.4)]),
            ..Default::default()
        };

        let response = rank(&repo, rank_request(&[unknown, other, known], None))
            .await
            .expect("ranking succeeds");

        assert_eq!(response.profiles.len(), 3, "a rank for every pubkey");
        assert_eq!(response.profiles[0].pubkey, known.to_hex());
        assert_eq!(response.profiles[0].rank, 0.9);
        assert_eq!(response.profiles[1].pubkey, other.to_hex());
        assert_eq!(response.profiles[1].rank, 0.4);
        assert_eq!(response.profiles[2].pubkey, unknown.to_hex());
        assert_eq!(response.profiles[2].rank, 0.0, "unknown pubkey backfilled");
    }

    #[tokio::test]
    async fn rank_clamps_results_to_limit() {
        let a = fresh_pubkey();
        let b = fresh_pubkey();
        let c = fresh_pubkey();
        let repo = MockRepo {
            pageranks: HashMap::from([(a, 0.1), (b, 0.5), (c, 0.9)]),
            ..Default::default()
        };

        let response = rank(&repo, rank_request(&[a, b, c], Some(2)))
            .await
            .expect("ranking succeeds");

        assert_eq!(response.profiles.len(), 2);
        assert_eq!(response.profiles[0].pubkey, c.to_hex());
        assert_eq!(response.profiles[1].pubkey, b.to_hex());
    }

    #[tokio::test]
    async fn rank_deduplicates_repeated_pubkeys() {
        let a = fresh_pubkey();
        let repo = MockRepo {
            pageranks: HashMap::from([(a, 0.3)]),
            ..Default::default()
        };

        let response = rank(&repo, rank_request(&[a, a, a], None))
            .await
            .expect("ranking succeeds");

        assert_eq!(response.profiles.len(), 1);
    }

    #[tokio::test]
    async fn rank_maps_repo_failure_to_500() {
        let repo = MockRepo {
            fail: true,
            ..Default::default()
        };
        let error = rank(&repo, rank_request(&[fresh_pubkey()], None))
            .await
            .expect_err("repo failure surfaces");
        assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    // ---- /reputation (ORE-06) ----

    fn account(pubkey: PublicKey, pagerank: f64, followers: u64, follows: u64) -> AccountInfo {
        AccountInfo::new(pubkey)
            .with_pagerank(Some(pagerank))
            .with_follower_count(Some(followers))
            .with_followee_count(Some(follows))
    }

    fn reputation_request(pubkey: &str) -> ReputationRequest {
        ReputationRequest {
            pubkey: Some(pubkey.to_string()),
            algorithm: None,
        }
    }

    #[tokio::test]
    async fn reputation_returns_rank_stats_and_trust_for_a_known_account() {
        let pubkey = fresh_pubkey();
        let repo = MockRepo {
            accounts: HashMap::from([(pubkey, account(pubkey, 0.5, 100, 20))]),
            ..Default::default()
        };

        let response = reputation(&repo, reputation_request(&pubkey.to_hex()))
            .await
            .expect("reputation succeeds");

        assert_eq!(response.pubkey, pubkey.to_hex());
        assert_eq!(response.rank, 0.5);
        let stats = response.stats.expect("known account has stats");
        assert_eq!(stats.followers, Some(100));
        assert_eq!(stats.follows, Some(20));
        assert!(
            response.x.expect("known account has extensions").trusted,
            "pagerank 0.5 is trusted"
        );
    }

    #[tokio::test]
    async fn reputation_low_pagerank_is_not_trusted() {
        let pubkey = fresh_pubkey();
        let repo = MockRepo {
            accounts: HashMap::from([(pubkey, account(pubkey, 0.1, 3, 9))]),
            ..Default::default()
        };

        let response = reputation(&repo, reputation_request(&pubkey.to_hex()))
            .await
            .expect("reputation succeeds");

        assert!(!response.x.expect("extensions present").trusted);
    }

    #[tokio::test]
    async fn reputation_unknown_pubkey_returns_zero_rank_and_omits_stats_and_x() {
        let repo = MockRepo::default();

        let response = reputation(&repo, reputation_request(&fresh_pubkey().to_hex()))
            .await
            .expect("reputation succeeds");

        assert_eq!(response.rank, 0.0);
        let json = serde_json::to_value(&response).unwrap();
        assert!(json.get("stats").is_none(), "stats omitted for unknown");
        assert!(json.get("x").is_none(), "x omitted for unknown");
    }

    #[tokio::test]
    async fn reputation_missing_pubkey_is_422() {
        let repo = MockRepo::default();
        let request = ReputationRequest {
            pubkey: None,
            algorithm: None,
        };
        let error = reputation(&repo, request)
            .await
            .expect_err("missing pubkey rejected");
        assert_eq!(error.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn reputation_invalid_pubkey_is_422() {
        let repo = MockRepo::default();
        let error = reputation(&repo, reputation_request("not-a-pubkey"))
            .await
            .expect_err("invalid pubkey rejected");
        assert_eq!(error.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn reputation_unknown_algorithm_is_422() {
        let repo = MockRepo::default();
        let request = ReputationRequest {
            pubkey: Some(fresh_pubkey().to_hex()),
            algorithm: Some("does-not-exist".into()),
        };
        let error = reputation(&repo, request)
            .await
            .expect_err("unknown algorithm rejected");
        assert_eq!(error.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    // ---- /followers (ORE-04) ----

    fn followers_request(pubkey: &str, limit: Option<i64>) -> FollowersRequest {
        FollowersRequest {
            pubkey: Some(pubkey.to_string()),
            algorithm: None,
            limit,
        }
    }

    #[tokio::test]
    async fn followers_returns_top_followers_in_order_with_total() {
        let target = fresh_pubkey();
        let a = fresh_pubkey();
        let b = fresh_pubkey();
        let c = fresh_pubkey();
        let repo = MockRepo {
            top_followers: HashMap::from([(target, vec![(a, 0.9), (b, 0.5), (c, 0.2)])]),
            ..Default::default()
        };

        let response = followers(&repo, followers_request(&target.to_hex(), None))
            .await
            .expect("followers succeeds");

        assert_eq!(response.total, Some(3));
        let pubkeys: Vec<String> = response.profiles.iter().map(|p| p.pubkey.clone()).collect();
        assert_eq!(pubkeys, vec![a.to_hex(), b.to_hex(), c.to_hex()]);
        assert_eq!(response.profiles[0].rank, 0.9);
    }

    #[tokio::test]
    async fn followers_respects_limit_but_total_reflects_full_set() {
        let target = fresh_pubkey();
        let a = fresh_pubkey();
        let b = fresh_pubkey();
        let c = fresh_pubkey();
        let repo = MockRepo {
            top_followers: HashMap::from([(target, vec![(a, 0.9), (b, 0.5), (c, 0.2)])]),
            ..Default::default()
        };

        let response = followers(&repo, followers_request(&target.to_hex(), Some(2)))
            .await
            .expect("followers succeeds");

        assert_eq!(response.profiles.len(), 2);
        assert_eq!(response.total, Some(3), "total is the full follower set");
    }

    #[tokio::test]
    async fn followers_unknown_target_returns_empty_and_omits_total() {
        let repo = MockRepo::default();

        let response = followers(&repo, followers_request(&fresh_pubkey().to_hex(), None))
            .await
            .expect("followers succeeds");

        assert!(response.profiles.is_empty());
        let json = serde_json::to_value(&response).unwrap();
        assert!(
            json.get("total").is_none(),
            "total omitted for unknown target"
        );
    }

    #[tokio::test]
    async fn followers_missing_pubkey_is_422() {
        let repo = MockRepo::default();
        let request = FollowersRequest {
            pubkey: None,
            algorithm: None,
            limit: None,
        };
        let error = followers(&repo, request)
            .await
            .expect_err("missing pubkey rejected");
        assert_eq!(error.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn followers_non_positive_limit_is_422() {
        let repo = MockRepo::default();
        let error = followers(&repo, followers_request(&fresh_pubkey().to_hex(), Some(0)))
            .await
            .expect_err("non-positive limit rejected");
        assert_eq!(error.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn followers_limit_above_maximum_is_422() {
        let repo = MockRepo::default();
        let error = followers(
            &repo,
            followers_request(&fresh_pubkey().to_hex(), Some(1001)),
        )
        .await
        .expect_err("oversized limit rejected");
        assert_eq!(error.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn followers_unknown_algorithm_is_422() {
        let repo = MockRepo::default();
        let request = FollowersRequest {
            pubkey: Some(fresh_pubkey().to_hex()),
            algorithm: Some("does-not-exist".into()),
            limit: None,
        };
        let error = followers(&repo, request)
            .await
            .expect_err("unknown algorithm rejected");
        assert_eq!(error.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }
}
