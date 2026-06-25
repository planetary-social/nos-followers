//! OpenRanking protocol support (<https://github.com/Open-Ranking/protocol>).
//!
//! Exposes the subset of the protocol that maps onto our follow graph:
//! capability discovery (ORE-01) plus the `/rank`, `/reputation`, `/followers`
//! and `/recommend` endpoints. See OREs 00-06 for the full specification.

use super::AppState;
use crate::recommendation_queue::{RecommendationQueue, RecommendationStatus};
use crate::relay_subscriber::GetEventsOf;
use crate::repo::RepoTrait;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header::CACHE_CONTROL, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use moka::future::Cache;
use nostr_sdk::PublicKey;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
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
                description: Some("Personalised follow recommendations from your social graph; returns up to 10 profiles."),
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
/// The discovery document is near-static config, so it carries the same hourly
/// cache hint as the PageRank endpoints it advertises.
pub(crate) async fn serve_capability_document() -> Response {
    cached_json(capability_document(), Some(RANK_TTL_SECONDS))
}

/// Maximum number of pubkeys accepted by `/rank` in a single request (ORE-02).
const MAX_PUBKEYS: usize = 1000;

/// TTL hint (seconds) for PageRank-derived responses. PageRank is recomputed on
/// a daily cron, so an hour is a safe "still fresh" hint for clients.
const RANK_TTL_SECONDS: u64 = 3600;

/// Default and maximum `limit` for `/followers`, which pages over its own
/// candidate set.
const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 1000;

/// Default and maximum `limit` for `/recommend`. [`RepoTrait::get_recommendations`]
/// caps results at 10 (the Jaccard-similarity query is expensive), so the
/// endpoint advertises and enforces the same maximum rather than silently
/// truncating larger requests.
const RECOMMEND_MAX_LIMIT: usize = 10;

/// TTL hint (seconds) for a `/recommend` response that is still warming: the
/// recommendations are being computed in the background (see [`recommend`]), so
/// this empty result is only valid briefly and the client should retry soon, by
/// which point the shared cache is populated.
const RECOMMEND_WARMING_TTL_SECONDS: u64 = 10;

/// An ORE-02 Profile: a ranked Nostr identity. Higher `rank` is always better.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct Profile {
    /// 64-char lowercase hex pubkey (ORE-00).
    pub pubkey: String,
    pub rank: f64,
}

/// The shared `/rank`-shaped response: profiles sorted by rank desc + a TTL hint.
#[derive(Debug, Clone, Serialize)]
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
#[derive(Debug, Clone)]
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

/// Build a JSON success response that mirrors the ORE body `ttl` as an HTTP
/// `Cache-Control` hint. Attached per successful response (never to errors), so a
/// CDN/browser may reuse it for exactly the freshness window the body advertises
/// — and crucially so a short-lived warming `/recommend` (ttl=10) is not pinned
/// by a long max-age. The open-ranking group carries no blanket cache middleware
/// precisely so each response can advertise its own TTL and errors stay uncached.
fn cached_json<T: Serialize>(body: T, ttl: Option<u64>) -> Response {
    let mut response = Json(body).into_response();
    if let Some(ttl) = ttl {
        if let Ok(value) = HeaderValue::from_str(&format!("public, max-age={ttl}")) {
            response.headers_mut().insert(CACHE_CONTROL, value);
        }
    }
    response
}

/// Records the wall-clock latency of an open-ranking handler into the
/// per-endpoint `openranking_request_seconds` histogram when dropped, so every
/// exit path — success, validation error, or repo error — is measured.
struct LatencyGuard {
    endpoint: &'static str,
    started: std::time::Instant,
}

impl LatencyGuard {
    fn new(endpoint: &'static str) -> Self {
        Self {
            endpoint,
            started: std::time::Instant::now(),
        }
    }
}

impl Drop for LatencyGuard {
    fn drop(&mut self) {
        crate::metrics::openranking_request_seconds(self.endpoint)
            .record(self.started.elapsed().as_secs_f64());
    }
}

/// Server-side TTL for the synchronous read caches. Short by design: it only has
/// to span a burst window, and PageRank is recomputed daily, so a minute of reuse
/// bounds staleness well under that while still collapsing the herd.
const READ_CACHE_TTL_SECONDS: u64 = 60;

/// Max distinct cached read responses per endpoint (LRU-evicted). Bounds memory
/// for `/rank`, whose key is an arbitrary pubkey set.
const READ_CACHE_CAPACITY: u64 = 1_000;

/// Short-lived per-endpoint result caches for the synchronous heavy reads
/// (`/rank`, `/reputation`, `/followers`). Each `try_get_with` both single-flights
/// — concurrent identical requests collapse to one Neo4j query — and reuses the
/// result for [`READ_CACHE_TTL_SECONDS`]. Errors are never cached, so a failed or
/// timed-out query is retried rather than replayed.
///
/// Known limits: this does not rescue a single query that exceeds the 5s route
/// timeout (that future is dropped before it can populate the cache — mitigating
/// it would need a background compute like `/recommend`, or a longer timeout).
/// And because these endpoints are POST, standard shared HTTP caches will not
/// reuse them; this server-side cache is what actually collapses the herd.
#[derive(Clone)]
pub(crate) struct OpenRankingReadCache {
    rank: Cache<String, RankResponse>,
    reputation: Cache<String, ReputationResponse>,
    followers: Cache<String, FollowersResponse>,
}

impl OpenRankingReadCache {
    pub(crate) fn new() -> Self {
        fn build<V: Clone + Send + Sync + 'static>() -> Cache<String, V> {
            Cache::builder()
                .time_to_live(Duration::from_secs(READ_CACHE_TTL_SECONDS))
                .max_capacity(READ_CACHE_CAPACITY)
                .build()
        }
        Self {
            rank: build(),
            reputation: build(),
            followers: build(),
        }
    }
}

impl Default for OpenRankingReadCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Cache key for `/reputation`: pubkey + algorithm. A missing/invalid pubkey
/// errors before the repo call (and errors are never cached), so the empty
/// placeholder for `None` is harmless.
fn reputation_cache_key(request: &ReputationRequest) -> String {
    format!(
        "{}|{}",
        request.pubkey.as_deref().unwrap_or(""),
        request.algorithm.as_deref().unwrap_or("")
    )
}

/// Cache key for `/followers`: pubkey + limit + algorithm.
fn followers_cache_key(request: &FollowersRequest) -> String {
    format!(
        "{}|{}|{}",
        request.pubkey.as_deref().unwrap_or(""),
        request.limit.map(|l| l.to_string()).unwrap_or_default(),
        request.algorithm.as_deref().unwrap_or("")
    )
}

/// Cache key for `/rank`: the order-independent pubkey set plus the algorithm
/// and limit, rendered as a canonical string (mirroring `reputation_cache_key`
/// and `followers_cache_key`). The response is determined by the set (it is
/// sorted by rank and unknowns rank 0), so request order must not split the
/// cache. Keying on the canonical representation rather than a 64-bit hash keeps
/// distinct requests in distinct entries: a digest could collide and serve one
/// request's ranks for another's.
fn rank_cache_key(request: &RankRequest) -> String {
    let mut pubkeys: Vec<&str> = request.pubkeys.iter().map(String::as_str).collect();
    pubkeys.sort_unstable();
    pubkeys.dedup();

    format!(
        "{}|{}|{}",
        pubkeys.join(","),
        request.algorithm.as_deref().unwrap_or(""),
        request.limit.map(|l| l.to_string()).unwrap_or_default(),
    )
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
) -> Result<Response, OpenRankingError>
where
    T: RepoTrait,
    U: GetEventsOf,
{
    let _latency = LatencyGuard::new(RANK_ENDPOINT);
    let request: RankRequest = parse_json_body(&body)?;
    let key = rank_cache_key(&request);
    let repo = state.repo.clone();
    let response = state
        .open_ranking_read_cache
        .rank
        .try_get_with(key, async move { rank(repo.as_ref(), request).await })
        .await
        .map_err(|err| (*err).clone())?;
    let ttl = response.ttl;
    Ok(cached_json(response, ttl))
}

/// An ORE-06 `stats` object: non-negative integer counts from our index.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct ReputationStats {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub followers: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub follows: Option<u64>,
}

/// Provider-specific `x` extensions for `/reputation` (ORE-06).
#[derive(Debug, Clone, Serialize)]
pub(crate) struct ReputationExtensions {
    /// Our boolean trust verdict (PageRank-based; see `AccountInfo::is_trusted`).
    pub trusted: bool,
}

/// Response body for `POST /reputation` (ORE-06). `pubkey` and `rank` are
/// always present; the rest are omitted when we have no data.
#[derive(Debug, Clone, Serialize)]
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
) -> Result<Response, OpenRankingError>
where
    T: RepoTrait,
    U: GetEventsOf,
{
    let _latency = LatencyGuard::new(REPUTATION_ENDPOINT);
    let request: ReputationRequest = parse_json_body(&body)?;
    let key = reputation_cache_key(&request);
    let repo = state.repo.clone();
    let response = state
        .open_ranking_read_cache
        .reputation
        .try_get_with(key, async move { reputation(repo.as_ref(), request).await })
        .await
        .map_err(|err| (*err).clone())?;
    let ttl = response.ttl;
    Ok(cached_json(response, ttl))
}

/// Response body for `POST /followers` (ORE-04).
#[derive(Debug, Clone, Serialize)]
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

/// Validate an optional `limit`, applying the given default and maximum.
fn resolve_limit(
    limit: Option<i64>,
    default: usize,
    max: usize,
) -> Result<usize, OpenRankingError> {
    match limit {
        None => Ok(default),
        Some(limit) if limit < 1 => Err(OpenRankingError::unprocessable(
            "limit must be a positive integer",
        )),
        Some(limit) if limit as usize > max => Err(OpenRankingError::unprocessable(format!(
            "limit exceeds the maximum of {max}"
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
    let limit = resolve_limit(request.limit, DEFAULT_LIMIT, MAX_LIMIT)?;

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
) -> Result<Response, OpenRankingError>
where
    T: RepoTrait,
    U: GetEventsOf,
{
    let _latency = LatencyGuard::new(FOLLOWERS_ENDPOINT);
    let request: FollowersRequest = parse_json_body(&body)?;
    let key = followers_cache_key(&request);
    let repo = state.repo.clone();
    let response = state
        .open_ranking_read_cache
        .followers
        .try_get_with(key, async move { followers(repo.as_ref(), request).await })
        .await
        .map_err(|err| (*err).clone())?;
    let ttl = response.ttl;
    Ok(cached_json(response, ttl))
}

/// Request body for `POST /recommend` (ORE-03). Unknown fields (e.g. `topic`,
/// which our algorithms do not use) are ignored for forward compatibility.
#[derive(Debug, Deserialize)]
pub(crate) struct RecommendRequest {
    #[serde(default)]
    pub algorithm: Option<String>,
    #[serde(default)]
    pub pov: Option<String>,
    #[serde(default)]
    pub limit: Option<i64>,
}

/// Core `/recommend` logic (ORE-03): personalised follow recommendations.
///
/// The protocol has no pending/processing state, so `/recommend` cannot return a
/// 202 like `/api/v1/recommendations`. Rather than block on the heavy Jaccard
/// query under the 5s route timeout — where a cold, heavy account could time out
/// and, because the dropped request future never reaches the cache write, leave
/// nothing cached and wedge every retry — the compute is routed through the
/// shared [`RecommendationQueue`]: a cache hit returns the profiles, while a miss
/// enqueues a background warm-up (run in the worker, immune to the route timeout,
/// writing the shared cache on completion) and returns a fast partial result
/// (empty profiles + a short TTL) so the client retries once the cache is warm.
pub(crate) async fn recommend<T: RepoTrait>(
    queue: &RecommendationQueue<T>,
    request: RecommendRequest,
) -> Result<RankResponse, OpenRankingError> {
    let algorithm = resolve_algorithm(RECOMMEND_ENDPOINT, request.algorithm.as_deref())?;

    // ORE-01: a personalised algorithm requires a pov pubkey.
    let pov = match (algorithm.pov, request.pov.as_deref()) {
        (true, None) => {
            return Err(OpenRankingError::unprocessable(format!(
                "algorithm '{}' requires a pov",
                algorithm.id
            )))
        }
        (true, Some(pov)) => PublicKey::from_hex(pov)
            .map_err(|e| OpenRankingError::unprocessable(format!("pov is invalid: {e}")))?,
        // Every /recommend algorithm we offer is personalised; a global one would
        // have no pubkey to recommend for.
        (false, _) => {
            return Err(OpenRankingError::unprocessable(
                "global recommendation algorithms are not supported",
            ))
        }
    };

    // `Repo::get_recommendations` caps results at RECOMMEND_MAX_LIMIT, so enforce
    // the same maximum here instead of silently truncating larger requests.
    let limit = resolve_limit(request.limit, RECOMMEND_MAX_LIMIT, RECOMMEND_MAX_LIMIT)?;

    // Enqueue (or join) a background compute via the shared queue. A hit returns
    // the cached recommendations; anything else means a warm-up is in flight, so
    // return a fast empty result rather than blocking on the heavy query.
    let status = queue
        .request_recommendations(pov, None, Some(limit))
        .await
        .map_err(|e| {
            error!("/recommend request_recommendations failed: {e}");
            OpenRankingError::Internal
        })?;

    let (recommendations, ttl) = match status {
        RecommendationStatus::Completed(recommendations) => (recommendations, RANK_TTL_SECONDS),
        // Queued (Pending), running (Processing), or the queue is saturated
        // (QueueFull): return an empty result and let the short TTL bring the
        // client back once the shared cache is warm.
        RecommendationStatus::Pending { .. }
        | RecommendationStatus::Processing
        | RecommendationStatus::QueueFull => {
            crate::metrics::recommend_cold_miss().increment(1);
            (Vec::new(), RECOMMEND_WARMING_TTL_SECONDS)
        }
        RecommendationStatus::Failed(reason) => {
            // The queue self-heals (request_recommendations re-enqueues on a
            // cached Failed), so a Failed reaching here is a transient blip: log
            // it, then return the same empty + short-TTL warming response as the
            // other in-flight states so the client retries per the
            // retry-after-ttl contract instead of receiving a sustained 500.
            error!("/recommend background computation failed, will retry: {reason}");
            // Count as a failure, not a warming cold-miss, so the two are not
            // conflated. (The worker already counts the failure when it occurs;
            // request_recommendations self-heals a cached Failed into a re-enqueue,
            // so this branch is rarely reached.)
            crate::metrics::recommend_failed().increment(1);
            (Vec::new(), RECOMMEND_WARMING_TTL_SECONDS)
        }
    };

    let profiles = recommendations
        .into_iter()
        .map(|recommendation| Profile {
            pubkey: recommendation.pubkey.to_hex(),
            rank: recommendation.similarity,
        })
        .collect();

    Ok(RankResponse {
        profiles,
        ttl: Some(ttl),
    })
}

/// `POST /recommend` (ORE-03).
pub(crate) async fn serve_recommend<T, U>(
    State(state): State<Arc<AppState<T, U>>>,
    body: Bytes,
) -> Result<Response, OpenRankingError>
where
    T: RepoTrait,
    U: GetEventsOf,
{
    let _latency = LatencyGuard::new(RECOMMEND_ENDPOINT);
    let request: RecommendRequest = parse_json_body(&body)?;
    let response = recommend(state.recommendation_queue.as_ref(), request).await?;
    let ttl = response.ttl;
    Ok(cached_json(response, ttl))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::AccountInfo;
    use crate::repo::{Recommendation, RecommendationReason, RepoError, RepoTrait, TopFollowers};
    use axum::http::StatusCode;
    use nostr_sdk::{Keys, PublicKey};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;
    use tokio_util::task::TaskTracker;

    fn fresh_pubkey() -> PublicKey {
        Keys::generate().public_key()
    }

    /// Wrap a `MockRepo` in a `RecommendationQueue` with an empty result cache and
    /// a running background worker, so `recommend` exercises the same async warm-up
    /// path as production: a cache miss enqueues a background compute that writes
    /// the shared result cache rather than blocking the request.
    fn recommendation_queue(repo: MockRepo) -> RecommendationQueue<MockRepo> {
        let result_cache = moka::future::Cache::builder().build();
        let (queue, receiver) = RecommendationQueue::new(Arc::new(repo), result_cache, 16);
        queue.start_workers(
            TaskTracker::new(),
            receiver,
            CancellationToken::new(),
            std::num::NonZeroUsize::new(1).unwrap(),
        );
        queue
    }

    /// Poll `/recommend` until the background worker has warmed the shared cache
    /// and the endpoint returns a non-empty result. Used by the happy-path tests,
    /// whose povs always have recommendations to compute.
    async fn recommend_until_warm(
        queue: &RecommendationQueue<MockRepo>,
        pov: &PublicKey,
        limit: Option<i64>,
    ) -> RankResponse {
        for _ in 0..1000 {
            let response = recommend(queue, recommend_request(Some(&pov.to_hex()), limit))
                .await
                .expect("recommend succeeds");
            if !response.profiles.is_empty() {
                return response;
            }
            tokio::task::yield_now().await;
        }
        panic!("background warm-up never populated the recommendation cache");
    }

    /// Render the Prometheus metrics emitted while running `body` under a fresh,
    /// test-local recorder. Lets a test assert which metrics a code path emits
    /// without installing the global recorder, so tests stay isolated and never
    /// race over shared metric state.
    fn render_metrics_for<F, Fut>(body: F) -> String
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        ::metrics::with_local_recorder(&recorder, || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("current-thread runtime")
                .block_on(body());
        });
        handle.render()
    }

    /// A repo stub that knows the PageRank of a fixed set of pubkeys and, like
    /// the real Cypher query, returns only the requested ones it knows about.
    #[derive(Default)]
    struct MockRepo {
        pageranks: HashMap<PublicKey, f64>,
        accounts: HashMap<PublicKey, AccountInfo>,
        /// Full follower set per target, sorted by rank desc (as the real query returns).
        top_followers: HashMap<PublicKey, Vec<(PublicKey, f64)>>,
        /// Recommendations per pov pubkey, sorted by similarity desc.
        recommendations: HashMap<PublicKey, Vec<Recommendation>>,
        /// Counts calls to `get_recommendations`, to assert cache reuse.
        recommendation_calls: Arc<AtomicUsize>,
        /// Counts calls to the synchronous heavy reads, to assert the
        /// single-flight + result cache collapses identical concurrent/repeat
        /// requests into one Neo4j query.
        pagerank_calls: Arc<AtomicUsize>,
        account_calls: Arc<AtomicUsize>,
        top_followers_calls: Arc<AtomicUsize>,
        /// Number of leading `get_recommendations` calls that fail transiently
        /// before recovering, to exercise the queue's self-healing Failed path.
        recommendation_failures: Arc<AtomicUsize>,
        fail: bool,
    }

    impl RepoTrait for MockRepo {
        async fn get_pageranks(
            &self,
            pubkeys: &[PublicKey],
        ) -> Result<HashMap<PublicKey, f64>, RepoError> {
            self.pagerank_calls.fetch_add(1, Ordering::SeqCst);
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
            self.account_calls.fetch_add(1, Ordering::SeqCst);
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
            self.top_followers_calls.fetch_add(1, Ordering::SeqCst);
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

        async fn get_recommendations(
            &self,
            pubkey: &PublicKey,
            _min_pagerank: Option<f64>,
            limit: Option<usize>,
        ) -> Result<Vec<Recommendation>, RepoError> {
            self.recommendation_calls.fetch_add(1, Ordering::Relaxed);
            if self.fail {
                return Err(RepoError::InvalidParameter("boom".into()));
            }
            // A bounded run of transient failures that then recover, so a test
            // can drive the queue Failed path and assert it self-heals.
            if self.recommendation_failures.load(Ordering::Relaxed) > 0 {
                self.recommendation_failures.fetch_sub(1, Ordering::Relaxed);
                return Err(RepoError::InvalidParameter("transient boom".into()));
            }
            let recommendations = self
                .recommendations
                .get(pubkey)
                .cloned()
                .unwrap_or_default();
            // Mirror Repo::get_recommendations: default 10, capped at 10.
            let limit = limit.unwrap_or(10).min(10);
            Ok(recommendations.into_iter().take(limit).collect())
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

    // ---- /rank cache key (canonical, collision-safe — not a 64-bit hash) ----

    #[test]
    fn rank_cache_key_is_canonical_sorted_and_deduplicated() {
        // The key is the canonical request representation — the sorted,
        // de-duplicated pubkey set plus algorithm and limit — so distinct
        // requests always land in distinct cache entries. A 64-bit hash digest
        // (the previous implementation) risks silent collisions that would
        // serve one request's ranks for another's.
        let key = rank_cache_key(&RankRequest {
            pubkeys: vec!["bbbb".into(), "aaaa".into(), "bbbb".into()],
            algorithm: Some("pagerank".into()),
            limit: Some(5),
        });

        assert_eq!(key, "aaaa,bbbb|pagerank|5");
    }

    #[test]
    fn rank_cache_key_renders_absent_algorithm_and_limit_as_empty_fields() {
        // Absent optionals render as empty fields (mirroring the sibling keys),
        // and a present limit is always >= 1 (smaller values are rejected before
        // caching), so `None` can never collide with a real limit.
        let key = rank_cache_key(&RankRequest {
            pubkeys: vec!["aaaa".into()],
            algorithm: None,
            limit: None,
        });

        assert_eq!(key, "aaaa||");
    }

    #[test]
    fn rank_cache_key_distinguishes_requests_that_differ_in_any_field() {
        let base = RankRequest {
            pubkeys: vec!["aaaa".into(), "bbbb".into()],
            algorithm: Some("pagerank".into()),
            limit: Some(5),
        };
        let key = rank_cache_key(&base);

        // A different pubkey set, algorithm, or limit must each change the key:
        // no field may be lost to a collision.
        assert_ne!(
            rank_cache_key(&RankRequest {
                pubkeys: vec!["aaaa".into(), "cccc".into()],
                algorithm: Some("pagerank".into()),
                limit: Some(5),
            }),
            key,
            "a different pubkey set must change the key"
        );
        assert_ne!(
            rank_cache_key(&RankRequest {
                pubkeys: vec!["aaaa".into(), "bbbb".into()],
                algorithm: Some("followers".into()),
                limit: Some(5),
            }),
            key,
            "a different algorithm must change the key"
        );
        assert_ne!(
            rank_cache_key(&RankRequest {
                pubkeys: vec!["aaaa".into(), "bbbb".into()],
                algorithm: Some("pagerank".into()),
                limit: Some(6),
            }),
            key,
            "a different limit must change the key"
        );
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

    // ---- /recommend (ORE-03) ----

    fn recommendation(pubkey: PublicKey, similarity: f64) -> Recommendation {
        Recommendation {
            pubkey,
            friendly_id: pubkey.to_hex(),
            similarity,
            pagerank: 0.5,
            reason_type: RecommendationReason::HighSimilarity,
        }
    }

    fn recommend_request(pov: Option<&str>, limit: Option<i64>) -> RecommendRequest {
        RecommendRequest {
            algorithm: None,
            pov: pov.map(|pov| pov.to_string()),
            limit,
        }
    }

    #[tokio::test]
    async fn recommend_does_not_block_on_a_cold_miss_and_warms_in_the_background() {
        // A cold cache miss must NOT block on the heavy compute under the route
        // timeout (which, on timeout, would also drop the result and cache
        // nothing, wedging every retry). The first request returns immediately
        // with a fast empty result; the background worker then warms the shared
        // cache so a later request is served.
        let pov = fresh_pubkey();
        let calls = Arc::new(AtomicUsize::new(0));
        let repo = MockRepo {
            recommendations: HashMap::from([(pov, vec![recommendation(fresh_pubkey(), 0.8)])]),
            recommendation_calls: calls.clone(),
            ..Default::default()
        };
        let queue = recommendation_queue(repo);

        let first = recommend(&queue, recommend_request(Some(&pov.to_hex()), None))
            .await
            .expect("recommend succeeds");
        assert!(
            first.profiles.is_empty(),
            "a cold miss returns a fast empty result instead of blocking on the compute"
        );

        let warmed = recommend_until_warm(&queue, &pov, None).await;
        assert_eq!(
            warmed.profiles.len(),
            1,
            "the background warm-up produced the recommendation"
        );
        assert_eq!(
            calls.load(Ordering::Relaxed),
            1,
            "the heavy query ran exactly once, in the background"
        );
    }

    #[tokio::test]
    async fn recommend_recovers_from_a_transient_failure_instead_of_wedging_on_a_500() {
        // A transient compute failure caches a Failed status. The queue must
        // treat that Failed as a miss and re-enqueue on the next request, so a
        // retry recomputes and warms the cache. Before the fix, the cached
        // Failed short-circuited every retry into a sustained HTTP 500 until the
        // status TTL expired, and /recommend never warmed.
        let pov = fresh_pubkey();
        let repo = MockRepo {
            recommendations: HashMap::from([(pov, vec![recommendation(fresh_pubkey(), 0.8)])]),
            // The first background compute fails, then recovers.
            recommendation_failures: Arc::new(AtomicUsize::new(1)),
            ..Default::default()
        };
        let queue = recommendation_queue(repo);

        // Even while the compute is failing, the warming response is an empty
        // result, never a 500.
        let first = recommend(&queue, recommend_request(Some(&pov.to_hex()), None))
            .await
            .expect("a transient failure yields the warming response, not a 500");
        assert!(
            first.profiles.is_empty(),
            "the warming response carries no profiles"
        );

        // Retries re-enqueue and the recovered compute warms the cache.
        let warmed = recommend_until_warm(&queue, &pov, None).await;
        assert_eq!(
            warmed.profiles.len(),
            1,
            "the recommendation warmed after the transient failure recovered"
        );
    }

    #[test]
    fn recommend_emits_a_cold_miss_metric_on_the_warming_path() {
        // A cold miss returns the empty + short-TTL warming response; that path
        // must bump the cold-miss counter so operators can watch its rate.
        let pov = fresh_pubkey();
        let rendered = render_metrics_for(|| async move {
            let queue = recommendation_queue(MockRepo::default());
            let response = recommend(&queue, recommend_request(Some(&pov.to_hex()), None))
                .await
                .expect("cold miss returns the warming response");
            assert!(response.profiles.is_empty(), "a cold miss is empty");
        });
        assert!(
            rendered.contains("recommend_cold_miss"),
            "the warming path increments the cold-miss counter:\n{rendered}"
        );
    }

    #[test]
    fn warm_compute_records_the_warm_latency_histogram() {
        // When the background worker completes a compute it records how long the
        // warm-up took, so operators can watch warm latency.
        let pov = fresh_pubkey();
        let rendered = render_metrics_for(|| async move {
            let repo = MockRepo {
                recommendations: HashMap::from([(pov, vec![recommendation(fresh_pubkey(), 0.8)])]),
                ..Default::default()
            };
            let queue = recommendation_queue(repo);
            recommend_until_warm(&queue, &pov, None).await;
        });
        assert!(
            rendered.contains("recommend_warm_seconds"),
            "a completed warm-up records the latency histogram:\n{rendered}"
        );
    }

    #[tokio::test]
    async fn recommend_maps_recommendations_to_profiles_ranked_by_similarity() {
        let pov = fresh_pubkey();
        let a = fresh_pubkey();
        let b = fresh_pubkey();
        let repo = MockRepo {
            recommendations: HashMap::from([(
                pov,
                vec![recommendation(a, 0.8), recommendation(b, 0.3)],
            )]),
            ..Default::default()
        };

        let queue = recommendation_queue(repo);
        let response = recommend_until_warm(&queue, &pov, None).await;

        let pubkeys: Vec<String> = response.profiles.iter().map(|p| p.pubkey.clone()).collect();
        assert_eq!(pubkeys, vec![a.to_hex(), b.to_hex()]);
        assert_eq!(response.profiles[0].rank, 0.8);
        assert_eq!(response.profiles[1].rank, 0.3);
    }

    #[tokio::test]
    async fn recommend_without_pov_is_422() {
        let repo = MockRepo::default();
        let error = recommend(&recommendation_queue(repo), recommend_request(None, None))
            .await
            .expect_err("missing pov rejected");
        assert_eq!(error.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn recommend_invalid_pov_is_422() {
        let repo = MockRepo::default();
        let error = recommend(
            &recommendation_queue(repo),
            recommend_request(Some("not-a-pubkey"), None),
        )
        .await
        .expect_err("invalid pov rejected");
        assert_eq!(error.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn recommend_respects_limit() {
        let pov = fresh_pubkey();
        let repo = MockRepo {
            recommendations: HashMap::from([(
                pov,
                vec![
                    recommendation(fresh_pubkey(), 0.9),
                    recommendation(fresh_pubkey(), 0.6),
                    recommendation(fresh_pubkey(), 0.4),
                ],
            )]),
            ..Default::default()
        };

        let queue = recommendation_queue(repo);
        let response = recommend_until_warm(&queue, &pov, Some(1)).await;

        assert_eq!(response.profiles.len(), 1);
    }

    #[tokio::test]
    async fn recommend_non_positive_limit_is_422() {
        let repo = MockRepo::default();
        let error = recommend(
            &recommendation_queue(repo),
            recommend_request(Some(&fresh_pubkey().to_hex()), Some(0)),
        )
        .await
        .expect_err("non-positive limit rejected");
        assert_eq!(error.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn recommend_unknown_algorithm_is_422() {
        let repo = MockRepo::default();
        let request = RecommendRequest {
            algorithm: Some("does-not-exist".into()),
            pov: Some(fresh_pubkey().to_hex()),
            limit: None,
        };
        let error = recommend(&recommendation_queue(repo), request)
            .await
            .expect_err("unknown algorithm rejected");
        assert_eq!(error.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn recommend_rejects_limit_above_cap_with_422() {
        // Above RECOMMEND_MAX_LIMIT must be an explicit error, not silent
        // truncation down to the cap.
        let repo = MockRepo::default();
        let error = recommend(
            &recommendation_queue(repo),
            recommend_request(
                Some(&fresh_pubkey().to_hex()),
                Some(RECOMMEND_MAX_LIMIT as i64 + 1),
            ),
        )
        .await
        .expect_err("limit above the recommend cap rejected");
        assert_eq!(error.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn recommend_default_limit_caps_results_at_the_maximum() {
        // More recommendations available than the cap; the default limit must
        // return at most RECOMMEND_MAX_LIMIT, never silently more.
        let pov = fresh_pubkey();
        let recommendations: Vec<Recommendation> = (0..RECOMMEND_MAX_LIMIT + 5)
            .map(|i| recommendation(fresh_pubkey(), 0.9 - i as f64 * 0.01))
            .collect();
        let repo = MockRepo {
            recommendations: HashMap::from([(pov, recommendations)]),
            ..Default::default()
        };

        let queue = recommendation_queue(repo);
        let response = recommend_until_warm(&queue, &pov, None).await;

        assert_eq!(response.profiles.len(), RECOMMEND_MAX_LIMIT);
    }

    #[tokio::test]
    async fn recommend_serves_repeated_requests_from_cache() {
        // Once warmed, an identical request hits the shared result cache rather
        // than recomputing the expensive query.
        let pov = fresh_pubkey();
        let calls = Arc::new(AtomicUsize::new(0));
        let repo = MockRepo {
            recommendations: HashMap::from([(pov, vec![recommendation(fresh_pubkey(), 0.8)])]),
            recommendation_calls: calls.clone(),
            ..Default::default()
        };
        let queue = recommendation_queue(repo);

        let warmed = recommend_until_warm(&queue, &pov, None).await;
        let again = recommend(&queue, recommend_request(Some(&pov.to_hex()), None))
            .await
            .expect("second recommend succeeds");

        assert_eq!(warmed.profiles, again.profiles, "cached result matches");
        assert_eq!(
            calls.load(Ordering::Relaxed),
            1,
            "the heavy query ran once; repeats hit the shared result cache"
        );
    }

    // ---- Cache-Control header: ORE body `ttl` mirrored as an HTTP cache hint ----

    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::header::CACHE_CONTROL;
    use axum::response::IntoResponse;

    /// Minimal nostr client: the open-ranking handlers never call it, so an empty
    /// stub is enough to satisfy `AppState`'s `GetEventsOf` bound.
    #[derive(Default)]
    struct StubNostrClient;

    #[async_trait::async_trait]
    impl GetEventsOf for StubNostrClient {
        async fn get_events_of(
            &self,
            _filters: Vec<nostr_sdk::Filter>,
            _timeout: Option<std::time::Duration>,
        ) -> Result<Vec<nostr_sdk::Event>, nostr_sdk::client::Error> {
            Ok(Vec::new())
        }
    }

    fn app_state(
        repo: MockRepo,
        queue: RecommendationQueue<MockRepo>,
    ) -> Arc<AppState<MockRepo, StubNostrClient>> {
        Arc::new(AppState::new(
            Arc::new(repo),
            Arc::new(StubNostrClient),
            Arc::new(queue),
        ))
    }

    fn idle_queue() -> RecommendationQueue<MockRepo> {
        recommendation_queue(MockRepo::default())
    }

    fn cache_control(response: &Response) -> Option<String> {
        response
            .headers()
            .get(CACHE_CONTROL)
            .map(|value| value.to_str().expect("ascii header").to_string())
    }

    fn json_body<T: Serialize>(value: &T) -> Bytes {
        Bytes::from(serde_json::to_vec(value).expect("request serializes"))
    }

    #[tokio::test]
    async fn rank_response_carries_cache_control_matching_its_ttl() {
        let pubkey = fresh_pubkey();
        let repo = MockRepo {
            pageranks: HashMap::from([(pubkey, 0.9)]),
            ..Default::default()
        };
        let state = app_state(repo, idle_queue());

        let response = serve_rank(
            State(state),
            json_body(&serde_json::json!({ "pubkeys": [pubkey.to_hex()] })),
        )
        .await
        .expect("rank succeeds")
        .into_response();

        assert_eq!(
            cache_control(&response).as_deref(),
            Some("public, max-age=3600"),
            "/rank advertises ttl=3600 in the body; the HTTP cache hint must mirror it"
        );
    }

    #[tokio::test]
    async fn reputation_response_carries_cache_control_matching_its_ttl() {
        let pubkey = fresh_pubkey();
        let repo = MockRepo {
            accounts: HashMap::from([(pubkey, account(pubkey, 0.5, 100, 20))]),
            ..Default::default()
        };
        let state = app_state(repo, idle_queue());

        let response = serve_reputation(
            State(state),
            json_body(&serde_json::json!({ "pubkey": pubkey.to_hex() })),
        )
        .await
        .expect("reputation succeeds")
        .into_response();

        assert_eq!(
            cache_control(&response).as_deref(),
            Some("public, max-age=3600")
        );
    }

    #[tokio::test]
    async fn followers_response_carries_cache_control_matching_its_ttl() {
        let target = fresh_pubkey();
        let repo = MockRepo {
            top_followers: HashMap::from([(target, vec![(fresh_pubkey(), 0.7)])]),
            ..Default::default()
        };
        let state = app_state(repo, idle_queue());

        let response = serve_followers(
            State(state),
            json_body(&serde_json::json!({ "pubkey": target.to_hex() })),
        )
        .await
        .expect("followers succeeds")
        .into_response();

        assert_eq!(
            cache_control(&response).as_deref(),
            Some("public, max-age=3600")
        );
    }

    #[tokio::test]
    async fn recommend_warming_response_carries_the_short_warming_cache_control() {
        // A cold /recommend returns the warming response (empty + short ttl). The
        // HTTP cache hint must mirror that SHORT ttl so a CDN/browser does not pin
        // the empty result and starve the client of the warmed result.
        let pov = fresh_pubkey();
        let repo = MockRepo {
            recommendations: HashMap::from([(pov, vec![recommendation(fresh_pubkey(), 0.8)])]),
            ..Default::default()
        };
        let state = app_state(MockRepo::default(), recommendation_queue(repo));

        let response = serve_recommend(
            State(state),
            json_body(&serde_json::json!({ "pov": pov.to_hex() })),
        )
        .await
        .expect("recommend succeeds")
        .into_response();

        assert_eq!(
            cache_control(&response).as_deref(),
            Some("public, max-age=10"),
            "the warming response must carry the short warming ttl, not the 3600 hint"
        );
    }

    #[tokio::test]
    async fn capability_document_carries_cache_control() {
        let response = serve_capability_document().await.into_response();
        assert_eq!(
            cache_control(&response).as_deref(),
            Some("public, max-age=3600"),
            "the discovery document is cacheable like the endpoints it advertises"
        );
    }

    #[test]
    fn open_ranking_errors_are_not_cacheable() {
        // Errors must never carry a long-lived cache hint (regression guard for the
        // decision to attach Cache-Control per successful response, not via a blunt
        // group-wide middleware that would also cache 4xx/5xx).
        let response = OpenRankingError::unprocessable("nope").into_response();
        assert!(
            cache_control(&response).is_none(),
            "error responses must not carry a Cache-Control header"
        );
    }

    // ---- single-flight + short result cache for the synchronous heavy reads ----

    use tokio::sync::Barrier;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_identical_reputation_requests_hit_the_repo_once() {
        // A thundering herd of identical /reputation requests must collapse to a
        // single Neo4j query (single-flight), not one query per request.
        const BURST: usize = 16;
        let pubkey = fresh_pubkey();
        let calls = Arc::new(AtomicUsize::new(0));
        let repo = MockRepo {
            accounts: HashMap::from([(pubkey, account(pubkey, 0.5, 10, 2))]),
            account_calls: calls.clone(),
            ..Default::default()
        };
        let state = app_state(repo, idle_queue());
        let body = json_body(&serde_json::json!({ "pubkey": pubkey.to_hex() }));

        let barrier = Arc::new(Barrier::new(BURST));
        let mut tasks = Vec::with_capacity(BURST);
        for _ in 0..BURST {
            let state = state.clone();
            let body = body.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                serve_reputation(State(state), body)
                    .await
                    .expect("reputation succeeds");
            }));
        }
        for task in tasks {
            task.await.expect("request task completes");
        }

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "identical concurrent /reputation reads must single-flight to one query"
        );
    }

    #[tokio::test]
    async fn repeated_reputation_request_is_served_from_cache() {
        let pubkey = fresh_pubkey();
        let calls = Arc::new(AtomicUsize::new(0));
        let repo = MockRepo {
            accounts: HashMap::from([(pubkey, account(pubkey, 0.5, 10, 2))]),
            account_calls: calls.clone(),
            ..Default::default()
        };
        let state = app_state(repo, idle_queue());
        let body = json_body(&serde_json::json!({ "pubkey": pubkey.to_hex() }));

        serve_reputation(State(state.clone()), body.clone())
            .await
            .expect("first reputation succeeds");
        serve_reputation(State(state), body)
            .await
            .expect("second reputation succeeds");

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the repeated identical request must be served from the result cache"
        );
    }

    #[tokio::test]
    async fn repeated_followers_request_is_served_from_cache() {
        let target = fresh_pubkey();
        let calls = Arc::new(AtomicUsize::new(0));
        let repo = MockRepo {
            top_followers: HashMap::from([(target, vec![(fresh_pubkey(), 0.7)])]),
            top_followers_calls: calls.clone(),
            ..Default::default()
        };
        let state = app_state(repo, idle_queue());
        let body = json_body(&serde_json::json!({ "pubkey": target.to_hex() }));

        serve_followers(State(state.clone()), body.clone())
            .await
            .expect("first followers succeeds");
        serve_followers(State(state), body)
            .await
            .expect("second followers succeeds");

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "second request hits the cache"
        );
    }

    #[tokio::test]
    async fn repeated_rank_request_is_served_from_cache() {
        let pubkey = fresh_pubkey();
        let calls = Arc::new(AtomicUsize::new(0));
        let repo = MockRepo {
            pageranks: HashMap::from([(pubkey, 0.9)]),
            pagerank_calls: calls.clone(),
            ..Default::default()
        };
        let state = app_state(repo, idle_queue());
        let body = json_body(&serde_json::json!({ "pubkeys": [pubkey.to_hex()] }));

        serve_rank(State(state.clone()), body.clone())
            .await
            .expect("first rank succeeds");
        serve_rank(State(state), body)
            .await
            .expect("second rank succeeds");

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "second request hits the cache"
        );
    }

    #[tokio::test]
    async fn reputation_repo_errors_are_not_cached() {
        // A failed query must not poison the cache: try_get_with never caches the
        // error, so the next request retries the repo instead of replaying a 500.
        let calls = Arc::new(AtomicUsize::new(0));
        let repo = MockRepo {
            account_calls: calls.clone(),
            fail: true,
            ..Default::default()
        };
        let state = app_state(repo, idle_queue());
        let body = json_body(&serde_json::json!({ "pubkey": fresh_pubkey().to_hex() }));

        let first = serve_reputation(State(state.clone()), body.clone()).await;
        let second = serve_reputation(State(state), body).await;

        assert!(first.is_err(), "a repo failure surfaces as an error");
        assert!(
            second.is_err(),
            "the retry also errors (nothing was cached)"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "each request retried the repo; the failure was not cached"
        );
    }

    // ---- observability metrics ----

    #[test]
    fn queue_depth_gauge_reflects_the_pending_count() {
        let rendered = render_metrics_for(|| async move {
            // No worker: nothing drains, so the gauge equals the number enqueued.
            let result_cache = Cache::builder().build();
            let (queue, _receiver) =
                RecommendationQueue::new(Arc::new(MockRepo::default()), result_cache, 4096);
            for _ in 0..3 {
                queue
                    .request_recommendations(fresh_pubkey(), None, Some(10))
                    .await
                    .expect("request accepted");
            }
        });
        assert!(
            rendered.contains("recommendation_queue_depth 3"),
            "the queue-depth gauge must track the pending count:\n{rendered}"
        );
    }

    #[test]
    fn saturated_queue_emits_the_queue_full_counter() {
        let rendered = render_metrics_for(|| async move {
            // No worker: enqueue past max_queue_size (50) to force QueueFull.
            let result_cache = Cache::builder().build();
            let (queue, _receiver) =
                RecommendationQueue::new(Arc::new(MockRepo::default()), result_cache, 4096);
            for _ in 0..60 {
                queue
                    .request_recommendations(fresh_pubkey(), None, Some(10))
                    .await
                    .expect("request accepted");
            }
        });
        assert!(
            rendered.contains("recommend_queue_full"),
            "shedding on a full queue must bump recommend_queue_full:\n{rendered}"
        );
    }

    #[test]
    fn worker_compute_failure_emits_recommend_failed() {
        let pov = fresh_pubkey();
        let rendered = render_metrics_for(|| async move {
            let calls = Arc::new(AtomicUsize::new(0));
            let repo = MockRepo {
                recommendation_calls: calls.clone(),
                // get_recommendations errors, so the background warm-up fails.
                fail: true,
                ..Default::default()
            };
            let queue = recommendation_queue(repo);
            recommend(&queue, recommend_request(Some(&pov.to_hex()), None))
                .await
                .expect("the warming response is returned even while failing");
            // Wait for the worker to attempt and fail the compute, then let its
            // error branch (metric + status write) finish.
            for _ in 0..1000 {
                if calls.load(Ordering::Relaxed) >= 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
            for _ in 0..50 {
                tokio::task::yield_now().await;
            }
        });
        assert!(
            rendered.contains("recommend_failed"),
            "a failed background warm-up must bump recommend_failed:\n{rendered}"
        );
    }

    #[test]
    fn rank_records_the_per_endpoint_latency_histogram() {
        let rendered = render_metrics_for(|| async move {
            let pubkey = fresh_pubkey();
            let repo = MockRepo {
                pageranks: HashMap::from([(pubkey, 0.9)]),
                ..Default::default()
            };
            let state = app_state(repo, idle_queue());
            serve_rank(
                State(state),
                json_body(&serde_json::json!({ "pubkeys": [pubkey.to_hex()] })),
            )
            .await
            .expect("rank succeeds");
        });
        assert!(
            rendered.contains("openranking_request_seconds"),
            "the endpoint must record a latency histogram:\n{rendered}"
        );
        assert!(
            rendered.contains("endpoint=\"/rank\""),
            "the latency histogram must be labelled by endpoint:\n{rendered}"
        );
    }
}
