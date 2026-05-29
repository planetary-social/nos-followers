//! OpenRanking protocol support (<https://github.com/Open-Ranking/protocol>).
//!
//! Exposes the subset of the protocol that maps onto our follow graph:
//! capability discovery (ORE-01) plus the `/rank`, `/reputation`, `/followers`
//! and `/recommend` endpoints. See OREs 00-06 for the full specification.

use axum::response::IntoResponse;
use serde::Serialize;

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
