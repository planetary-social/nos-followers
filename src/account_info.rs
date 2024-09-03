use crate::{
    relay_subscriber::GetEventsOf,
    repo::{Repo, RepoTrait},
};
use cached::proc_macro::cached;
use cached::TimedSizedCache;
use chrono::{DateTime, Utc};
use metrics::counter;
use nostr_sdk::prelude::*;
use serde::Serialize;
use serde::Serializer;
use std::fmt::Display;
use std::sync::Arc;
use tracing::{debug, error};

#[derive(Debug, PartialEq, Clone, PartialOrd, Ord, Eq)]
pub enum FriendlyId {
    DisplayName(String),
    Name(String),
    Npub(String),
    Nip05(String),
    PublicKey(String),
    DB(String),
}

impl FriendlyId {}

impl Display for FriendlyId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FriendlyId::DisplayName(s)
            | FriendlyId::Name(s)
            | FriendlyId::Npub(s)
            | FriendlyId::Nip05(s)
            | FriendlyId::PublicKey(s)
            | FriendlyId::DB(s) => write!(f, "{}", s),
        }
    }
}

impl Serialize for FriendlyId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            FriendlyId::DisplayName(s)
            | FriendlyId::Name(s)
            | FriendlyId::Npub(s)
            | FriendlyId::Nip05(s)
            | FriendlyId::PublicKey(s)
            | FriendlyId::DB(s) => serializer.serialize_str(s),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AccountInfo {
    pub friendly_id: FriendlyId,
    pub created_at: Option<DateTime<Utc>>,
}

/// Get useful info about an account
// We cache 10_000 entries and each entry expires after 50 minutes
// TODO: The number is arbitrary, adjust based on metrics
#[cached(
    ty = "TimedSizedCache<[u8; 32], AccountInfo>",
    create = "{ TimedSizedCache::with_size_and_lifespan(10_000, 60 * 50) }",
    convert = r#"{ public_key.to_bytes() }"#
)]
pub async fn fetch_account_info<T: GetEventsOf>(
    nostr_client: &Arc<T>,
    public_key: &PublicKey,
) -> AccountInfo {
    let filter: Filter = Filter::new()
        .author(*public_key)
        .kind(Kind::Metadata)
        .limit(1);

    let Ok(events) = nostr_client.get_events_of(vec![filter], None).await else {
        let friendly_id = verified_friendly_id(None, public_key, Nip05Verifier).await;

        return AccountInfo {
            friendly_id,
            created_at: None,
        };
    };

    let (maybe_metadata, maybe_event) = match events.first() {
        Some(event) => match Metadata::from_json(event.content()) {
            Ok(metadata) => (Some(metadata), Some(event)),
            Err(e) => {
                debug!(
                    "Failed to fetch metadata for public key {}: {}",
                    public_key.to_hex(),
                    e
                );
                (None, Some(event))
            }
        },
        None => {
            debug!(
                "No metadata event found for public key {}",
                public_key.to_hex(),
            );
            (None, None)
        }
    };

    debug!(
        "Fetched metadata for public key {}: {:?}",
        public_key.to_hex(),
        maybe_metadata
    );

    let friendly_id = verified_friendly_id(maybe_metadata, public_key, Nip05Verifier).await;

    AccountInfo {
        friendly_id,
        created_at: maybe_event
            .map(|m| DateTime::from_timestamp(m.created_at.as_u64() as i64, 0))
            .unwrap_or(None),
    }
}

// Try to return an identifier that is not the public key. Save it in DB
// We cache 10_000 entries and each entry expires after 50 minutes
// TODO: The number is arbitrary, adjust based on metrics
#[cached(
    ty = "TimedSizedCache<[u8; 32], FriendlyId>",
    create = "{ TimedSizedCache::with_size_and_lifespan(10_000, 60 * 50) }",
    convert = r#"{ public_key.to_bytes() }"#
)]
pub async fn refresh_friendly_id<T: GetEventsOf>(
    repo: &Arc<Repo>,
    nostr_client: &Arc<T>,
    public_key: &PublicKey,
) -> FriendlyId {
    let AccountInfo { friendly_id, .. } = fetch_account_info(nostr_client, public_key).await;

    match &friendly_id {
        FriendlyId::DisplayName(display_name)
        | FriendlyId::Name(display_name)
        | FriendlyId::Nip05(display_name)
        | FriendlyId::Npub(display_name) => {
            if let Err(e) = repo.set_friendly_id(public_key, display_name).await {
                error!(
                    "Failed to add friendly ID for public key {}: {}",
                    public_key.to_hex(),
                    e
                );
            }
        }
        FriendlyId::PublicKey(_display_name) | FriendlyId::DB(_display_name) => {}
    }

    friendly_id
}

trait VerifyNip05 {
    async fn verify_nip05(&self, public_key: &PublicKey, nip05_value: &str) -> bool;
}

struct Nip05Verifier;

impl VerifyNip05 for Nip05Verifier {
    async fn verify_nip05(&self, public_key: &PublicKey, nip05_value: &str) -> bool {
        nip05::verify(public_key, nip05_value, None)
            .await
            .unwrap_or(false)
    }
}

async fn verified_friendly_id(
    maybe_metadata: Option<Metadata>,
    public_key: &PublicKey,
    nip05_verifier: impl VerifyNip05,
) -> FriendlyId {
    let npub_or_pubkey = public_key
        .to_bech32()
        .map(FriendlyId::Npub)
        .unwrap_or_else(|_| FriendlyId::PublicKey(public_key.to_hex()));

    let Some(metadata) = maybe_metadata else {
        return npub_or_pubkey;
    };

    let name_or_npub_or_pubkey = metadata
        .display_name
        .filter(|s| !s.trim().is_empty())
        .map(FriendlyId::DisplayName)
        .or_else(|| {
            metadata
                .name
                .filter(|s| !s.trim().is_empty())
                .map(FriendlyId::Name)
        })
        .unwrap_or(npub_or_pubkey);

    if let Some(nip05_value) = metadata.nip05 {
        if nip05_verifier.verify_nip05(public_key, &nip05_value).await {
            counter!("verified_nip05").increment(1);
            return FriendlyId::Nip05(nip05_value);
        }
        return name_or_npub_or_pubkey;
    }

    name_or_npub_or_pubkey
}

#[cfg(test)]
mod tests {
    use core::panic;

    use super::*;

    struct TrueNip05Verifier;

    impl VerifyNip05 for TrueNip05Verifier {
        async fn verify_nip05(&self, _: &PublicKey, _: &str) -> bool {
            true
        }
    }

    struct FalseNip05Verifier;

    impl VerifyNip05 for FalseNip05Verifier {
        async fn verify_nip05(&self, _: &PublicKey, _: &str) -> bool {
            false
        }
    }

    struct PanicNip05Verifier;

    impl VerifyNip05 for PanicNip05Verifier {
        async fn verify_nip05(&self, _: &PublicKey, _: &str) -> bool {
            panic!("The verifier should not be called");
        }
    }

    #[tokio::test]
    async fn test_fetch_friendly_id_empty_metadata() {
        let public_key =
            PublicKey::from_hex("89ef92b9ebe6dc1e4ea398f6477f227e95429627b0a33dc89b640e137b256be5")
                .unwrap();
        let metadata = Metadata::default();

        let friendly_id =
            verified_friendly_id(Some(metadata), &public_key, PanicNip05Verifier).await;

        assert_eq!(
            friendly_id,
            FriendlyId::Npub(
                "npub138he9w0tumwpun4rnrmywlez06259938kz3nmjymvs8px7e9d0js8lrdr2".to_string()
            )
        );
    }

    #[tokio::test]
    async fn test_fetch_friendly_id_display_name() {
        let public_key =
            PublicKey::from_hex("89ef92b9ebe6dc1e4ea398f6477f227e95429627b0a33dc89b640e137b256be5")
                .unwrap();
        let mut metadata = Metadata::default();
        metadata.display_name = Some("Alice".to_string());

        let friendly_id =
            verified_friendly_id(Some(metadata), &public_key, PanicNip05Verifier).await;

        assert_eq!(friendly_id, FriendlyId::DisplayName("Alice".to_string()));
    }

    #[tokio::test]
    async fn test_fetch_friendly_id_name() {
        let public_key =
            PublicKey::from_hex("89ef92b9ebe6dc1e4ea398f6477f227e95429627b0a33dc89b640e137b256be5")
                .unwrap();
        let mut metadata = Metadata::default();
        metadata.name = Some("Alice".to_string());

        let friendly_id =
            verified_friendly_id(Some(metadata), &public_key, PanicNip05Verifier).await;

        assert_eq!(friendly_id, FriendlyId::Name("Alice".to_string()));
    }

    #[tokio::test]
    async fn test_fetch_friendly_id_nip05_verified() {
        let public_key =
            PublicKey::from_hex("89ef92b9ebe6dc1e4ea398f6477f227e95429627b0a33dc89b640e137b256be5")
                .unwrap();
        let mut metadata = Metadata::default();
        metadata.display_name = Some("Alice".to_string());
        metadata.name = Some("Alice".to_string());
        metadata.nip05 = Some("alice@nos.social".to_string());

        let friendly_id =
            verified_friendly_id(Some(metadata), &public_key, TrueNip05Verifier).await;

        assert_eq!(
            friendly_id,
            FriendlyId::Nip05("alice@nos.social".to_string())
        );
    }

    #[tokio::test]
    async fn test_fetch_friendly_id_nip05_not_verified() {
        let public_key =
            PublicKey::from_hex("89ef92b9ebe6dc1e4ea398f6477f227e95429627b0a33dc89b640e137b256be5")
                .unwrap();
        let mut metadata = Metadata::default();
        metadata.display_name = Some("AliceDisplayName".to_string());
        metadata.name = Some("AliceName".to_string());
        metadata.nip05 = Some("alice@nos.social".to_string());

        let friendly_id =
            verified_friendly_id(Some(metadata), &public_key, FalseNip05Verifier).await;

        assert_eq!(
            friendly_id,
            FriendlyId::DisplayName("AliceDisplayName".to_string())
        );
    }
}
