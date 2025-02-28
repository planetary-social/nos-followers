use crate::metrics;
use crate::{
    relay_subscriber::GetEventsOf,
    repo::{Repo, RepoTrait},
    trust_policy,
};
use cached::proc_macro::cached;
use cached::TimedSizedCache;
use chrono::{DateTime, Utc};
use nostr_sdk::prelude::*;
use serde::Serialize;
use serde::Serializer;
use std::fmt::Display;
use std::sync::Arc;
use tokio::time::timeout;
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

impl From<&PublicKey> for FriendlyId {
    fn from(public_key: &PublicKey) -> Self {
        public_key
            .to_bech32()
            .map(FriendlyId::Npub)
            .unwrap_or_else(|_| FriendlyId::PublicKey(public_key.to_hex()))
    }
}

impl FriendlyId {
    pub fn enriched_display(&self, npub: Option<&str>) -> String {
        match self {
            FriendlyId::Npub(_) | FriendlyId::PublicKey(_) => self.to_string(),
            _ => {
                if let Some(npub) = npub {
                    format!("{} ({})", self, npub)
                } else {
                    self.to_string()
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct AccountInfo {
    pub public_key: PublicKey,
    pub friendly_id: Option<FriendlyId>,
    pub created_at: Option<DateTime<Utc>>,
    pub pagerank: Option<f64>,
    pub follower_count: Option<u64>,
    pub followee_count: Option<u64>,
    pub last_contact_list_at: Option<DateTime<Utc>>,
}

impl AccountInfo {
    pub fn new(public_key: PublicKey) -> Self {
        Self {
            public_key,
            friendly_id: None,
            created_at: None,
            pagerank: None,
            follower_count: None,
            followee_count: None,
            last_contact_list_at: None,
        }
    }

    /// Determines if this account is trusted based on the trust policy
    ///
    /// # Returns
    /// `true` if the account is trusted, `false` otherwise
    pub fn is_trusted(&self) -> bool {
        // Check if the account has a high enough pagerank
        let has_good_enough_followers = self.pagerank.is_some_and(|pr| pr > 0.2);

        // TODO: This is a placeholder, we need to implement this from a nos user registration endpoint
        let is_nos_user = false;

        // The account exists since we have an instance of it
        trust_policy::is_trusted(true, has_good_enough_followers, is_nos_user)
    }

    pub fn with_friendly_id(self, friendly_id: Option<FriendlyId>) -> Self {
        Self {
            friendly_id,
            ..self
        }
    }

    pub fn with_created_at(self, created_at: Option<DateTime<Utc>>) -> Self {
        Self { created_at, ..self }
    }

    pub fn with_pagerank(self, pagerank: Option<f64>) -> Self {
        Self { pagerank, ..self }
    }

    pub fn with_follower_count(self, follower_count: Option<u64>) -> Self {
        Self {
            follower_count,
            ..self
        }
    }

    pub fn with_followee_count(self, followee_count: Option<u64>) -> Self {
        Self {
            followee_count,
            ..self
        }
    }

    pub fn with_last_contact_list_at(self, last_contact_list_at: Option<DateTime<Utc>>) -> Self {
        Self {
            last_contact_list_at,
            ..self
        }
    }

    pub async fn refresh_metadata<T: GetEventsOf>(&mut self, nostr_client: &Arc<T>, verify: bool) {
        let metadata = fetch_metadata(nostr_client, &self.public_key).await;

        self.friendly_id = if verify {
            Some(cached_verify_nip05(&self.public_key, &metadata).await)
        } else {
            Some(metadata.get_friendly_id(&self.public_key))
        };

        // We care about the oldest value to infer when this account was created
        // TODO: Fetch other type of events to see if we find something older
        if self
            .created_at
            .is_some_and(|previous_at| previous_at > metadata.created_at)
        {
            self.created_at = Some(metadata.created_at);
        }
    }
}

impl Display for AccountInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AccountInfo(public_key: {}, friendly_id: {}, created_at: {}, pagerank: {}, follower_count: {}, followee_count: {}, last_contact_list_at: {})",
            self.public_key.to_hex(),
            self.friendly_id.as_ref().map(|f| f.to_string()).unwrap_or("Not set".to_string()),
            self.created_at.map(|d| d.to_rfc3339()).unwrap_or("Not set".to_string()),
            self.pagerank.map(|p| p.to_string()).unwrap_or("Not set".to_string()),
            self.follower_count.map(|fc| fc.to_string()).unwrap_or("Not set".to_string()),
            self.followee_count.map(|fc| fc.to_string()).unwrap_or("Not set".to_string()),
            self.last_contact_list_at.map(|d| d.to_rfc3339()).unwrap_or("Not set".to_string())
        )
    }
}

#[derive(Debug, Default, Clone)]
pub struct NostrMetadata {
    pub created_at: DateTime<Utc>,
    pub display_name: Option<String>,
    pub name: Option<String>,
    pub nip05: Option<String>,
}

impl NostrMetadata {
    pub fn from(event: Event) -> Self {
        assert!(event.kind == Kind::Metadata, "Expected Metadata event");

        match Metadata::from_json(event.content()) {
            Ok(metadata) => NostrMetadata {
                created_at: DateTime::from_timestamp(event.created_at.as_u64() as i64, 0)
                    .unwrap_or_default(),
                display_name: metadata.display_name,
                name: metadata.name,
                nip05: metadata.nip05,
            },
            Err(_) => NostrMetadata::default(),
        }
    }

    pub fn get_friendly_id(&self, public_key: &PublicKey) -> FriendlyId {
        let friendly_id: FriendlyId = public_key.into();

        if let Some(display_name) = self.display_name.as_ref().filter(|s| !s.trim().is_empty()) {
            return FriendlyId::DisplayName(display_name.to_string());
        }

        if let Some(name) = self.name.as_ref().filter(|s| !s.trim().is_empty()) {
            return FriendlyId::Name(name.to_string());
        }

        friendly_id
    }
}

/// Get useful info about an account
// We cache 10_000 entries and each entry expires after 50 minutes
// TODO: The number is arbitrary, adjust based on metrics
#[cached(
    ty = "TimedSizedCache<[u8; 32], NostrMetadata>",
    create = "{ TimedSizedCache::with_size_and_lifespan(10_000, 60 * 50) }",
    convert = r#"{ public_key.to_bytes() }"#
)]
async fn fetch_metadata<T: GetEventsOf>(
    nostr_client: &Arc<T>,
    public_key: &PublicKey,
) -> NostrMetadata {
    let filter: Filter = Filter::new()
        .author(*public_key)
        .kind(Kind::Metadata)
        .limit(1);

    let Ok(mut events) = nostr_client.get_events_of(vec![filter], None).await else {
        return NostrMetadata::default();
    };

    let Some(metadata_event) = events.pop() else {
        return NostrMetadata::default();
    };

    let metadata = NostrMetadata::from(metadata_event);

    debug!(
        "Fetched metadata for public key {}: {:?}",
        public_key.to_hex(),
        metadata
    );

    metadata
}

/// Tries to upgrade the friendly ID of an account to a NIP05 verified one
// We cache 10_000 entries and each entry expires after 50 minutes
// TODO: The number is arbitrary, adjust based on metrics
#[cached(
    ty = "TimedSizedCache<[u8; 32], FriendlyId>",
    create = "{ TimedSizedCache::with_size_and_lifespan(10_000, 60 * 50) }",
    convert = r#"{ public_key.to_bytes() }"#
)]
async fn cached_verify_nip05(public_key: &PublicKey, nostr_metadata: &NostrMetadata) -> FriendlyId {
    get_verified_friendly_id(nostr_metadata, public_key, Nip05Verifier).await
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
    let mut account_info = AccountInfo::new(*public_key);
    account_info.refresh_metadata(nostr_client, true).await;

    let friendly_id = account_info
        .friendly_id
        .unwrap_or_else(|| FriendlyId::DB(public_key.to_hex()));

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
        timeout(
            std::time::Duration::from_secs(2),
            nip05::verify(public_key, nip05_value, None),
        )
        .await
        .is_ok_and(|inner_result| inner_result.unwrap_or(false))
    }
}

async fn get_verified_friendly_id(
    metadata: &NostrMetadata,
    public_key: &PublicKey,
    nip05_verifier: impl VerifyNip05,
) -> FriendlyId {
    let Some(ref nip05_value) = metadata.nip05 else {
        // No NIP05 value to verify
        return metadata.get_friendly_id(public_key);
    };

    if !nip05_verifier.verify_nip05(public_key, nip05_value).await {
        // Verification failed
        return metadata.get_friendly_id(public_key);
    }

    metrics::verified_nip05().increment(1);
    FriendlyId::Nip05(nip05_value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::panic;

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

    async fn assert_friendly_id_from_metadata(
        keys: Option<&Keys>,
        verifier: impl VerifyNip05,
        metadata: Metadata,
        expected_friendly_id: FriendlyId,
    ) {
        let keys = match keys {
            Some(keys) => keys,
            None => &Keys::generate(),
        };

        let profile_event = EventBuilder::metadata(&metadata).to_event(keys).unwrap();
        let nostr_metadata = NostrMetadata::from(profile_event);

        let friendly_id =
            get_verified_friendly_id(&nostr_metadata, &keys.public_key(), verifier).await;

        assert_eq!(friendly_id, expected_friendly_id);
    }

    #[tokio::test]
    async fn test_fetch_friendly_id_empty_metadata() {
        let keys = Keys::generate();
        let metadata = Metadata::default();

        assert_friendly_id_from_metadata(
            Some(&keys),
            PanicNip05Verifier,
            metadata,
            FriendlyId::Npub(keys.public_key().to_bech32().unwrap().to_string()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_fetch_friendly_id_display_name() {
        let metadata = Metadata {
            display_name: Some("Alice".to_string()),
            ..Default::default()
        };

        assert_friendly_id_from_metadata(
            None,
            PanicNip05Verifier,
            metadata,
            FriendlyId::DisplayName("Alice".to_string()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_fetch_friendly_id_name() {
        let metadata = Metadata {
            name: Some("Alice".to_string()),
            ..Default::default()
        };

        assert_friendly_id_from_metadata(
            None,
            PanicNip05Verifier,
            metadata,
            FriendlyId::Name("Alice".to_string()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_fetch_friendly_id_nip05_verified() {
        let metadata = Metadata {
            display_name: Some("Alice".to_string()),
            name: Some("Alice".to_string()),
            nip05: Some("alice@nos.social".to_string()),
            ..Default::default()
        };

        assert_friendly_id_from_metadata(
            None,
            TrueNip05Verifier,
            metadata,
            FriendlyId::Nip05("alice@nos.social".to_string()),
        )
        .await;
    }

    #[tokio::test]
    async fn test_fetch_friendly_id_nip05_not_verified() {
        let metadata = Metadata {
            display_name: Some("AliceDisplayName".to_string()),
            name: Some("AliceName".to_string()),
            nip05: Some("alice@nos.social".to_string()),
            ..Default::default()
        };

        assert_friendly_id_from_metadata(
            None,
            FalseNip05Verifier,
            metadata,
            FriendlyId::DisplayName("AliceDisplayName".to_string()),
        )
        .await;
    }
}
