use crate::repo::Repo;
use cached::proc_macro::cached;
use cached::TimedSizedCache;
use nostr_sdk::prelude::*;
use std::sync::Arc;
use tracing::error;

#[derive(Debug, PartialEq)]
pub enum FriendlyId {
    DisplayName(String),
    Name(String),
    Npub(String),
    Nip05(String),
    PublicKey(String),
}

impl FriendlyId {
    pub fn is_set(&self) -> bool {
        match self {
            FriendlyId::DisplayName(s)
            | FriendlyId::Name(s)
            | FriendlyId::Npub(s)
            | FriendlyId::Nip05(s)
            | FriendlyId::PublicKey(s) => !s.trim().is_empty(),
        }
    }
}

trait VerifyNip05 {
    async fn verify_nip05(&self, public_key: &PublicKey, nip05_value: &str) -> bool;
}

struct Nip05Verifier;

impl VerifyNip05 for Nip05Verifier {
    async fn verify_nip05(&self, public_key: &PublicKey, nip05_value: &str) -> bool {
        if let Ok(verified) = nip05::verify(public_key, nip05_value, None).await {
            return verified;
        }

        return false;
    }
}

async fn fetch_friendly_id(
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
            return FriendlyId::Nip05(nip05_value);
        }
        return name_or_npub_or_pubkey;
    }

    name_or_npub_or_pubkey
}

// Try to return an identifier that is not the public key. Save it in DB
// We cache 1_000_000 entries and each entry expires after 50 minutes
#[cached(
    ty = "TimedSizedCache<[u8; 32], String>",
    create = "{ TimedSizedCache::with_size_and_lifespan(1_000_000, 60 * 50) }",
    convert = r#"{ public_key.to_bytes() }"#
)]
pub async fn refresh_friendly_id(
    repo: &Arc<Repo>,
    nostr_client: &Client,
    public_key: &PublicKey,
) -> String {
    let metadata_result = nostr_client.metadata(*public_key).await.ok();
    let friendly_id = fetch_friendly_id(metadata_result, public_key, Nip05Verifier).await;

    match friendly_id {
        FriendlyId::DisplayName(display_name)
        | FriendlyId::Name(display_name)
        | FriendlyId::Nip05(display_name)
        | FriendlyId::Npub(display_name) => {
            if let Err(e) = repo.add_friendly_id(public_key, &display_name).await {
                error!(
                    "Failed to add friendly ID for public key {}: {}",
                    public_key.to_hex(),
                    e
                );
            }
            display_name
        }
        FriendlyId::PublicKey(pk) => pk,
    }
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

        let friendly_id = fetch_friendly_id(Some(metadata), &public_key, PanicNip05Verifier).await;

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

        let friendly_id = fetch_friendly_id(Some(metadata), &public_key, PanicNip05Verifier).await;

        assert_eq!(friendly_id, FriendlyId::DisplayName("Alice".to_string()));
    }

    #[tokio::test]
    async fn test_fetch_friendly_id_name() {
        let public_key =
            PublicKey::from_hex("89ef92b9ebe6dc1e4ea398f6477f227e95429627b0a33dc89b640e137b256be5")
                .unwrap();
        let mut metadata = Metadata::default();
        metadata.name = Some("Alice".to_string());

        let friendly_id = fetch_friendly_id(Some(metadata), &public_key, PanicNip05Verifier).await;

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

        let friendly_id = fetch_friendly_id(Some(metadata), &public_key, TrueNip05Verifier).await;

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

        let friendly_id = fetch_friendly_id(Some(metadata), &public_key, FalseNip05Verifier).await;

        assert_eq!(
            friendly_id,
            FriendlyId::DisplayName("AliceDisplayName".to_string())
        );
    }
}
