use crate::repo::Repo;
use cached::proc_macro::cached;
use cached::TimedSizedCache;
use nostr_sdk::prelude::*;
use std::sync::Arc;
use tracing::error;

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

async fn fetch_friendly_id(maybe_metadata: Option<Metadata>, public_key: &PublicKey) -> FriendlyId {
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
        if let Ok(verified) = nip05::verify(public_key, &nip05_value, None).await {
            if verified {
                return FriendlyId::Nip05(nip05_value);
            }
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
    let friendly_id = fetch_friendly_id(metadata_result, public_key).await;

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
