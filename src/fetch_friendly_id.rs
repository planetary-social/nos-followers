use cached::proc_macro::cached;
use cached::TimedSizedCache;
use nostr_sdk::prelude::*;

// Try to return an identifier that is not the public key.
// We cache 1_000_000 entries and each entry expires after 50 minutes
#[cached(
    ty = "TimedSizedCache<[u8; 32], String>",
    create = "{ TimedSizedCache::with_size_and_lifespan(1_000_000, 60 * 50) }",
    convert = r#"{ public_key.to_bytes() }"#
)]
pub async fn fetch_friendly_id(client: &Client, public_key: &PublicKey, verify: bool) -> String {
    let npub_or_pubkey = match public_key.to_bech32() {
        Ok(npub) => npub,
        Err(_) => return public_key.to_hex(),
    };

    let Some(metadata) = client.metadata(*public_key).await.ok() else {
        return npub_or_pubkey;
    };

    let name_or_npub_or_pubkey = metadata.name.unwrap_or(npub_or_pubkey);

    if let Some(nip05_value) = metadata.nip05 {
        if verify {
            let Ok(verified) = nip05::verify(public_key, &nip05_value, None).await else {
                return name_or_npub_or_pubkey;
            };

            if !verified {
                return name_or_npub_or_pubkey;
            }

            return nip05_value;
        }

        return nip05_value;
    }

    name_or_npub_or_pubkey
}
