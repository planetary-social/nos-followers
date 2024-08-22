use nostr_sdk::prelude::*;

pub async fn fetch_friendly_id(
    client: &Client,
    public_key: &PublicKey,
    verify: bool,
    fetch_nip05: bool,
) -> String {
    let npub_or_pubkey = match public_key.to_bech32() {
        Ok(npub) => npub,
        Err(_) => return public_key.to_hex(),
    };

    if !fetch_nip05 {
        return npub_or_pubkey;
    }

    let Some(metadata) = client.metadata(*public_key).await.ok() else {
        return npub_or_pubkey;
    };

    let name_or_npub_or_pubkey = metadata
        .name
        .filter(|name| !name.is_empty())
        .unwrap_or(npub_or_pubkey);

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
