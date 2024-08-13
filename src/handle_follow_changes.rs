use crate::domain::follow_change::FollowChange;
use crate::fetch_friendly_id::fetch_friendly_id;
use crate::google_publisher::GooglePublisher;
use crate::repo::Repo;
use cached::proc_macro::cached;
use cached::TimedSizedCache;
use nostr_sdk::prelude::*;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

pub async fn handle_follow_changes(
    nostr_client: Arc<Client>,
    repo: Arc<Repo>,
    mut follow_change_receiver: Receiver<FollowChange>,
) -> Result<JoinHandle<()>> {
    let mut google_publisher = GooglePublisher::create().await?;

    let task = tokio::spawn(async move {
        let mut verify: bool;
        let mut fetch_nip05: bool;
        let max_capacity = follow_change_receiver.max_capacity();

        while let Some(mut follow_change) = follow_change_receiver.recv().await {
            // Handle backpressure through graceful degradation
            // TODO: Move this to the worker pool, it can then periodically send
            // the ratio to the workers
            let capacity = follow_change_receiver.capacity();

            // We stop verification for nip05 if the channel is 80% full
            verify = capacity > max_capacity * 8 / 10;

            // We stop fetching nip05 if the channel is 90% full. So in the
            // worst scenario we just convert the pubkey to npub and send
            // that
            fetch_nip05 = capacity > max_capacity * 9 / 10;

            let FollowChange {
                follower, followee, ..
            } = &follow_change;

            let (friendly_follower, friendly_followee) = tokio::join!(
                handle_friendly_id(&repo, &nostr_client, follower, verify, fetch_nip05),
                handle_friendly_id(&repo, &nostr_client, followee, verify, fetch_nip05),
            );

            follow_change.friendly_follower = Some(friendly_follower);
            follow_change.friendly_followee = Some(friendly_followee);

            debug!("{}", follow_change);

            if let Err(e) = google_publisher.queue_publication(follow_change).await {
                error!("Failed to publish follow change: {}", e);
            }
        }

        info!("Follow change task ended");
    });

    Ok(task)
}

// Try to return an identifier that is not the public key.
// We cache 1_000_000 entries and each entry expires after 50 minutes
#[cached(
    ty = "TimedSizedCache<[u8; 32], String>",
    create = "{ TimedSizedCache::with_size_and_lifespan(1_000_000, 60 * 50) }",
    convert = r#"{ public_key.to_bytes() }"#
)]
async fn handle_friendly_id(
    repo: &Arc<Repo>,
    nostr_client: &Client,
    public_key: &PublicKey,
    verify: bool,
    fetch_nip05: bool,
) -> String {
    let friendly_id = fetch_friendly_id(nostr_client, public_key, verify, fetch_nip05).await;

    if let Err(e) = repo.add_friendly_id(public_key, &friendly_id).await {
        error!("Failed to add friendly ID for follower: {}", e);
    }

    friendly_id
}
