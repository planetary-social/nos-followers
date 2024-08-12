use crate::fetch_friendly_id::fetch_friendly_id;
use crate::follows_differ::FollowChange;
use crate::repo::Repo;
use cached::proc_macro::cached;
use cached::TimedSizedCache;
use nostr_sdk::prelude::*;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

pub fn handle_follow_changes(
    nostr_client: Arc<Client>,
    repo: Arc<Repo>,
    follow_change_tx: Sender<FollowChange>,
    mut follow_change_rx: Receiver<FollowChange>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut verify = true;
        let mut fetch_nip05 = true;

        while let Some(follow_change) = follow_change_rx.recv().await {
            let current_seconds = match SystemTime::now().duration_since(UNIX_EPOCH) {
                Ok(duration) => duration.as_secs(),
                Err(e) => {
                    error!("Failed to get current time: {}", e);
                    continue;
                }
            };

            // Handle backpressure through graceful degradation
            // Every 5 seconds we check if the channel buffer is almost full
            // TODO: Move this to the worker pool, it can then periodically send
            // the ratio to the workers
            if current_seconds % 5 == 0 {
                let capacity = follow_change_tx.capacity();
                let max_capacity = follow_change_tx.max_capacity();

                // We stop verification for nip05 if the channel is 80% full
                verify = capacity > max_capacity / 5;

                // We stop fetching nip05 if the channel is 90% full. So in the
                // worse scenario we just convert the pubkey to npub and send
                // that
                fetch_nip05 = capacity > max_capacity / 10;
            }

            match follow_change {
                FollowChange::Followed {
                    at,
                    follower,
                    followee,
                } => {
                    let (friendly_follower, friendly_followee) = tokio::join!(
                        handle_friendly_id(&repo, &nostr_client, &follower, verify, fetch_nip05),
                        handle_friendly_id(&repo, &nostr_client, &followee, verify, fetch_nip05),
                    );

                    debug!(
                        "Followed: {}({}) -> {}({}) at {}",
                        follower,
                        friendly_follower,
                        followee,
                        friendly_followee,
                        at.to_human_datetime()
                    );
                    // TODO: Send to pubsub
                }
                FollowChange::Unfollowed {
                    at,
                    follower,
                    followee,
                } => {
                    let (friendly_follower, friendly_followee) = tokio::join!(
                        handle_friendly_id(&repo, &nostr_client, &follower, verify, fetch_nip05),
                        handle_friendly_id(&repo, &nostr_client, &followee, verify, fetch_nip05),
                    );

                    debug!(
                        "Unfollowed: {}({}) -> {}({}) at {}",
                        follower,
                        friendly_follower,
                        followee,
                        friendly_followee,
                        at.to_human_datetime()
                    )
                    // TODO: Send to pubsub
                }
            }
        }

        info!("Follow change task ended");
    })
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
