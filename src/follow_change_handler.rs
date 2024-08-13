use crate::domain::follow_change::FollowChange;
use crate::fetch_friendly_id::fetch_friendly_id;
use crate::google_publisher::GooglePublisher;
use crate::repo::Repo;
use crate::worker_pool::{WorkerTask, WorkerTaskItem};
use cached::proc_macro::cached;
use cached::TimedSizedCache;
use nostr_sdk::prelude::*;
use std::error::Error;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

pub struct FollowChangeHandler {
    repo: Arc<Repo>,
    nostr_client: Client,
    google_publisher: GooglePublisher,
}

impl FollowChangeHandler {
    pub async fn new(
        repo: Arc<Repo>,
        nostr_client: Client,
        cancellation_token: CancellationToken,
    ) -> Result<Self> {
        let google_publisher = GooglePublisher::create(cancellation_token.clone()).await?;

        Ok(Self {
            repo,
            nostr_client,
            google_publisher,
        })
    }
}

impl WorkerTask<FollowChange> for FollowChangeHandler {
    async fn call(
        &self,
        worker_task_item: WorkerTaskItem<FollowChange>,
    ) -> Result<(), Box<dyn Error>> {
        let WorkerTaskItem {
            item: mut follow_change,
            channel_load,
        } = worker_task_item;

        // We stop verification for nip05 if the channel is 80% full
        let verify = channel_load < 80;

        // We stop fetching nip05 if the channel is 90% full. So in the
        // worst scenario we just convert the pubkey to npub and send
        // that
        let fetch_nip05 = channel_load < 90;

        let FollowChange {
            follower, followee, ..
        } = &follow_change;

        let (friendly_follower, friendly_followee) = tokio::join!(
            handle_friendly_id(
                &self.repo,
                &self.nostr_client,
                follower,
                verify,
                fetch_nip05
            ),
            handle_friendly_id(
                &self.repo,
                &self.nostr_client,
                followee,
                verify,
                fetch_nip05
            ),
        );

        follow_change.friendly_follower = Some(friendly_follower);
        follow_change.friendly_followee = Some(friendly_followee);

        debug!("{}", follow_change);

        self.google_publisher
            .queue_publication(follow_change)
            .await
            .map_err(|e| e.into())
    }
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
