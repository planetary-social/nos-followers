use crate::config::Settings;
use crate::domain::FollowChange;
use crate::google_publisher::GooglePublisher;
use crate::google_pubsub_client::GooglePubSubClient;
use crate::refresh_friendly_id::refresh_friendly_id;
use crate::relay_subscriber::GetEventsOf;
use crate::repo::{Repo, RepoTrait};
use crate::worker_pool::{WorkerTask, WorkerTaskItem};
use nostr_sdk::prelude::*;
use std::error::Error;
use std::sync::Arc;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::debug;
/// Fetches friendly ids and then sends follow change to google pubsub
pub struct FollowChangeHandler<T: GetEventsOf> {
    repo: Arc<Repo>,
    google_publisher: GooglePublisher,
    nostr_client: Arc<T>,
    timeout_secs: u64,
}

impl<T> FollowChangeHandler<T>
where
    T: GetEventsOf,
{
    pub async fn new(
        repo: Arc<Repo>,
        nostr_client: Arc<T>,
        cancellation_token: CancellationToken,
        settings: &Settings,
    ) -> Result<Self> {
        let google_publisher_client =
            GooglePubSubClient::new(&settings.google_project_id, &settings.google_topic).await?;
        let google_publisher = GooglePublisher::create(
            cancellation_token.clone(),
            google_publisher_client,
            settings.seconds_threshold,
            settings.size_threshold,
        )
        .await?;

        Ok(Self {
            repo,
            nostr_client,
            google_publisher,
            timeout_secs: settings.worker_timeout_secs,
        })
    }
}

impl<T: GetEventsOf> WorkerTask<FollowChange> for FollowChangeHandler<T> {
    async fn call(
        &self,
        worker_task_item: WorkerTaskItem<FollowChange>,
    ) -> Result<(), Box<dyn Error>> {
        let WorkerTaskItem {
            item: mut follow_change,
        } = worker_task_item;
        // Fetch friendly IDs for the pubkeys or get it from DB if it takes more
        // than timeout_secs. Whatever if found through the network is cached.
        let (friendly_follower, friendly_followee) = tokio::select!(
            result = fetch_friendly_ids(
                &self.repo,
                self.nostr_client.clone(),
                &follow_change
            ) => result,
            result = get_friendly_ids_from_db(&self.repo, &follow_change, self.timeout_secs) => result
        );

        follow_change.friendly_follower = Some(friendly_follower);
        follow_change.friendly_followee = Some(friendly_followee);

        debug!(
            "Fetched friendly IDs for follow change from {:?} to {:?}, about to send to google pubsub",
            follow_change.friendly_follower, follow_change.friendly_followee
        );

        self.google_publisher
            .queue_publication(follow_change)
            .await?;
        Ok(())
    }
}

/// Get pubkey info from Nostr metadata or nip05 servers
async fn fetch_friendly_ids<T: GetEventsOf>(
    repo: &Arc<Repo>,
    nostr_client: Arc<T>,
    follow_change: &FollowChange,
) -> (String, String) {
    let (friendly_follower, friendly_followee) = tokio::join!(
        refresh_friendly_id(repo, &nostr_client, &follow_change.follower),
        refresh_friendly_id(repo, &nostr_client, &follow_change.followee),
    );

    (friendly_follower, friendly_followee)
}

/// Waits some seconds (to give some time for the pubkey info to be found from
/// nostr metadata or nip05 servers) and then just fetches whatever is found in
/// the DB
async fn get_friendly_ids_from_db(
    repo: &Arc<Repo>,
    follow_change: &FollowChange,
    timeout_secs: u64,
) -> (String, String) {
    sleep(std::time::Duration::from_secs(timeout_secs)).await;

    let (friendly_follower, friendly_followee) = tokio::join!(
        repo.get_friendly_id(&follow_change.follower),
        repo.get_friendly_id(&follow_change.followee)
    );

    (
        friendly_follower.ok().flatten().unwrap_or(
            follow_change
                .follower
                .to_bech32()
                .unwrap_or(follow_change.follower.to_hex()),
        ),
        friendly_followee.ok().flatten().unwrap_or(
            follow_change
                .followee
                .to_bech32()
                .unwrap_or(follow_change.followee.to_hex()),
        ),
    )
}
