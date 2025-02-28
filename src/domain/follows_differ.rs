use crate::domain::AccountInfo;
use crate::metrics;
use crate::relay_subscriber::GetEventsOf;
use crate::repo::RepoTrait;
use crate::{
    domain::{contact_list_follow::ContactListFollow, follow_change::FollowChange},
    worker_pool::WorkerTask,
};
use chrono::{DateTime, Duration, Utc};
use nostr_sdk::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast::Sender;
use tracing::{debug, info};

#[derive(Default, Debug)]
struct FollowsDiff {
    stored_follow: Option<ContactListFollow>,
    exists_in_latest_contact_list: bool,
}

pub struct FollowsDiffer<T, U>
where
    T: RepoTrait,
    U: GetEventsOf + Sync + Send,
{
    repo: Arc<T>,
    nostr_client: Arc<U>,
    follow_change_sender: Sender<Box<FollowChange>>,
}

#[async_trait]
impl<T, U> WorkerTask<Box<Event>> for FollowsDiffer<T, U>
where
    T: RepoTrait,
    U: GetEventsOf + Sync + Send,
{
    async fn call(&self, event: Box<Event>) -> Result<()> {
        debug!("Processing event here");

        if event.kind != Kind::ContactList {
            debug!(
                "Skipping event of kind {:?} for {}",
                event.kind,
                event.pubkey.to_bech32().unwrap_or_default()
            );
            return Ok(());
        }

        let follower = event.pubkey;
        let event_created_at = convert_timestamp(event.created_at.as_u64())?;
        let maybe_account_info = self.repo.get_account_info(&follower).await?;
        let mut follower_account_info =
            maybe_account_info.unwrap_or_else(|| AccountInfo::new(follower));

        if probably_inactive_or_spam(&event, &follower_account_info) {
            debug!(
                "Skipping event from {} as it's probably spam",
                event.pubkey.to_bech32().unwrap_or_default()
            );
            return Ok(());
        }

        // Check if the event is older than the latest stored update and skip if so
        if let Some(last_contact_list_at) = follower_account_info.last_contact_list_at {
            if event_created_at < last_contact_list_at {
                debug!(
                    "Skipping follow list for {} as it's older than the last update",
                    follower.to_bech32().unwrap_or_default()
                );

                metrics::already_seen_contact_lists().increment(1);
                return Ok(());
            }
        }

        follower_account_info.last_contact_list_at = Some(event_created_at);
        self.repo
            .update_last_contact_list_at(&follower, &event_created_at)
            .await?;

        // Grab profile nostr event and update what we know
        // TODO: This should be done in a separate worker
        follower_account_info
            .refresh_metadata(&self.nostr_client, true)
            .await;

        // Get the stored follows and the latest update time from the database
        let mut follows_diff = self.initialize_follows_diff(&follower).await?;

        // Populate the new follows from the event tags
        self.populate_new_follows(&mut follows_diff, &event);

        // Process the follows_diff and apply changes
        let (followed_counter, unfollowed_counter, unchanged) = self
            .process_follows_diff(
                follows_diff,
                &follower,
                event_created_at,
                &follower_account_info,
            )
            .await?;

        if let Some(log_line) = log_line(
            follower,
            followed_counter,
            unfollowed_counter,
            unchanged,
            follower_account_info,
            &event,
        ) {
            info!("{}", log_line);
        }

        metrics::contact_lists_processed().increment(1);
        Ok(())
    }
}

impl<T, U> FollowsDiffer<T, U>
where
    T: RepoTrait + Sync + Send,
    U: GetEventsOf + Sync + Send,
{
    pub fn new(
        repo: Arc<T>,
        nostr_client: Arc<U>,
        follow_change_sender: Sender<Box<FollowChange>>,
    ) -> Self {
        Self {
            repo,
            nostr_client,
            follow_change_sender,
        }
    }

    /// Initializes a structure that holds the differences between the stored
    /// follows and the latest contact list.
    async fn initialize_follows_diff(
        &self,
        follower: &PublicKey,
    ) -> Result<HashMap<PublicKey, FollowsDiff>> {
        let stored_follows = self.repo.get_follows(follower).await?;
        let mut follows_diff: HashMap<PublicKey, FollowsDiff> = HashMap::new();

        for stored_follow in stored_follows {
            follows_diff
                .entry(stored_follow.followee)
                .or_default()
                .stored_follow = Some(stored_follow.clone());
        }

        Ok(follows_diff)
    }

    fn populate_new_follows(
        &self,
        follows_diff: &mut HashMap<PublicKey, FollowsDiff>,
        event: &Event,
    ) {
        for tag in &event.tags {
            if let Some(TagStandard::PublicKey { public_key, .. }) = tag.as_standardized() {
                follows_diff
                    .entry(*public_key)
                    .or_default()
                    .exists_in_latest_contact_list = true;
            }
        }
    }

    async fn process_follows_diff(
        &self,
        follows_diff: HashMap<PublicKey, FollowsDiff>,
        follower: &PublicKey,
        event_created_at: DateTime<Utc>,
        follower_account_info: &AccountInfo,
    ) -> Result<(usize, usize, usize)> {
        debug!(
            "Processing follows diff for {}",
            follower.to_bech32().unwrap_or_default()
        );
        let mut followed_counter = 0;
        let mut unfollowed_counter = 0;
        let mut unchanged = 0;

        let send_notifications =
            should_send_notifications(follower_account_info, follower, event_created_at).await?;

        // Get the trust status of the follower
        let is_follower_trusted = follower_account_info.is_trusted();

        for (followee, diff) in follows_diff {
            match diff.stored_follow {
                Some(mut stored_follow) => {
                    if diff.exists_in_latest_contact_list {
                        stored_follow.updated_at = event_created_at;
                        self.repo.upsert_follow(&stored_follow).await?;
                        unchanged += 1;
                    } else {
                        self.repo.delete_follow(&followee, follower).await?;

                        if send_notifications {
                            let follow_change =
                                FollowChange::new_unfollowed(event_created_at, *follower, followee)
                                    .with_friendly_follower(
                                        follower_account_info.friendly_id.clone(),
                                    )
                                    .with_trusted(is_follower_trusted);
                            self.send_follow_change(follow_change)?;
                        }
                        unfollowed_counter += 1;
                    }
                }
                None => {
                    if followee != *follower {
                        let follow = ContactListFollow {
                            followee,
                            follower: *follower,
                            updated_at: event_created_at,
                            created_at: event_created_at,
                        };
                        self.repo.upsert_follow(&follow).await?;

                        if send_notifications {
                            let follow_change =
                                FollowChange::new_followed(event_created_at, *follower, followee)
                                    .with_friendly_follower(
                                        follower_account_info.friendly_id.clone(),
                                    )
                                    .with_trusted(is_follower_trusted);
                            self.send_follow_change(follow_change)?;
                        }
                        followed_counter += 1;
                    } else {
                        debug!(
                            "Skipping self-follow for {}",
                            followee.to_bech32().unwrap_or_default()
                        );
                    }
                }
            }
        }

        Ok((followed_counter, unfollowed_counter, unchanged))
    }

    fn send_follow_change(&self, follow_change: FollowChange) -> Result<()> {
        self.follow_change_sender.send(Box::new(follow_change))?;
        Ok(())
    }
}

const FIVE_MINUTES_DURATION: Duration = Duration::minutes(5);
const ONE_DAY_DURATION: Duration = Duration::seconds(60 * 60 * 24);
const ONE_WEEK_DURATION: Duration = Duration::seconds(60 * 60 * 24 * 7);
const ONE_MONTH_DURATION: Duration = Duration::seconds(60 * 60 * 24 * 7 * 4);

/// Heuristics to decide if we should send notifications for a contact list or just update the DB.
async fn should_send_notifications(
    account_info: &AccountInfo,
    follower: &PublicKey,
    event_created_at: DateTime<Utc>,
) -> Result<bool> {
    let older_than_a_day = older_than_a_day(event_created_at.timestamp() as u64);
    if older_than_a_day {
        debug!(
            "Event from {} is older than a day {}. Skipping notifications.",
            follower.to_bech32().unwrap_or_default(),
            event_created_at.to_rfc3339()
        );
        return Ok(false);
    }

    if account_info.last_contact_list_at.is_none() {
        // This is the first time we see a contact list from this follower.
        let created_at = account_info.created_at;

        // Not really the creation date, but the last time the profile was updated
        let Some(follower_created_at) = created_at else {
            return Ok(true);
        };

        if (event_created_at - follower_created_at) > ONE_WEEK_DURATION {
            // If there's a big gap from the time of creation of the follower to
            // the current contact list, then we assume that most of the follows
            // are from long ago and we skip creating follow changes for this
            // one to avoid noise, only used to update the DB and have the
            // initial contact list for upcoming diffs.

            info!(
                "Skipping notifications for first seen list of an old account: {}",
                follower.to_bech32().unwrap_or_default()
            );

            return Ok(false);
        }
    }

    Ok(true)
}

/// Heuristic to decide whether to skip processing a contact list event.  We
/// don't skip old lists unless they have one or fewer follows. This is because
/// we could be running a backfill from the TCP port, so we won't send
/// notifications in that case, but we still want to process the contact list.
fn probably_inactive_or_spam(event: &Event, account_info: &AccountInfo) -> bool {
    let too_new =
        !older_than_five_minutes(account_info.created_at.unwrap_or(Utc::now()).timestamp() as u64);

    if too_new
        && event.public_keys().any(|pk| {
            // Quick harcoded filter for this spammer
            pk.to_hex() == "4bc7982c4ee4078b2ada5340ae673f18d3b6a664b1f97e8d6799e6074cb5c39d"
        })
    {
        return true;
    }

    if older_than_a_month(event.created_at.as_u64()) {
        let number_of_p_tags = event
            .tags()
            .iter()
            .filter(|tag| matches!(tag.as_standardized(), Some(TagStandard::PublicKey { .. })))
            .count();

        return number_of_p_tags < 2;
    }

    false
}

fn older_than_five_minutes(event_created_at_timestamp: u64) -> bool {
    older_than(event_created_at_timestamp, FIVE_MINUTES_DURATION)
}

fn older_than_a_day(event_created_at_timestamp: u64) -> bool {
    older_than(event_created_at_timestamp, ONE_DAY_DURATION)
}

fn older_than_a_month(event_created_at_timestamp: u64) -> bool {
    older_than(event_created_at_timestamp, ONE_MONTH_DURATION)
}

fn older_than(event_created_at_timestamp: u64, duration: Duration) -> bool {
    // We use tokio instant here so we can easily mock
    let time_ago = (Utc::now() - duration.to_std().unwrap()).timestamp();

    event_created_at_timestamp < time_ago as u64
}

fn log_line(
    follower: PublicKey,
    followed_counter: usize,
    unfollowed_counter: usize,
    unchanged: usize,
    account_info: AccountInfo,
    event: &Event,
) -> Option<String> {
    if unchanged > 0 && followed_counter == 0 && unfollowed_counter == 0 {
        // This one is not interesting
        debug!(
            "No changes for {}. Last seen contact list: {:?}",
            follower.to_bech32().unwrap_or_default(),
            account_info.last_contact_list_at,
        );
        return None;
    }

    let human_event_created_at = event.created_at.to_human_datetime();

    let last_seen_contact_list =
        if let Some(last_seen_contact_list) = account_info.last_contact_list_at {
            last_seen_contact_list.to_rfc3339()
        } else {
            "new".to_string()
        };

    let timestamp_diff = format!("[{}->{}]", last_seen_contact_list, human_event_created_at);

    if account_info.last_contact_list_at.is_none() {
        return Some(format!(
            "Npub {}: date {}, {} followed, new follows list",
            follower.to_bech32().unwrap_or_default(),
            timestamp_diff,
            followed_counter,
        ));
    }

    // Investigate states in which there are no followees but there are unfollowed followees
    if followed_counter == 0 && unfollowed_counter > 1 && unchanged == 0 {
        metrics::sudden_follow_drops().increment(1);

        if has_nos_agent(event) {
            metrics::nos_sudden_follow_drops().increment(1);
        }

        return Some(format!(
            "ALL UNFOLLOWED: Npub {}: date {}, {} unfollowed, {} unchanged, {}",
            follower.to_bech32().unwrap_or_default(),
            timestamp_diff,
            unfollowed_counter,
            unchanged,
            event.as_json(),
        ));
    }

    Some(format!(
        "Npub {}: date {}, {} followed, {} unfollowed, {} unchanged",
        follower.to_bech32().unwrap_or_default(),
        timestamp_diff,
        followed_counter,
        unfollowed_counter,
        unchanged,
    ))
}

fn has_nos_agent(event: &Event) -> bool {
    event.tags.iter().any(|tag| {
        let tag_components = tag.as_vec();

        tag_components[0] == "client" && tag_components[1] == "nos"
    })
}

fn convert_timestamp(timestamp: u64) -> Result<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp(timestamp as i64, 0).ok_or("Invalid timestamp".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::contact_list_follow::ContactListFollow;
    use crate::repo::RepoError;
    use chrono::{Duration, Utc};
    use nostr_sdk::PublicKey;
    use std::borrow::Cow;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::LazyLock;
    use tokio::sync::broadcast::channel;
    use tokio::sync::Mutex;
    use tokio::time::sleep;

    static NOW: LazyLock<DateTime<Utc>> =
        LazyLock::new(|| DateTime::<Utc>::from_timestamp(Utc::now().timestamp(), 0).unwrap());

    #[derive(Default)]
    struct MockNostrClient;

    #[async_trait]
    impl GetEventsOf for MockNostrClient {
        async fn get_events_of(
            &self,
            _filter: Vec<Filter>,
            _timeout: Option<core::time::Duration>,
        ) -> Result<Vec<Event>, nostr_sdk::client::Error> {
            Ok(vec![])
        }
    }

    type TestHashMap =
        Arc<Mutex<HashMap<PublicKey, (Vec<ContactListFollow>, Option<DateTime<Utc>>)>>>;
    struct MockRepo {
        follows: TestHashMap,
    }

    impl Default for MockRepo {
        fn default() -> Self {
            Self {
                follows: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }

    impl RepoTrait for MockRepo {
        async fn update_last_contact_list_at(
            &self,
            public_key: &PublicKey,
            at: &DateTime<Utc>,
        ) -> Result<(), RepoError> {
            let mut follows = self.follows.lock().await;
            let entry = follows.entry(*public_key).or_default();
            let previous_value = entry.1;

            if previous_value.is_none() || at > previous_value.as_ref().unwrap() {
                entry.1 = Some(*at);
            }

            Ok(())
        }

        async fn upsert_follow(&self, follow: &ContactListFollow) -> Result<(), RepoError> {
            let mut follows = self.follows.lock().await;
            let entry = follows.entry(follow.follower).or_default();
            let follow = follow.clone();
            entry.0.retain(|f| f.followee != follow.followee);
            entry.0.push(follow);
            Ok(())
        }

        async fn delete_follow(
            &self,
            followee: &PublicKey,
            follower: &PublicKey,
        ) -> Result<(), RepoError> {
            let mut follows = self.follows.lock().await;
            let entry = follows.entry(*follower).or_default();
            entry.0.retain(|f| f.followee != *followee);
            Ok(())
        }

        async fn get_follows(
            &self,
            follower: &PublicKey,
        ) -> Result<Vec<ContactListFollow>, RepoError> {
            let follows = self.follows.lock().await;
            Ok(follows.get(follower).cloned().unwrap_or_default().0)
        }

        async fn get_account_info(
            &self,
            follower: &PublicKey,
        ) -> Result<Option<AccountInfo>, RepoError> {
            let follows = self.follows.lock().await;
            Ok(follows
                .get(follower)
                .cloned()
                .map(|(_follows, last)| AccountInfo {
                    public_key: *follower,
                    last_contact_list_at: last,
                    friendly_id: None,
                    created_at: None,
                    follower_count: None,
                    followee_count: None,
                    pagerank: None,
                }))
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_follows_differ_for_new_contact_list() {
        let followee_pubkey = Keys::generate().public_key();
        let follower_keys = Keys::generate();
        let follower_pubkey = follower_keys.public_key();

        let contact_events = vec![create_contact_event(
            &follower_keys,
            vec![followee_pubkey],
            seconds_to_datetime(1000),
        )];

        assert_follow_changes(
            contact_events,
            vec![FollowChange::new_followed(
                seconds_to_datetime(1000),
                follower_pubkey,
                followee_pubkey,
            )],
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_follows_differ_for_new_added_contact() {
        let followee1_pubkey = Keys::generate().public_key();
        let followee2_pubkey = Keys::generate().public_key();
        let follower_keys = Keys::generate();
        let follower_pubkey = follower_keys.public_key();

        let contact_events = vec![
            create_contact_event(
                &follower_keys,
                vec![followee1_pubkey],
                seconds_to_datetime(1),
            ),
            create_contact_event(
                &follower_keys,
                vec![followee1_pubkey, followee2_pubkey],
                seconds_to_datetime(2),
            ),
        ];

        assert_follow_changes(
            contact_events,
            vec![
                FollowChange::new_followed(
                    seconds_to_datetime(1),
                    follower_pubkey,
                    followee1_pubkey,
                ),
                FollowChange::new_followed(
                    seconds_to_datetime(2),
                    follower_pubkey,
                    followee2_pubkey,
                ),
            ],
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_follows_differ_for_removed_contact() {
        let followee1_pubkey = Keys::generate().public_key();
        let followee2_pubkey = Keys::generate().public_key();
        let follower_keys = Keys::generate();
        let follower_pubkey = follower_keys.public_key();

        let contact_events = vec![
            create_contact_event(
                &follower_keys,
                vec![followee1_pubkey],
                seconds_to_datetime(1),
            ),
            create_contact_event(
                &follower_keys,
                vec![followee1_pubkey, followee2_pubkey],
                seconds_to_datetime(2),
            ),
            create_contact_event(
                &follower_keys,
                vec![followee1_pubkey],
                seconds_to_datetime(3),
            ),
        ];

        assert_follow_changes(
            contact_events,
            vec![
                FollowChange::new_followed(
                    seconds_to_datetime(1),
                    follower_pubkey,
                    followee1_pubkey,
                ),
                FollowChange::new_followed(
                    seconds_to_datetime(2),
                    follower_pubkey,
                    followee2_pubkey,
                ),
                FollowChange::new_unfollowed(
                    seconds_to_datetime(3),
                    follower_pubkey,
                    followee2_pubkey,
                ),
            ],
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_follows_differ_ignores_adds_from_older_contact_list() {
        let followee1_pubkey = Keys::generate().public_key();
        let followee2_pubkey = Keys::generate().public_key();
        let follower_keys = Keys::generate();
        let follower_pubkey = follower_keys.public_key();

        let contact_events = vec![
            create_contact_event(
                &follower_keys,
                vec![followee1_pubkey],
                seconds_to_datetime(1),
            ),
            create_contact_event(
                &follower_keys,
                vec![followee1_pubkey, followee2_pubkey],
                seconds_to_datetime(2),
            ),
            create_contact_event(
                &follower_keys,
                vec![followee1_pubkey],
                seconds_to_datetime(4),
            ),
            create_contact_event(
                &follower_keys,
                vec![followee1_pubkey, followee2_pubkey],
                seconds_to_datetime(3),
            ),
        ];

        assert_follow_changes(
            contact_events,
            vec![
                FollowChange::new_followed(
                    seconds_to_datetime(1),
                    follower_pubkey,
                    followee1_pubkey,
                ),
                FollowChange::new_followed(
                    seconds_to_datetime(2),
                    follower_pubkey,
                    followee2_pubkey,
                ),
                FollowChange::new_unfollowed(
                    seconds_to_datetime(4),
                    follower_pubkey,
                    followee2_pubkey,
                ),
            ],
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_follows_differ_ignores_removes_from_older_contact_list() {
        let followee1_pubkey = Keys::generate().public_key();
        let followee2_pubkey = Keys::generate().public_key();
        let follower_keys = Keys::generate();
        let follower_pubkey = follower_keys.public_key();

        let contact_events = vec![
            create_contact_event(
                &follower_keys,
                vec![followee1_pubkey],
                seconds_to_datetime(1),
            ),
            create_contact_event(
                &follower_keys,
                vec![followee1_pubkey, followee2_pubkey],
                seconds_to_datetime(3),
            ),
            create_contact_event(
                &follower_keys,
                vec![followee1_pubkey],
                seconds_to_datetime(2),
            ),
        ];

        assert_follow_changes(
            contact_events,
            vec![
                FollowChange::new_followed(
                    seconds_to_datetime(1),
                    follower_pubkey,
                    followee1_pubkey,
                ),
                FollowChange::new_followed(
                    seconds_to_datetime(3),
                    follower_pubkey,
                    followee2_pubkey,
                ),
            ],
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_no_follows_in_initial_contact_list() {
        let follower_keys = Keys::generate();

        // An empty contact list
        let contact_events = vec![create_contact_event(
            &follower_keys,
            vec![],
            seconds_to_datetime(1000),
        )];

        assert_follow_changes(contact_events, vec![]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_unfollow_all_contacts() {
        let followee1_pubkey = Keys::generate().public_key();
        let followee2_pubkey = Keys::generate().public_key();
        let follower_keys = Keys::generate();
        let follower_pubkey = follower_keys.public_key();

        let contact_events = vec![
            create_contact_event(
                &follower_keys,
                vec![followee1_pubkey, followee2_pubkey],
                seconds_to_datetime(1),
            ),
            create_contact_event(&follower_keys, vec![], seconds_to_datetime(2)),
        ];

        assert_follow_changes(
            contact_events,
            vec![
                FollowChange::new_followed(
                    seconds_to_datetime(1),
                    follower_pubkey,
                    followee1_pubkey,
                ),
                FollowChange::new_followed(
                    seconds_to_datetime(1),
                    follower_pubkey,
                    followee2_pubkey,
                ),
                FollowChange::new_unfollowed(
                    seconds_to_datetime(2),
                    follower_pubkey,
                    followee1_pubkey,
                ),
                FollowChange::new_unfollowed(
                    seconds_to_datetime(2),
                    follower_pubkey,
                    followee2_pubkey,
                ),
            ],
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_no_changes_for_unchanged_contact_list() {
        let followee1_pubkey = Keys::generate().public_key();
        let followee2_pubkey = Keys::generate().public_key();
        let follower_keys = Keys::generate();
        let follower_pubkey = follower_keys.public_key();

        let contact_events = vec![
            create_contact_event(
                &follower_keys,
                vec![followee1_pubkey, followee2_pubkey],
                seconds_to_datetime(1000),
            ),
            create_contact_event(
                &follower_keys,
                vec![followee1_pubkey, followee2_pubkey],
                seconds_to_datetime(990),
            ),
        ];

        assert_follow_changes(
            contact_events,
            vec![
                FollowChange::new_followed(
                    seconds_to_datetime(1000),
                    follower_pubkey,
                    followee1_pubkey,
                ),
                FollowChange::new_followed(
                    seconds_to_datetime(1000),
                    follower_pubkey,
                    followee2_pubkey,
                ),
            ],
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_follow_self_ignored() {
        let follower_keys = Keys::generate();
        let follower_pubkey = follower_keys.public_key();

        let contact_events = vec![create_contact_event(
            &follower_keys,
            vec![follower_pubkey],
            seconds_to_datetime(1000),
        )];

        assert_follow_changes(contact_events, vec![]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_mixed_follow_and_unfollow_in_single_update() {
        let followee1_pubkey = Keys::generate().public_key();
        let followee2_pubkey = Keys::generate().public_key();
        let followee3_pubkey = Keys::generate().public_key();
        let follower_keys = Keys::generate();
        let follower_pubkey = follower_keys.public_key();

        let contact_events = vec![
            create_contact_event(
                &follower_keys,
                vec![followee1_pubkey, followee2_pubkey],
                seconds_to_datetime(1),
            ),
            create_contact_event(
                &follower_keys,
                vec![followee2_pubkey, followee3_pubkey],
                seconds_to_datetime(2),
            ),
        ];

        assert_follow_changes(
            contact_events,
            vec![
                FollowChange::new_followed(
                    seconds_to_datetime(1),
                    follower_pubkey,
                    followee2_pubkey,
                ),
                FollowChange::new_followed(
                    seconds_to_datetime(1),
                    follower_pubkey,
                    followee1_pubkey,
                ),
                FollowChange::new_followed(
                    seconds_to_datetime(2),
                    follower_pubkey,
                    followee3_pubkey,
                ),
                FollowChange::new_unfollowed(
                    seconds_to_datetime(2),
                    follower_pubkey,
                    followee1_pubkey,
                ),
            ],
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_final_state_with_older_list_ignored() {
        let followee1_pubkey = Keys::generate().public_key();
        let followee2_pubkey = Keys::generate().public_key();
        let followee3_pubkey = Keys::generate().public_key();
        let followee4_pubkey = Keys::generate().public_key();
        let follower_keys = Keys::generate();
        let follower_pubkey = follower_keys.public_key();

        // Monday's list: follower follows followee1
        let monday_list = create_contact_event(
            &follower_keys,
            vec![followee1_pubkey, followee2_pubkey],
            seconds_to_datetime(1),
        );

        // Tuesday's update: follower stops following followee1, starts following followee3
        let tuesday_list = create_contact_event(
            &follower_keys,
            vec![followee2_pubkey, followee3_pubkey],
            seconds_to_datetime(2),
        );

        // Wednesday's update: follower stops following followee2, starts following followee4
        let wednesday_update = create_contact_event(
            &follower_keys,
            vec![followee3_pubkey, followee4_pubkey],
            seconds_to_datetime(3),
        );

        // Apply Monday's list, then Wednesday's update, and finally the late Tuesday list
        assert_follow_changes(
            vec![monday_list, wednesday_update, tuesday_list],
            vec![
                // Monday: 1 and 2 are followed
                FollowChange::new_followed(
                    seconds_to_datetime(1),
                    follower_pubkey,
                    followee1_pubkey,
                ),
                FollowChange::new_followed(
                    seconds_to_datetime(1),
                    follower_pubkey,
                    followee2_pubkey,
                ),
                // Wednesday: 3 and 4 are followed, 1 and 2 are unfollowed
                FollowChange::new_unfollowed(
                    seconds_to_datetime(3),
                    follower_pubkey,
                    followee1_pubkey,
                ),
                FollowChange::new_unfollowed(
                    seconds_to_datetime(3),
                    follower_pubkey,
                    followee2_pubkey,
                ),
                FollowChange::new_followed(
                    seconds_to_datetime(3),
                    follower_pubkey,
                    followee3_pubkey,
                ),
                FollowChange::new_followed(
                    seconds_to_datetime(3),
                    follower_pubkey,
                    followee4_pubkey,
                ),
                // Tuesday's late update should do nothing
            ],
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_it_does_not_process_non_contact_events() {
        let follower_keys = Keys::generate();
        let followee_pubkey = Keys::generate().public_key();

        let wrong_event = create_event(
            &follower_keys,
            vec![followee_pubkey],
            seconds_to_datetime(1000),
            Kind::TextNote,
        );

        assert_follow_changes(vec![wrong_event], vec![]);
    }

    #[test]
    fn test_nos_client_tag() {
        let tag = Tag::custom(TagKind::Custom(Cow::Borrowed("client")), ["foobar"]);

        let keys = Keys::generate();
        let event_not_matching = EventBuilder::new(Kind::ContactList, "", vec![tag])
            .to_event(&keys)
            .unwrap();

        let tag = Tag::custom(TagKind::Custom(Cow::Borrowed("client")), ["nos"]);
        let event_matching = EventBuilder::new(Kind::ContactList, "", vec![tag])
            .to_event(&keys)
            .unwrap();

        let event_with_no_tag = EventBuilder::new(Kind::ContactList, "", vec![])
            .to_event(&keys)
            .unwrap();

        assert!(has_nos_agent(&event_matching));
        assert!(!has_nos_agent(&event_not_matching));
        assert!(!has_nos_agent(&event_with_no_tag));
    }

    #[track_caller]
    fn assert_follow_changes(contact_events: Vec<Event>, mut expected: Vec<FollowChange>) {
        // First, get the follow changes in a blocking context
        let mut follow_changes = tokio::task::block_in_place(|| {
            let handle = tokio::runtime::Handle::current();
            handle.block_on(async {
                get_follow_changes_from_contact_events(contact_events)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|fc| *fc)
                    .collect::<Vec<FollowChange>>()
            })
        });

        // Then do the assertions outside the block_in_place to preserve caller location
        assert_eq!(
            follow_changes.len(),
            expected.len(),
            "Number of follow changes doesn't match expected"
        );

        follow_changes.sort();
        expected.sort();

        for (i, (actual, expected)) in follow_changes.iter().zip(expected.iter()).enumerate() {
            assert_eq!(
                actual.change_type(),
                expected.change_type(),
                "Change type mismatch at index {}",
                i
            );
            assert_eq!(
                actual.followed_at(),
                expected.followed_at(),
                "Followed_at mismatch at index {}",
                i
            );
            assert_eq!(
                actual.follower().to_bech32(),
                expected.follower().to_bech32(),
                "Follower mismatch at index {}",
                i
            );
            assert_eq!(
                actual.followee().to_bech32(),
                expected.followee().to_bech32(),
                "Followee mismatch at index {}",
                i
            );
            assert_eq!(
                actual.is_trusted(),
                expected.is_trusted(),
                "Trust status mismatch at index {}",
                i
            );
        }
    }

    fn create_contact_event(
        follower: &Keys,
        followees: Vec<PublicKey>,
        created_at: DateTime<Utc>,
    ) -> Event {
        create_event(follower, followees, created_at, Kind::ContactList)
    }

    fn create_event(
        follower: &Keys,
        followees: Vec<PublicKey>,
        created_at: DateTime<Utc>,
        kind: Kind,
    ) -> Event {
        let contacts = followees
            .into_iter()
            .map(|followee| Contact::new::<String>(followee, None, None))
            .collect::<Vec<Contact>>();

        let tags = contacts.into_iter().map(|contact| {
            Tag::from_standardized_without_cell(TagStandard::PublicKey {
                public_key: contact.public_key,
                relay_url: contact.relay_url,
                alias: contact.alias,
                uppercase: false,
            })
        });

        EventBuilder::new(kind, "", tags)
            .custom_created_at((created_at.timestamp() as u64).into())
            .to_event(follower)
            .unwrap()
    }

    async fn get_follow_changes_from_contact_events(
        contact_events: Vec<Event>,
    ) -> Result<Vec<Box<FollowChange>>> {
        let (follow_change_sender, _) = channel(100);
        let repo = Arc::new(MockRepo::default());
        let follows_differ = FollowsDiffer::new(
            repo.clone(),
            Arc::new(MockNostrClient),
            follow_change_sender.clone(),
        );

        let mut follow_change_receiver = follow_change_sender.subscribe();
        let follow_changes: Arc<Mutex<Vec<Box<FollowChange>>>> = Arc::new(Mutex::new(Vec::new()));
        let shared_follow_changes = follow_changes.clone();
        let follow_change_task = tokio::spawn(async move {
            loop {
                let follow_change = follow_change_receiver.recv().await.unwrap();
                shared_follow_changes.lock().await.push(follow_change);
            }
        });

        for event in contact_events {
            follows_differ.call(Box::new(event)).await?
        }

        sleep(Duration::milliseconds(100).to_std()?).await;
        follow_change_task.abort();

        let mut follow_changes_vec = follow_changes.lock().await.clone();
        follow_changes_vec.sort();

        Ok(follow_changes_vec)
    }

    fn seconds_to_datetime(seconds: usize) -> DateTime<Utc> {
        NOW.to_utc() + Duration::seconds(seconds as i64)
    }
}
