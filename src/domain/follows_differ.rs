use crate::refresh_friendly_id::{fetch_account_info, AccountInfo};
use crate::relay_subscriber::GetEventsOf;
use crate::repo::RepoTrait;
use crate::{
    domain::{follow::Follow, follow_change::FollowChange},
    worker_pool::{WorkerTask, WorkerTaskItem},
};
use chrono::{DateTime, Utc};
use metrics::counter;
use nostr_sdk::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast::Sender;
use tokio::time::{Duration, Instant};
use tracing::{debug, info};

#[derive(Default, Debug)]
struct FollowsDiff {
    stored_follow: Option<Follow>,
    exists_in_latest_contact_list: bool,
}

pub struct FollowsDiffer<T, U>
where
    T: RepoTrait + Sync + Send,
    U: GetEventsOf + Sync + Send,
{
    repo: Arc<T>,
    nostr_client: Arc<U>,
    follow_change_sender: Sender<FollowChange>,
}

impl<T, U> FollowsDiffer<T, U>
where
    T: RepoTrait + Sync + Send,
    U: GetEventsOf + Sync + Send,
{
    pub fn new(
        repo: Arc<T>,
        nostr_client: Arc<U>,
        follow_change_sender: Sender<FollowChange>,
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
        maybe_last_seen_contact_list_at: Option<DateTime<Utc>>,
    ) -> Result<(usize, usize, usize)> {
        let mut followed_counter = 0;
        let mut unfollowed_counter = 0;
        let mut unchanged = 0;

        let send_notifications = should_send_notifications(
            &self.nostr_client,
            maybe_last_seen_contact_list_at,
            follower,
            event_created_at,
        )
        .await?;

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
                                FollowChange::new_unfollowed(event_created_at, *follower, followee);
                            self.send_follow_change(follow_change)?;
                        }
                        unfollowed_counter += 1;
                    }
                }
                None => {
                    if followee != *follower {
                        let follow = Follow {
                            followee,
                            follower: *follower,
                            updated_at: event_created_at,
                            created_at: event_created_at,
                        };
                        self.repo.upsert_follow(&follow).await?;

                        if send_notifications {
                            let follow_change =
                                FollowChange::new_followed(event_created_at, *follower, followee);
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
        self.follow_change_sender.send(follow_change)?;
        Ok(())
    }
}

const ONE_DAY_DURATION: Duration = Duration::from_secs(60 * 60 * 24);
const ONE_WEEK_DURATION: Duration = Duration::from_secs(60 * 60 * 24 * 7);

impl<T, U> WorkerTask<Box<Event>> for FollowsDiffer<T, U>
where
    T: RepoTrait + Sync + Send,
    U: GetEventsOf + Sync + Send,
{
    async fn call(&self, worker_task_item: WorkerTaskItem<Box<Event>>) -> Result<()> {
        let WorkerTaskItem { item: event } = worker_task_item;

        let one_day_ago = Timestamp::from((Instant::now() - ONE_DAY_DURATION).elapsed().as_secs());

        if event.kind != Kind::ContactList || event.created_at < one_day_ago {
            return Ok(());
        }

        let follower = event.pubkey;

        let event_created_at = convert_timestamp(event.created_at.as_u64())?;
        let maybe_last_seen_contact_list = self
            .repo
            .maybe_update_last_contact_list_at(&follower, &event_created_at)
            .await?;

        // Check if the event is older than the latest stored update and skip if so
        if let Some(last_seen_contact_list) = maybe_last_seen_contact_list {
            if event.created_at <= (last_seen_contact_list.timestamp() as u64).into() {
                debug!(
                    "Skipping follow list for {} as it's older than the last update",
                    follower.to_bech32().unwrap_or_default()
                );
                return Ok(());
            }
        }

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
                maybe_last_seen_contact_list,
            )
            .await?;

        if let Some(log_line) = log_line(
            follower,
            followed_counter,
            unfollowed_counter,
            unchanged,
            maybe_last_seen_contact_list,
            &event,
        ) {
            info!("{}", log_line);
        }

        counter!("contact_lists_processed").increment(1);
        Ok(())
    }
}

/// An heuristic to decide if we should send notifications for a contact list
async fn should_send_notifications<T: GetEventsOf>(
    nostr_client: &Arc<T>,
    maybe_last_seen_contact_list: Option<DateTime<Utc>>,
    follower: &PublicKey,
    event_created_at: DateTime<Utc>,
) -> Result<bool> {
    if maybe_last_seen_contact_list.is_none() {
        // This is the first time we see a contact list from this follower.
        let AccountInfo { created_at, .. } = fetch_account_info(nostr_client, follower).await;
        let Some(follower_created_at) = created_at else {
            return Ok(true);
        };

        if (event_created_at - follower_created_at).to_std()? > ONE_WEEK_DURATION {
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

fn log_line(
    follower: PublicKey,
    followed_counter: usize,
    unfollowed_counter: usize,
    unchanged: usize,
    maybe_last_seen_contact_list: Option<DateTime<Utc>>,
    event: &Event,
) -> Option<String> {
    if unchanged > 0 && followed_counter == 0 && unfollowed_counter == 0 {
        // This one is not interesting
        return None;
    }

    let human_event_created_at = event.created_at.to_human_datetime();

    let last_seen_contact_list = if let Some(last_seen_contact_list) = maybe_last_seen_contact_list
    {
        last_seen_contact_list.to_rfc3339()
    } else {
        "new".to_string()
    };

    let timestamp_diff = format!("[{}->{}]", last_seen_contact_list, human_event_created_at);

    if maybe_last_seen_contact_list.is_none() {
        return Some(format!(
            "Npub {}: date {}, {} followed, new follows list",
            follower.to_bech32().unwrap_or_default(),
            timestamp_diff,
            followed_counter,
        ));
    }

    // Investigate states in which there are no followees but there are unfollowed followees
    if followed_counter == 0 && unfollowed_counter > 0 && unchanged == 0 {
        return Some(format!(
            "ALL UNFOLLOWED: Npub {}: date {}, {} unfollowed, {} unchanged, {}",
            follower.to_bech32().unwrap_or_default(),
            timestamp_diff,
            unfollowed_counter,
            unchanged,
            event.as_json()
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

fn convert_timestamp(timestamp: u64) -> Result<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp(timestamp as i64, 0).ok_or("Invalid timestamp".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::follow::Follow;
    use crate::repo::RepoError;
    use chrono::{Duration, Utc};
    use nostr_sdk::PublicKey;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::UNIX_EPOCH;
    use tokio::sync::broadcast::channel;
    use tokio::sync::Mutex;
    use tokio::time::sleep;

    #[derive(Default)]

    struct MockNostrClient;
    impl GetEventsOf for MockNostrClient {
        async fn get_events_of(
            &self,
            _filter: Vec<Filter>,
            _timeout: Option<core::time::Duration>,
        ) -> Result<Vec<Event>, nostr_sdk::client::Error> {
            Ok(vec![])
        }
    }
    struct MockRepo {
        follows: Arc<Mutex<HashMap<PublicKey, (Vec<Follow>, Option<DateTime<Utc>>)>>>,
    }

    impl Default for MockRepo {
        fn default() -> Self {
            Self {
                follows: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }

    impl RepoTrait for MockRepo {
        async fn maybe_update_last_contact_list_at(
            &self,
            public_key: &PublicKey,
            at: &DateTime<Utc>,
        ) -> Result<Option<DateTime<Utc>>, RepoError> {
            let mut follows = self.follows.lock().await;
            let entry = follows.entry(*public_key).or_default();
            let previous_value = entry.1;

            if previous_value.is_none() || at > previous_value.as_ref().unwrap() {
                entry.1 = Some(*at);
            }

            Ok(previous_value)
        }

        async fn upsert_follow(&self, follow: &Follow) -> Result<(), RepoError> {
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

        async fn get_follows(&self, follower: &PublicKey) -> Result<Vec<Follow>, RepoError> {
            let follows = self.follows.lock().await;
            Ok(follows.get(follower).cloned().unwrap_or_default().0)
        }
    }

    #[tokio::test(start_paused = true)]
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
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
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
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
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
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
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
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
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
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_no_follows_in_initial_contact_list() {
        let follower_keys = Keys::generate();

        // An empty contact list
        let contact_events = vec![create_contact_event(
            &follower_keys,
            vec![],
            seconds_to_datetime(1000),
        )];

        assert_follow_changes(contact_events, vec![]).await;
    }

    #[tokio::test(start_paused = true)]
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
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
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
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_follow_self_ignored() {
        let follower_keys = Keys::generate();
        let follower_pubkey = follower_keys.public_key();

        let contact_events = vec![create_contact_event(
            &follower_keys,
            vec![follower_pubkey],
            seconds_to_datetime(1000),
        )];

        assert_follow_changes(contact_events, vec![]).await;
    }

    #[tokio::test(start_paused = true)]
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
                FollowChange::new_followed(
                    seconds_to_datetime(2),
                    follower_pubkey,
                    followee3_pubkey,
                ),
            ],
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
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
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_it_does_not_process_non_contact_events() {
        let follower_keys = Keys::generate();
        let followee_pubkey = Keys::generate().public_key();

        let wrong_event = create_event(
            &follower_keys,
            vec![followee_pubkey],
            seconds_to_datetime(1000),
            Kind::TextNote,
        );

        assert_follow_changes(vec![wrong_event], vec![]).await;
    }

    async fn assert_follow_changes(contact_events: Vec<Event>, mut expected: Vec<FollowChange>) {
        let follow_changes = get_follow_changes_from_contact_events(contact_events)
            .await
            .unwrap();

        expected.sort(); // Sort the expected follow changes
        assert_eq!(follow_changes, expected);
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
    ) -> Result<Vec<FollowChange>> {
        let (follow_change_sender, _) = channel(100);
        let repo = Arc::new(MockRepo::default());
        let follows_differ = FollowsDiffer::new(
            repo.clone(),
            Arc::new(MockNostrClient),
            follow_change_sender.clone(),
        );

        let mut follow_change_receiver = follow_change_sender.subscribe();
        let follow_changes: Arc<Mutex<Vec<FollowChange>>> = Arc::new(Mutex::new(Vec::new()));
        let shared_follow_changes = follow_changes.clone();
        let follow_change_task = tokio::spawn(async move {
            loop {
                let follow_change = follow_change_receiver.recv().await.unwrap();
                shared_follow_changes.lock().await.push(follow_change);
            }
        });

        for event in contact_events {
            follows_differ
                .call(WorkerTaskItem::new(Box::new(event)))
                .await?
        }

        sleep(Duration::milliseconds(100).to_std()?).await;
        follow_change_task.abort();

        let mut follow_changes_vec = follow_changes.lock().await.clone();
        follow_changes_vec.sort();

        Ok(follow_changes_vec)
    }

    fn seconds_to_datetime(seconds: i64) -> DateTime<Utc> {
        DateTime::<Utc>::from(
            UNIX_EPOCH
                + Duration::days(100).to_std().unwrap()
                + Duration::seconds(seconds).to_std().unwrap(),
        )
    }
}
