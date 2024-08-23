use crate::repo::RepoTrait;
use crate::{
    domain::{follow::Follow, follow_change::FollowChange},
    worker_pool::{WorkerTask, WorkerTaskItem},
};
use chrono::{DateTime, Utc};
use nostr_sdk::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast::Sender;
use tracing::{debug, info};

#[derive(Default, Debug)]
struct FollowsDiff {
    stored_follow: Option<Follow>,
    exists_in_latest_contact_list: bool,
}

pub struct FollowsDiffer<T>
where
    T: RepoTrait + Sync + Send,
{
    repo: Arc<T>,
    follow_change_sender: Sender<FollowChange>,
}

impl<T> FollowsDiffer<T>
where
    T: RepoTrait + Sync + Send,
{
    pub fn new(repo: Arc<T>, follow_change_sender: Sender<FollowChange>) -> Self {
        Self {
            repo,
            follow_change_sender,
        }
    }

    /// Initializes a structure that holds the differences between the stored
    /// follows and the latest contact list.
    async fn initialize_follows_diff(
        &self,
        follower: &PublicKey,
    ) -> Result<(HashMap<PublicKey, FollowsDiff>, Option<Timestamp>)> {
        let stored_follows = self.repo.get_follows(follower).await?;
        let mut follows_diff: HashMap<PublicKey, FollowsDiff> = HashMap::new();
        let mut maybe_latest_stored_updated_at: Option<Timestamp> = None;

        for stored_follow in stored_follows {
            let updated_at = Timestamp::from(stored_follow.updated_at.timestamp() as u64);
            if let Some(ref mut latest_stored_updated_at) = maybe_latest_stored_updated_at {
                if updated_at > *latest_stored_updated_at {
                    *latest_stored_updated_at = updated_at;
                }
            } else {
                maybe_latest_stored_updated_at = Some(updated_at);
            }

            follows_diff
                .entry(stored_follow.followee)
                .or_default()
                .stored_follow = Some(stored_follow.clone());
        }

        Ok((follows_diff, maybe_latest_stored_updated_at))
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
    ) -> Result<(usize, usize, usize)> {
        let mut followed_counter = 0;
        let mut unfollowed_counter = 0;
        let mut unchanged = 0;

        for (followee, diff) in follows_diff {
            match diff.stored_follow {
                Some(mut stored_follow) => {
                    if diff.exists_in_latest_contact_list {
                        stored_follow.updated_at = event_created_at;
                        self.repo.upsert_follow(&stored_follow).await?;
                        unchanged += 1;
                    } else {
                        self.repo.delete_follow(&followee, follower).await?;
                        self.send_follow_change(FollowChange::new_unfollowed(
                            Timestamp::from(event_created_at.timestamp() as u64),
                            *follower,
                            followee,
                        ))?;
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
                        self.send_follow_change(FollowChange::new_followed(
                            Timestamp::from(event_created_at.timestamp() as u64),
                            *follower,
                            followee,
                        ))?;
                        followed_counter += 1;
                    } else {
                        debug!("Skipping self-follow for {}", followee);
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

impl<T> WorkerTask<Box<Event>> for FollowsDiffer<T>
where
    T: RepoTrait + Sync + Send,
{
    async fn call(&self, worker_task_item: WorkerTaskItem<Box<Event>>) -> Result<()> {
        let WorkerTaskItem { item: event } = worker_task_item;
        let follower = event.pubkey;

        let event_created_at = convert_timestamp(event.created_at.as_u64())?;

        // Get the stored follows and the latest update time from the database
        let (mut follows_diff, maybe_latest_stored_updated_at) =
            self.initialize_follows_diff(&follower).await?;

        // Populate the new follows from the event tags
        self.populate_new_follows(&mut follows_diff, &event);

        // Check if the event is older than the latest stored update and skip if so
        if let Some(latest_stored_updated_at) = maybe_latest_stored_updated_at {
            if event.created_at <= latest_stored_updated_at {
                debug!(
                    "Skipping follow list for {} as it's older than the last update",
                    follower
                );
                return Ok(());
            }
        }

        let first_seen = follows_diff.is_empty();
        // Process the follows_diff and apply changes
        let (followed_counter, unfollowed_counter, unchanged) = self
            .process_follows_diff(follows_diff, &follower, event_created_at)
            .await?;

        if let Some(log_line) = log_line(
            follower,
            event.created_at,
            followed_counter,
            unfollowed_counter,
            unchanged,
            first_seen,
            maybe_latest_stored_updated_at,
        ) {
            info!("{}", log_line);
        }

        Ok(())
    }
}

fn log_line(
    follower: PublicKey,
    event_created_at: Timestamp,
    followed_counter: usize,
    unfollowed_counter: usize,
    unchanged: usize,
    first_seen: bool,
    maybe_latest_stored_updated_at: Option<Timestamp>,
) -> Option<String> {
    let timestamp_diff = if let Some(latest_stored_updated_at) = maybe_latest_stored_updated_at {
        format!(
            "[{}->{}]",
            latest_stored_updated_at.to_human_datetime(),
            event_created_at.to_human_datetime()
        )
    } else {
        format!("[new->{}]", event_created_at.to_human_datetime())
    };

    if first_seen && followed_counter > 0 {
        return Some(format!(
            "Pubkey {}: date {}, {} followed, new follows list",
            follower, timestamp_diff, followed_counter,
        ));
    } else if followed_counter > 0 || unfollowed_counter > 0 {
        return Some(format!(
            "Pubkey {}: date {}, {} followed, {} unfollowed, {} unchanged",
            follower, timestamp_diff, followed_counter, unfollowed_counter, unchanged,
        ));
    }

    debug!(
        "Pubkey {}: date {}, {} followed, {} unfollowed, {} unchanged, first seen: {}",
        follower, timestamp_diff, followed_counter, unfollowed_counter, unchanged, first_seen
    );

    None
}

fn convert_timestamp(timestamp: u64) -> Result<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp(timestamp as i64, 0).ok_or("Invalid timestamp".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::follow::Follow;
    use crate::repo::RepoError;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::broadcast::channel;
    use tokio::sync::Mutex;
    use tokio::time::{sleep, Duration};

    #[derive(Default)]
    struct MockRepo {
        follows: Arc<Mutex<HashMap<PublicKey, Vec<Follow>>>>,
    }

    impl RepoTrait for MockRepo {
        async fn upsert_follow(&self, follow: &Follow) -> Result<(), RepoError> {
            let mut follows = self.follows.lock().await;
            let entry = follows.entry(follow.follower).or_default();
            let follow = follow.clone();
            entry.retain(|f| f.followee != follow.followee);
            entry.push(follow);
            Ok(())
        }

        async fn delete_follow(
            &self,
            followee: &PublicKey,
            follower: &PublicKey,
        ) -> Result<(), RepoError> {
            let mut follows = self.follows.lock().await;
            let entry = follows.entry(*follower).or_default();
            entry.retain(|f| f.followee != *followee);
            Ok(())
        }

        async fn get_follows(&self, follower: &PublicKey) -> Result<Vec<Follow>, RepoError> {
            let follows = self.follows.lock().await;
            Ok(follows.get(follower).cloned().unwrap_or_default())
        }
    }

    #[tokio::test]
    async fn test_follows_differ_for_new_contact_list() {
        let followee_pubkey = Keys::generate().public_key();
        let follower_keys = Keys::generate();
        let follower_pubkey = follower_keys.public_key();

        let contact_events = vec![create_contact_event(
            &follower_keys,
            vec![followee_pubkey],
            1000000000,
        )];

        assert_follow_changes(
            contact_events,
            vec![FollowChange::new_followed(
                1000000000.into(),
                follower_pubkey,
                followee_pubkey,
            )],
        )
        .await;
    }

    #[tokio::test]
    async fn test_follows_differ_for_new_added_contact() {
        let followee1_pubkey = Keys::generate().public_key();
        let followee2_pubkey = Keys::generate().public_key();
        let follower_keys = Keys::generate();
        let follower_pubkey = follower_keys.public_key();

        let contact_events = vec![
            create_contact_event(&follower_keys, vec![followee1_pubkey], 1000000000),
            create_contact_event(
                &follower_keys,
                vec![followee1_pubkey, followee2_pubkey],
                1000000010,
            ),
        ];

        assert_follow_changes(
            contact_events,
            vec![
                FollowChange::new_followed(1000000000.into(), follower_pubkey, followee1_pubkey),
                FollowChange::new_followed(1000000010.into(), follower_pubkey, followee2_pubkey),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_follows_differ_for_removed_contact() {
        let followee1_pubkey = Keys::generate().public_key();
        let followee2_pubkey = Keys::generate().public_key();
        let follower_keys = Keys::generate();
        let follower_pubkey = follower_keys.public_key();

        let contact_events = vec![
            create_contact_event(&follower_keys, vec![followee1_pubkey], 1000000000),
            create_contact_event(
                &follower_keys,
                vec![followee1_pubkey, followee2_pubkey],
                1000000010,
            ),
            create_contact_event(&follower_keys, vec![followee1_pubkey], 1000000020),
        ];

        assert_follow_changes(
            contact_events,
            vec![
                FollowChange::new_followed(1000000000.into(), follower_pubkey, followee1_pubkey),
                FollowChange::new_followed(1000000010.into(), follower_pubkey, followee2_pubkey),
                FollowChange::new_unfollowed(1000000020.into(), follower_pubkey, followee2_pubkey),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_follows_differ_ignores_adds_from_older_contact_list() {
        let followee1_pubkey = Keys::generate().public_key();
        let followee2_pubkey = Keys::generate().public_key();
        let follower_keys = Keys::generate();
        let follower_pubkey = follower_keys.public_key();

        let contact_events = vec![
            create_contact_event(&follower_keys, vec![followee1_pubkey], 1000000000),
            create_contact_event(
                &follower_keys,
                vec![followee1_pubkey, followee2_pubkey],
                1000000010,
            ),
            create_contact_event(&follower_keys, vec![followee1_pubkey], 1000000020),
            create_contact_event(
                &follower_keys,
                vec![followee1_pubkey, followee2_pubkey],
                1000000015,
            ),
        ];

        assert_follow_changes(
            contact_events,
            vec![
                FollowChange::new_followed(1000000000.into(), follower_pubkey, followee1_pubkey),
                FollowChange::new_followed(1000000010.into(), follower_pubkey, followee2_pubkey),
                FollowChange::new_unfollowed(1000000020.into(), follower_pubkey, followee2_pubkey),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_follows_differ_ignores_removes_from_older_contact_list() {
        let followee1_pubkey = Keys::generate().public_key();
        let followee2_pubkey = Keys::generate().public_key();
        let follower_keys = Keys::generate();
        let follower_pubkey = follower_keys.public_key();

        let contact_events = vec![
            create_contact_event(&follower_keys, vec![followee1_pubkey], 1000000000),
            create_contact_event(
                &follower_keys,
                vec![followee1_pubkey, followee2_pubkey],
                1000000010,
            ),
            create_contact_event(&follower_keys, vec![followee1_pubkey], 1000000005),
        ];

        assert_follow_changes(
            contact_events,
            vec![
                FollowChange::new_followed(1000000000.into(), follower_pubkey, followee1_pubkey),
                FollowChange::new_followed(1000000010.into(), follower_pubkey, followee2_pubkey),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_no_follows_in_initial_contact_list() {
        let follower_keys = Keys::generate();

        // An empty contact list
        let contact_events = vec![create_contact_event(&follower_keys, vec![], 1000000000)];

        assert_follow_changes(contact_events, vec![]).await;
    }

    #[tokio::test]
    async fn test_unfollow_all_contacts() {
        let followee1_pubkey = Keys::generate().public_key();
        let followee2_pubkey = Keys::generate().public_key();
        let follower_keys = Keys::generate();
        let follower_pubkey = follower_keys.public_key();

        let contact_events = vec![
            create_contact_event(
                &follower_keys,
                vec![followee1_pubkey, followee2_pubkey],
                1000000000,
            ),
            create_contact_event(&follower_keys, vec![], 1000000010),
        ];

        assert_follow_changes(
            contact_events,
            vec![
                FollowChange::new_followed(1000000000.into(), follower_pubkey, followee1_pubkey),
                FollowChange::new_followed(1000000000.into(), follower_pubkey, followee2_pubkey),
                FollowChange::new_unfollowed(1000000010.into(), follower_pubkey, followee1_pubkey),
                FollowChange::new_unfollowed(1000000010.into(), follower_pubkey, followee2_pubkey),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_no_changes_for_unchanged_contact_list() {
        let followee1_pubkey = Keys::generate().public_key();
        let followee2_pubkey = Keys::generate().public_key();
        let follower_keys = Keys::generate();
        let follower_pubkey = follower_keys.public_key();

        let contact_events = vec![
            create_contact_event(
                &follower_keys,
                vec![followee1_pubkey, followee2_pubkey],
                1000000000,
            ),
            create_contact_event(
                &follower_keys,
                vec![followee1_pubkey, followee2_pubkey],
                1000000010,
            ),
        ];

        assert_follow_changes(
            contact_events,
            vec![
                FollowChange::new_followed(1000000000.into(), follower_pubkey, followee1_pubkey),
                FollowChange::new_followed(1000000000.into(), follower_pubkey, followee2_pubkey),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_follow_self_ignored() {
        let follower_keys = Keys::generate();
        let follower_pubkey = follower_keys.public_key();

        let contact_events = vec![create_contact_event(
            &follower_keys,
            vec![follower_pubkey],
            1000000000,
        )];

        assert_follow_changes(contact_events, vec![]).await;
    }

    #[tokio::test]
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
                1000000000,
            ),
            create_contact_event(
                &follower_keys,
                vec![followee2_pubkey, followee3_pubkey],
                1000000010,
            ),
        ];

        assert_follow_changes(
            contact_events,
            vec![
                FollowChange::new_followed(1000000000.into(), follower_pubkey, followee1_pubkey),
                FollowChange::new_followed(1000000000.into(), follower_pubkey, followee2_pubkey),
                FollowChange::new_unfollowed(1000000010.into(), follower_pubkey, followee1_pubkey),
                FollowChange::new_followed(1000000010.into(), follower_pubkey, followee3_pubkey),
            ],
        )
        .await;
    }

    async fn assert_follow_changes(contact_events: Vec<Event>, mut expected: Vec<FollowChange>) {
        let follow_changes = get_follow_changes_from_contact_events(contact_events)
            .await
            .unwrap();

        expected.sort(); // Sort the expected follow changes
        assert_eq!(follow_changes, expected);
    }

    fn create_contact_event(follower: &Keys, followees: Vec<PublicKey>, created_at: u64) -> Event {
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

        EventBuilder::new(Kind::ContactList, "", tags)
            .custom_created_at(created_at.into())
            .to_event(&follower)
            .unwrap()
    }

    async fn get_follow_changes_from_contact_events(
        contact_events: Vec<Event>,
    ) -> Result<Vec<FollowChange>> {
        let (follow_change_sender, _) = channel(100);
        let repo = Arc::new(MockRepo::default());
        let follows_differ = FollowsDiffer::new(repo.clone(), follow_change_sender.clone());

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
                .await
                .unwrap();
        }

        sleep(Duration::from_millis(100)).await;
        follow_change_task.abort();

        let mut follow_changes_vec = follow_changes.lock().await.clone();
        follow_changes_vec.sort();

        Ok(follow_changes_vec)
    }
}
