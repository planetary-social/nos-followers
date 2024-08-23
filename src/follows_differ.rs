use crate::repo::RepoTrait;
use crate::{
    domain::{follow::Follow, follow_change::FollowChange},
    worker_pool::{WorkerTask, WorkerTaskItem},
};
use chrono::DateTime;
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
}

impl<T> WorkerTask<Box<Event>> for FollowsDiffer<T>
where
    T: RepoTrait + Sync + Send,
{
    async fn call(&self, worker_task_item: WorkerTaskItem<Box<Event>>) -> Result<()> {
        let WorkerTaskItem { item: event } = worker_task_item;

        let mut followed_counter = 0;
        let mut unfollowed_counter = 0;
        let mut unchanged = 0;
        let follower = event.pubkey;
        let mut follows_diff: HashMap<PublicKey, FollowsDiff> = HashMap::new();

        let date_time = DateTime::from_timestamp(event.created_at.as_u64() as i64, 0)
            .ok_or("Failed to convert timestamp to datetime")?;
        let event_created_at = date_time.fixed_offset();

        // Populate stored follows
        let stored_follows = self.repo.get_follows(&follower).await?;

        let mut maybe_latest_stored_updated_at: Option<Timestamp> = None;
        for stored_follow in stored_follows {
            if let Some(ref mut latest_stored_updated_at) = maybe_latest_stored_updated_at {
                if (stored_follow.updated_at.timestamp() as u64) > latest_stored_updated_at.as_u64()
                {
                    *latest_stored_updated_at =
                        Timestamp::from(stored_follow.updated_at.timestamp() as u64);
                }
            } else {
                maybe_latest_stored_updated_at =
                    Some(Timestamp::from(stored_follow.updated_at.timestamp() as u64));
            };

            follows_diff
                .entry(stored_follow.followee)
                .or_default()
                .stored_follow = Some(stored_follow.clone());
        }

        let first_seen = follows_diff.is_empty();

        // Populate new follows
        for tag in event.tags.iter() {
            if let Some(TagStandard::PublicKey { public_key, .. }) = tag.as_standardized() {
                follows_diff
                    .entry(*public_key)
                    .or_default()
                    .exists_in_latest_contact_list = true;
            }
        }

        if let Some(latest_stored_updated_at) = maybe_latest_stored_updated_at {
            if event.created_at <= latest_stored_updated_at {
                debug!(
                    "Skipping follow list for {} as it's older than the last update",
                    follower
                );
                return Ok(());
            }
        }

        // Process follows_diff
        for (
            followee,
            FollowsDiff {
                stored_follow: maybe_stored_follow,
                exists_in_latest_contact_list,
            },
        ) in follows_diff.into_iter()
        {
            match maybe_stored_follow {
                // We have a DB entry for this followee
                Some(mut stored_follow) => {
                    if exists_in_latest_contact_list {
                        // Still following same followee, run an upsert that just updates the date
                        stored_follow.updated_at = event_created_at;

                        self.repo.upsert_follow(&stored_follow).await?;

                        unchanged += 1;
                    } else {
                        // Doesn't exist in the new follows list so we delete the follow
                        self.repo.delete_follow(&followee, &follower).await?;

                        let follow_change =
                            FollowChange::new_unfollowed(event.created_at, follower, followee);
                        self.follow_change_sender.send(follow_change)?;
                        unfollowed_counter += 1;
                    }
                }
                None => {
                    if followee == follower {
                        debug!("Skipping self-follow for {}", followee);
                        continue;
                    }

                    // There's no existing follow entry for this followee so this is a new follow
                    let follow = Follow {
                        followee,
                        follower,
                        updated_at: event_created_at,
                        created_at: event_created_at,
                    };

                    self.repo.upsert_follow(&follow).await?;

                    let follow_change =
                        FollowChange::new_followed(event.created_at, follower, followee);

                    self.follow_change_sender.send(follow_change)?;
                    followed_counter += 1;
                }
            }
        }

        // If we have a new follow list, we log it if it has any follows to avoid noise
        if first_seen && followed_counter > 0 {
            info!(
                "Pubkey {}: date {}, {} followed, new follows list",
                follower,
                event.created_at.to_human_datetime(),
                followed_counter,
            );

            return Ok(());
        }

        // If nothing changed, we don't log anything, it's just noise from older events
        if followed_counter > 0 || unfollowed_counter > 0 {
            info!(
                "Pubkey {}: date {}, {} followed, {} unfollowed, {} unchanged",
                follower,
                event.created_at.to_human_datetime(),
                followed_counter,
                unfollowed_counter,
                unchanged
            );
        }

        Ok(())
    }
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
