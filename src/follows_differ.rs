use crate::repo::Repo;
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

pub struct FollowsDiffer {
    repo: Arc<Repo>,
    follow_change_sender: Sender<FollowChange>,
}

impl FollowsDiffer {
    pub fn new(repo: Arc<Repo>, follow_change_sender: Sender<FollowChange>) -> Self {
        Self {
            repo,
            follow_change_sender,
        }
    }
}

impl WorkerTask<Box<Event>> for FollowsDiffer {
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
