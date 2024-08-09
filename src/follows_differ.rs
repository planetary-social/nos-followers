use crate::repo::Repo;
use crate::send_with_checks::SendWithChecks;
use crate::{domain::follow::Follow, worker_pool::WorkerTask};
use anyhow::{Context, Result};
use chrono::{DateTime, FixedOffset};
use nostr_sdk::prelude::*;
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;
use tracing::{debug, info};

#[derive(Default, Debug)]
struct FollowsDiff {
    stored_follow: Option<Follow>,
    exists_in_latest_contact_list: bool,
}

pub enum FollowChange {
    Followed {
        at: Timestamp,
        follower: PublicKey,
        followee: PublicKey,
    },
    Unfollowed {
        at: Timestamp,
        follower: PublicKey,
        followee: PublicKey,
    },
}

pub struct FollowsDiffer {
    repo: Repo,
    diff_result_tx: Sender<FollowChange>,
}

impl FollowsDiffer {
    pub fn new(repo: Repo, diff_result_tx: Sender<FollowChange>) -> Self {
        Self {
            repo,
            diff_result_tx,
        }
    }
}

impl WorkerTask<Box<Event>> for FollowsDiffer {
    async fn call(&self, event: Box<Event>) -> Result<()> {
        let mut followed_counter = 0;
        let mut unfollowed_counter = 0;
        let mut unchanged = 0;
        let follower = event.pubkey;
        let mut follows_diff: HashMap<PublicKey, FollowsDiff> = HashMap::new();

        // Populate stored follows
        let stored_follows = self
            .repo
            .get_follows(&follower)
            .await
            .context("Failed to get follows")?;

        for stored_follow in stored_follows {
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
                    if event.created_at.as_u64() <= stored_follow.updated_at.timestamp() as u64 {
                        // We only process follow lists that are newer than the last update
                        debug!(
                            "Skipping follow list for {} as it's older than the last update",
                            followee
                        );
                        continue;
                    }

                    if exists_in_latest_contact_list {
                        // Still following same followee, run an upsert that just updates the date
                        stored_follow.updated_at = timestamp_to_datetime(event.created_at);

                        self.repo
                            .upsert_follow(&stored_follow)
                            .await
                            .context(format!("Failed to upsert follow {}", follower))?;

                        unchanged += 1;
                    } else {
                        // Doesn't exist in the new follows list so we delete the follow
                        self.repo
                            .delete_follow(&followee, &follower)
                            .await
                            .context(format!("Failed to delete follow for {}", follower))?;

                        let follow_change = FollowChange::Unfollowed {
                            at: event.created_at,
                            follower,
                            followee,
                        };
                        self.diff_result_tx
                            .send_with_checks(follow_change)
                            .context(format!("Failed to send follow change for {}", follower))?;
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
                        updated_at: timestamp_to_datetime(event.created_at),
                        created_at: timestamp_to_datetime(event.created_at),
                    };

                    self.repo
                        .upsert_follow(&follow)
                        .await
                        .context(format!("Failed to upsert follow {}", follower))?;

                    let follow_change = FollowChange::Followed {
                        at: event.created_at,
                        follower,
                        followee,
                    };

                    self.diff_result_tx
                        .send_with_checks(follow_change)
                        .context(format!("Failed to send follow change for {}", follower))?;
                    followed_counter += 1;
                }
            }
        }

        if first_seen {
            info!(
                "Pubkey {}: date {}, {} followed, new follows list",
                follower,
                event.created_at.to_human_datetime(),
                followed_counter,
            );

            return Ok(());
        }

        info!(
            "Pubkey {}: date {}, {} followed, {} unfollowed, {} unchanged",
            follower,
            event.created_at.to_human_datetime(),
            followed_counter,
            unfollowed_counter,
            unchanged
        );

        Ok(())
    }
}

fn timestamp_to_datetime(timestamp: Timestamp) -> DateTime<FixedOffset> {
    DateTime::from_timestamp(timestamp.as_u64() as i64, 0)
        .unwrap()
        .fixed_offset()
}
