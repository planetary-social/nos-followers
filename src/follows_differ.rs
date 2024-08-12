use crate::repo::Repo;
use crate::send_with_checks::SendWithChecks;
use crate::{domain::follow::Follow, worker_pool::WorkerTask};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, FixedOffset};
use nostr_sdk::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
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
    repo: Arc<Repo>,
    diff_result_tx: Sender<FollowChange>,
}

impl FollowsDiffer {
    pub fn new(repo: Arc<Repo>, diff_result_tx: Sender<FollowChange>) -> Self {
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
        let event_created_at = timestamp_to_datetime(event.created_at)?;

        // Populate stored follows
        let stored_follows = self
            .repo
            .get_follows(&follower)
            .await
            .context("Failed to get follows")?;

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
                        updated_at: event_created_at,
                        created_at: event_created_at,
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

fn timestamp_to_datetime(timestamp: Timestamp) -> Result<DateTime<FixedOffset>> {
    match DateTime::from_timestamp(timestamp.as_u64() as i64, 0) {
        Some(dt) => Ok(dt.fixed_offset()),
        None => {
            bail!(
                "Failed to convert timestamp to datetime: {}",
                timestamp.as_u64()
            )
        }
    }
}
