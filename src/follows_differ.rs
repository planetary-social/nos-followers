use crate::repo::Repo;
use crate::{domain::follow::Follow, worker_pool::WorkerTask};
use nostr_sdk::prelude::*;
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;
use tracing::{error, info};

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
    async fn call(&self, event: Box<Event>) {
        let mut followed_counter = 0;
        let mut unfollowed_counter = 0;
        let mut unchanged = 0;
        let follower = event.pubkey;
        let mut follows_diff: HashMap<PublicKey, FollowsDiff> = HashMap::new();

        // Populate stored follows
        match self.repo.get_follows(&follower).await {
            Ok(stored_follows) => {
                for stored_follow in stored_follows {
                    follows_diff
                        .entry(stored_follow.followee)
                        .or_default()
                        .stored_follow = Some(stored_follow.clone());
                }
            }
            Err(e) => {
                error!("Failed to get follows: {}", e);
                return;
            }
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
                    if event.created_at <= stored_follow.updated_at {
                        // We only process follow lists that are newer than the last update
                        continue;
                    }

                    if exists_in_latest_contact_list {
                        // Still following same followee, run an upsert that just updates the date
                        stored_follow.updated_at = event.created_at;

                        if let Err(e) = self.repo.upsert_follow(&stored_follow, None).await {
                            error!("Failed to upsert follow: {}", e);
                        }

                        unchanged += 1;
                    } else {
                        // Doesn't exist in the new follows list so we delete the follow
                        if let Err(e) = self.repo.delete_follow(&followee, &follower, None).await {
                            error!("Failed to delete follow: {}", e);
                        }

                        if let Err(e) = self
                            .diff_result_tx
                            .send(FollowChange::Unfollowed {
                                at: event.created_at,
                                follower,
                                followee,
                            })
                            .await
                        {
                            error!("Failed to send follow change: {}", e);
                        }
                        unfollowed_counter += 1;
                    }
                }
                None => {
                    // There's no existing follow entry for this followee so this is a new follow
                    let follow = Follow {
                        followee,
                        follower,
                        updated_at: event.created_at,
                        created_at: event.created_at,
                    };

                    if let Err(e) = self.repo.upsert_follow(&follow, None).await {
                        error!("Failed to upsert follow: {}", e);
                    }

                    if let Err(e) = self
                        .diff_result_tx
                        .send(FollowChange::Followed {
                            at: event.created_at,
                            follower,
                            followee,
                        })
                        .await
                    {
                        error!("Failed to send follow change: {}", e);
                    }
                    followed_counter += 1;
                }
            }
        }

        if first_seen {
            info!(
                "Pubkey {}: date {}, {} followed, new follows list",
                follower,
                event.created_at.to_human_datetime(),
                followed_counter
            );
        } else {
            info!(
                "Pubkey {}: date {}, {} followed, {} unfollowed, {} unchanged",
                follower,
                event.created_at.to_human_datetime(),
                followed_counter,
                unfollowed_counter,
                unchanged
            );
        }
    }
}
