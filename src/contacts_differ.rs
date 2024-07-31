use crate::repo::Repo;
use crate::{domain::follow::Follow, worker_pool::WorkerTask};
use nostr_sdk::prelude::*;
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;
use tracing::{debug, error};

#[derive(Default, Debug)]
struct FollowsDiff {
    old_updated_at: Option<Timestamp>,
    new_updated_at: Option<Timestamp>,
}

pub enum FollowChangeType {
    Followed(Timestamp),
    Unfollowed(Timestamp),
}

pub struct FollowChange {
    pub follow_type: FollowChangeType,
    pub followee: PublicKey,
    pub follower: PublicKey,
}

pub struct ContactsDiffer {
    repo: Repo,
    diff_result_tx: Sender<FollowChange>,
}

impl ContactsDiffer {
    pub fn new(repo: Repo, diff_result_tx: Sender<FollowChange>) -> Self {
        Self {
            repo,
            diff_result_tx,
        }
    }
}

impl WorkerTask<Box<Event>> for ContactsDiffer {
    async fn call(&self, event: Box<Event>) {
        let mut follows_diff: HashMap<PublicKey, FollowsDiff> = HashMap::new();
        let follower = event.pubkey;
        debug!("Processing contact list for {}", follower);

        // Populate old follows
        match self.repo.get_follows(follower).await {
            Ok(follows) => {
                for old_follow in follows {
                    follows_diff
                        .entry(old_follow.followee)
                        .or_default()
                        .old_updated_at = Some(old_follow.updated_at);
                }
            }
            Err(e) => {
                error!("Failed to get follows: {}", e);
                return;
            }
        }

        // Populate new follows
        for tag in event.tags.iter() {
            if let Some(TagStandard::PublicKey { public_key, .. }) = tag.as_standardized() {
                follows_diff.entry(*public_key).or_default().new_updated_at =
                    Some(event.created_at);
            } else {
                debug!("Ignoring tag from kind 3: {:?}", tag);
            }
        }

        // Process follows_diff
        for (
            followee,
            FollowsDiff {
                old_updated_at,
                new_updated_at,
            },
        ) in follows_diff.iter()
        {
            match old_updated_at {
                Some(old_at) => match new_updated_at {
                    None => {
                        if let Err(e) = self.repo.delete_follow(*followee, follower, None).await {
                            error!("Failed to delete follow: {}", e);
                        }
                        if let Err(e) = self
                            .diff_result_tx
                            .send(FollowChange {
                                follow_type: FollowChangeType::Unfollowed(event.created_at),
                                followee: *followee,
                                follower,
                            })
                            .await
                        {
                            error!("Failed to send follow change: {}", e);
                        }
                    }
                    Some(new_at) if old_at < new_at => {
                        let follow = Follow {
                            followee: *followee,
                            follower,
                            updated_at: *new_at,
                            created_at: *new_at,
                        };

                        if let Err(e) = self.repo.upsert_follow(follow, None).await {
                            error!("Failed to upsert follow: {}", e);
                        }
                    }
                    _ => {}
                },
                None => {
                    if let Some(new_updated_at) = new_updated_at {
                        let follow = Follow {
                            followee: *followee,
                            follower,
                            updated_at: *new_updated_at,
                            created_at: *new_updated_at,
                        };

                        if let Err(e) = self.repo.upsert_follow(follow, None).await {
                            error!("Failed to upsert follow: {}", e);
                        }
                        if let Err(e) = self
                            .diff_result_tx
                            .send(FollowChange {
                                follow_type: FollowChangeType::Followed(event.created_at),
                                followee: *followee,
                                follower,
                            })
                            .await
                        {
                            error!("Failed to send follow change: {}", e);
                        }
                    }
                }
            }
        }
    }
}
