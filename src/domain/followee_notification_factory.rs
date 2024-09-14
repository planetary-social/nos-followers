use super::{FollowChange, NotificationMessage, MAX_FOLLOWERS_PER_BATCH};
use nostr_sdk::PublicKey;
use ordermap::OrderMap;
use std::fmt::Debug;
use std::time::Duration;
use tokio::time::Instant;

type Follower = PublicKey;
type Followee = PublicKey;

pub struct FolloweeNotificationFactory {
    pub follow_changes: OrderMap<Follower, Box<FollowChange>>,
    pub followee: Option<Followee>,
    min_time_between_messages: Duration,
    emptied_at: Option<Instant>,
}

impl FolloweeNotificationFactory {
    pub fn new(min_time_between_messages: Duration) -> Self {
        Self {
            follow_changes: OrderMap::with_capacity(100),
            followee: None,
            min_time_between_messages,
            emptied_at: None,
        }
    }

    pub fn insert(&mut self, follow_change: Box<FollowChange>) {
        match &self.followee {
            Some(followee) => {
                assert_eq!(
                    followee,
                    follow_change.followee(),
                    "Followee mismatch in add_follower_change"
                );
            }
            None => {
                self.followee = Some(*follow_change.followee());
            }
        }

        let follower = follow_change.follower();

        if let Some(existing_change) = self.follow_changes.get(follower) {
            if !existing_change.is_older_than(&follow_change) {
                return;
            }

            if existing_change.is_reverse_of(&follow_change) {
                self.follow_changes.remove(follower);
                return;
            }
        }

        self.follow_changes.insert(*follower, follow_change);
    }

    // This is basically a sliding window log rate limiter
    // No flushes if the time since the last flush is less than min_time_between_messages
    pub fn should_flush(&self) -> bool {
        match self.emptied_at {
            Some(emptied_at) => {
                let now = Instant::now();
                assert!(emptied_at <= now);
                now.duration_since(emptied_at) > self.min_time_between_messages
            }
            None => true,
        }
    }

    pub fn should_delete(&self) -> bool {
        self.follow_changes.is_empty() && self.should_flush()
    }

    pub fn no_notifiables(&self) -> bool {
        !self.follow_changes.iter().any(|(_, v)| v.is_notifiable())
    }

    // Only followers are accumulated into messages, unfollowers are not, but
    // all of them are drained
    pub fn flush(&mut self) -> Vec<NotificationMessage> {
        if self.no_notifiables() {
            return vec![];
        }

        if self.should_flush() {
            self.emptied_at = Some(Instant::now());

            return self
                .follow_changes
                .drain(..)
                .map(|(_, v)| v)
                .filter(|v| v.is_notifiable())
                .collect::<Vec<Box<FollowChange>>>()
                .chunks(MAX_FOLLOWERS_PER_BATCH)
                .map(|batch| batch.to_vec().into())
                .collect();
        }

        vec![]
    }
}

impl Debug for FolloweeNotificationFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FolloweeNotificationFactory")
            .field("follow_changes", &self.follow_changes)
            .finish()
    }
}
