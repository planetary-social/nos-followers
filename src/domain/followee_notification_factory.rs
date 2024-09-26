use super::{FollowChange, NotificationMessage, MAX_FOLLOWERS_PER_BATCH};
use crate::rate_limiter::RateLimiter;
use nostr_sdk::PublicKey;
use ordermap::OrderMap;
use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::time::Duration;
use tokio::time::Instant;
use tracing::info;

type Follower = PublicKey;
type Followee = PublicKey;

static ONE_DAY: Duration = Duration::from_secs(24 * 60 * 60);
/// Accumulates messages for a followee and flushes them in batches
pub struct FolloweeNotificationFactory {
    pub follow_changes: OrderMap<Follower, Box<FollowChange>>,
    pub followee: Option<Followee>,
    rate_limiter: RateLimiter,
    emptied_at: Option<Instant>,
}

impl FolloweeNotificationFactory {
    pub fn new(capacity: u16, min_seconds_between_messages: NonZeroUsize) -> Self {
        // Rate limiter for 1 message every `min_seconds_between_messages`, with a
        // burst of `capacity`.
        let min_time_between_messages =
            Duration::from_secs(min_seconds_between_messages.get() as u64);
        let rate_limiter = RateLimiter::new(capacity as f64, min_time_between_messages);

        Self {
            follow_changes: OrderMap::with_capacity(100),
            followee: None,
            rate_limiter,
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

    // Flushes if minimum time between messages has elapsed and rate limit is not exceeded.
    // If a day has elapsed since the last flush, it will flush regardless of the rate limit.
    pub fn should_flush(&mut self) -> bool {
        let now = Instant::now();

        let one_day_elapsed = match self.emptied_at {
            Some(emptied_at) => now.duration_since(emptied_at) >= ONE_DAY,
            None => true,
        };

        if one_day_elapsed {
            return true;
        }

        // Check if tokens are available without consuming them
        self.rate_limiter.can_consume(1.0)
    }

    pub fn followers_len(&self) -> usize {
        self.follow_changes
            .iter()
            .filter(|(_, v)| v.is_follower())
            .count()
    }

    pub fn should_delete(&mut self) -> bool {
        // If it has been empty for a day, it's ok to delete
        self.follow_changes.is_empty()
            && self.emptied_at.map_or(true, |emptied_at| {
                Instant::now().duration_since(emptied_at) >= ONE_DAY
            })
    }

    pub fn no_followers(&self) -> bool {
        !self.follow_changes.iter().any(|(_, v)| v.is_follower())
    }

    // Only followers are accumulated into messages, unfollowers are not, but
    // all of them are drained
    pub fn flush(&mut self) -> Vec<NotificationMessage> {
        if self.no_followers() {
            return vec![];
        }

        if self.should_flush() {
            self.emptied_at = Some(Instant::now());

            let followers = self
                .follow_changes
                .drain(..)
                .map(|(_, v)| v)
                .filter(|v| v.is_follower())
                .collect::<Vec<Box<FollowChange>>>();

            let messages: Vec<NotificationMessage> = followers
                .chunks(MAX_FOLLOWERS_PER_BATCH)
                .map(|batch| batch.to_vec().into())
                .collect();

            let tokens_needed = messages.len() as f64;

            // Just to sample the rate limiter
            if tokens_needed > 1.0 {
                info!(
                    "Rate limiter for followee {} after flush: {}",
                    self.followee.unwrap(),
                    self.rate_limiter
                );
            }

            self.rate_limiter.overcharge(tokens_needed);

            return messages;
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
