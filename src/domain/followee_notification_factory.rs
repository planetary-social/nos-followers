use super::{FollowChange, NotificationMessage, MAX_FOLLOWERS_PER_BATCH};
use crate::rate_limiter::RateLimiter;
use nostr_sdk::PublicKey;
use ordermap::OrderMap;
use std::fmt::Debug;
use std::time::Duration;
use tokio::time::Instant;

type Follower = PublicKey;
type Followee = PublicKey;

/// Accumulates messages for a followee and flushes them in batches
pub struct FolloweeNotificationFactory {
    pub follow_changes: OrderMap<Follower, Box<FollowChange>>,
    pub followee: Option<Followee>,
    min_time_between_messages: Duration,
    rate_limiter: RateLimiter,
    emptied_at: Option<Instant>,
}

impl FolloweeNotificationFactory {
    pub fn new(min_time_between_messages: Duration) -> Self {
        // Rate limiter for 1 message every 12 hours, bursts of 10
        let capacity = 10.0;
        let rate_limiter = RateLimiter::new(capacity, Duration::from_secs(12 * 60 * 60));

        Self {
            follow_changes: OrderMap::with_capacity(100),
            followee: None,
            min_time_between_messages,
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

        let min_time_elapsed = match self.emptied_at {
            Some(emptied_at) => now.duration_since(emptied_at) >= self.min_time_between_messages,
            None => true,
        };

        if !min_time_elapsed {
            return false;
        }

        let one_day_elapsed = match self.emptied_at {
            Some(emptied_at) => now.duration_since(emptied_at) >= Duration::from_secs(24 * 60 * 60),
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
        self.follow_changes.is_empty() && self.should_flush()
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
            let now = Instant::now();
            let one_day_elapsed = match self.emptied_at {
                Some(emptied_at) => {
                    now.duration_since(emptied_at) >= Duration::from_secs(24 * 60 * 60)
                }
                None => true,
            };

            self.emptied_at = Some(now);

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

            if one_day_elapsed {
                // Overcharge the rate limiter to consume tokens regardless of availability
                self.rate_limiter.overcharge(tokens_needed);
            } else {
                if !self.rate_limiter.consume(tokens_needed) {
                    return vec![];
                }
            }

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
