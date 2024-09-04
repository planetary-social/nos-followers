use super::FollowChange;
use crate::rate_counter::RateCounter;
use governor::{clock::Clock, clock::Reference, nanos::Nanos};
use nostr_sdk::PublicKey;
use ordermap::OrderMap;
use std::fmt::Debug;
use std::time::Duration;

type Follower = PublicKey;

pub const ONE_HOUR: Duration = Duration::from_secs(60 * 60);

pub enum CollectedFollowChange {
    Single(FollowChange),
    Batchable(FollowChange),
}

pub struct FolloweeNotificationFactory<T: Clock> {
    pub rate_counter: RateCounter<T>,
    pub follow_changes: OrderMap<Follower, (T::Instant, FollowChange)>,
    clock: T,
}

impl<T: Clock> FolloweeNotificationFactory<T> {
    pub fn new(max_messages_per_hour: u32, clock: T) -> Self {
        let rate_counter = RateCounter::new(max_messages_per_hour, ONE_HOUR, clock.clone());

        Self {
            rate_counter,
            follow_changes: OrderMap::with_capacity(100),
            clock,
        }
    }

    pub fn add_follower_change(&mut self, follow_change: FollowChange) {
        let follower = follow_change.follower;

        if let Some((_, existing_change)) = self.follow_changes.get(&follower) {
            if follow_change.at < existing_change.at {
                return;
            }

            if follow_change.change_type != existing_change.change_type {
                self.follow_changes.remove(&follower);
                return;
            }
        }

        self.follow_changes
            .insert(follower, (self.clock.now(), follow_change));
    }

    pub fn is_deletable(&mut self) -> bool {
        // We want to retain the followee info even if there are no
        // changes so we remember the rate limit for one more period in
        // case new changes arrive
        self.follow_changes.is_empty() && !self.rate_counter.is_hit()
    }

    pub fn drain_into_messages(
        &mut self,
        max_retention: &Duration,
        follow_changes_to_publish: &mut Vec<CollectedFollowChange>,
    ) {
        // TODO: extract_if would have been great here, keep an eye on nightly
        let rate_counter = &mut self.rate_counter;
        let follow_change_map = &mut self.follow_changes;

        follow_change_map.retain(|_follower, (inserted_at, follow_change)| {
            collect_follow_change(
                max_retention,
                inserted_at,
                follow_changes_to_publish,
                follow_change,
                &self.clock,
                rate_counter,
            )
        });
    }
}

impl<T: Clock> Debug for FolloweeNotificationFactory<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FolloweeNotificationFactory")
            .field("follow_changes", &self.follow_changes)
            .finish()
    }
}

/// Collects a follow change into a batched message and returns whether the change should
/// be retained for later due to rate limits.
///
/// - If the messages sent so far have been rate-limited, the change will be
///   retained for later processing but only within the max retention period.
///
/// - Once the retention period is elapsed, the retained changes are sent in batched messages.
///   Messages with only one item will include friendly ID information, the
///   notification service will show them as "foobar@nos.social is a new
///   follower!"
///   Messages with multiple items will be shown as "You have 29 new followers and 29 unfollows!"
///
/// - The batching process ensures that no message contains more than
///   MAX_FOLLOWERS_PER_BATCH changes. If it didn't we'd hit the APNS max
///   payload limit.
fn collect_follow_change<T: Clock>(
    max_retention: &Duration,
    inserted_at: &mut T::Instant,
    follow_changes_to_publish: &mut Vec<CollectedFollowChange>,
    follow_change: &mut FollowChange,
    clock: &T,
    rate_counter: &mut RateCounter<T>,
) -> bool {
    let retained_for_too_long =
        clock.now().duration_since(*inserted_at) > Nanos::new(max_retention.as_nanos() as u64);

    let rate_limited = rate_counter.is_hit();
    if !rate_limited && !retained_for_too_long {
        send_single(follow_change, follow_changes_to_publish, rate_counter);
        return false;
    }

    if rate_limited && !retained_for_too_long {
        return true;
    }

    // If we reached this point it means that the batch is full or the retention time has elapsed
    send_batchable(follow_change, follow_changes_to_publish, rate_counter);

    rate_counter.bump();
    false
}

fn send_batchable<T: Clock>(
    follow_change: &FollowChange,
    follow_changes_to_publish: &mut Vec<CollectedFollowChange>,
    rate_counter: &mut RateCounter<T>,
) {
    follow_changes_to_publish.push(CollectedFollowChange::Batchable(follow_change.clone()));
    rate_counter.bump();
}

fn send_single<T: Clock>(
    follow_change: &FollowChange,
    follow_changes_to_publish: &mut Vec<CollectedFollowChange>,
    rate_counter: &mut RateCounter<T>,
) {
    follow_changes_to_publish.push(CollectedFollowChange::Single(follow_change.clone()));
    rate_counter.bump();
}
