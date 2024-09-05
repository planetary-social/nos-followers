use super::FollowChange;
use crate::rate_counter::RateCounter;
use governor::{clock::Clock, clock::Reference};
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
        self.follow_changes
            .retain(|_follower, (inserted_at, follow_change)| {
                collect_follow_change(
                    max_retention,
                    inserted_at,
                    follow_changes_to_publish,
                    follow_change,
                    &self.clock,
                    &mut self.rate_counter,
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

fn collect_follow_change<T: Clock>(
    max_retention: &Duration,
    inserted_at: &mut T::Instant,
    follow_changes_to_publish: &mut Vec<CollectedFollowChange>,
    follow_change: &mut FollowChange,
    clock: &T,
    rate_counter: &mut RateCounter<T>,
) -> bool {
    let retained_for_too_long = clock.now().duration_since(*inserted_at) > (*max_retention).into();

    if retained_for_too_long {
        send_batchable(follow_change, follow_changes_to_publish, rate_counter);
        return false;
    }

    let rate_limited = rate_counter.is_hit();
    if rate_limited {
        return true;
    }

    send_single(follow_change, follow_changes_to_publish, rate_counter);
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
