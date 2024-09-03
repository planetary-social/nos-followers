use super::{FollowChangeBatch, MAX_FOLLOWERS_PER_BATCH};
use crate::domain::FollowChange;
use crate::rate_counter::RateCounter;
use governor::{clock::Clock, clock::Reference, nanos::Nanos};
use nostr_sdk::PublicKey;
use ordermap::OrderMap;
use std::time::Duration;

type Follower = PublicKey;

const ONE_HOUR: Duration = Duration::from_secs(60 * 60);

pub struct FolloweeAggregator<T: Clock> {
    rate_counter: RateCounter<T>,
    pub follow_changes: OrderMap<Follower, (T::Instant, FollowChange)>,
    clock: T,
}

impl<T: Clock> FolloweeAggregator<T> {
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
            // If the new follow_change is older, do nothing
            if follow_change.at < existing_change.at {
                return;
            }

            // If the new change is of a different type, remove the existing
            // entry, they cancel each other
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
        !self.follow_changes.is_empty() || self.rate_counter.is_hit()
    }

    pub fn drain_into_batches(
        &mut self,
        max_retention: &Duration,
        messages_map: &mut OrderMap<PublicKey, Vec<FollowChangeBatch>>,
    ) {
        // TODO: extract_if would have been great here, keep an eye on nightly
        let rate_counter = &mut self.rate_counter;
        let follow_change_map = &mut self.follow_changes;

        follow_change_map.retain(|_follower, (inserted_at, follow_change)| {
            collect_follow_change(
                max_retention,
                inserted_at,
                messages_map,
                follow_change,
                &self.clock,
                rate_counter,
            )
        });
    }
}

/// Collects a follow change into a batch and returns whether the change should
/// be retained for later due to rate limits.
///
/// - If the batches sent so far have been rate-limited, the change will be
///   retained for later processing but only within the max retention period.
///
/// - Once the retention period is elapsed, the retained changes are sent in batches.
///   Batches with only one item will include friendly ID information, the
///   notification service will show them as "foobar@nos.social is a new
///   follower!"
///   Batches with multiple items will be shown as "You have 29 new followers and 29 unfollows!"
///
/// - The batching process ensures that no batch contains more than
///   MAX_FOLLOWERS_PER_BATCH changes. If it didn't we'd hit the APNS max
///   payload limit.
fn collect_follow_change<T: Clock>(
    max_retention: &Duration,
    inserted_at: &mut T::Instant,
    messages_map: &mut OrderMap<PublicKey, Vec<FollowChangeBatch>>,
    follow_change: &mut FollowChange,
    clock: &T,
    rate_counter: &mut RateCounter<T>,
) -> bool {
    let followee = follow_change.followee;
    //let retained_for_too_long = inserted_at.elapsed() > *max_retention;
    let retained_for_too_long =
        clock.now().duration_since(*inserted_at) > Nanos::new(max_retention.as_nanos() as u64);
    let followee_batches = messages_map
        .entry(followee)
        .or_insert_with_key(|followee| vec![FollowChangeBatch::new(*followee)]);

    let latest_batch_for_followee = followee_batches
        .last_mut()
        .expect("Expected a non-empty batch for the followee");

    let used_batch_has_room = !latest_batch_for_followee.is_empty()
        && latest_batch_for_followee.len() < MAX_FOLLOWERS_PER_BATCH;

    let rate_limited = rate_counter.is_hit();
    if !rate_limited && !retained_for_too_long {
        add_to_new_batch(followee, follow_change.clone(), followee_batches);
        rate_counter.bump();
        return false;
    }

    if used_batch_has_room {
        latest_batch_for_followee.add(follow_change.clone());
        return false;
    }

    if rate_limited && !retained_for_too_long {
        return true;
    }

    // If we reached this point it means that the batch is full or the retention time has elapsed
    add_to_new_batch(followee, follow_change.clone(), followee_batches);
    rate_counter.bump();
    false
}

fn add_to_new_batch(
    followee: PublicKey,
    follow_change: FollowChange,
    followee_batches: &mut Vec<FollowChangeBatch>,
) {
    let mut batch = FollowChangeBatch::new(followee);
    batch.add(follow_change);
    followee_batches.push(batch);
}
