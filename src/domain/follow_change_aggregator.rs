use super::{FollowChangeBatch, MAX_FOLLOWERS_PER_MESSAGE};
use crate::domain::FollowChange;
use anyhow::Result;
use governor::clock::{DefaultClock, Reference};
use governor::{
    clock::Clock, middleware::NoOpMiddleware, nanos::Nanos, state::keyed::DefaultKeyedStateStore,
    Quota, RateLimiter,
};
use metrics::{counter, gauge, histogram};
use nostr_sdk::PublicKey;
use ordermap::OrderMap;
use std::num::NonZero;
use std::time::Duration;
use tracing::{debug, info};

type Follower = PublicKey;
type Followee = PublicKey;
type FollowChangeRateLimiter<T> = RateLimiter<
    PublicKey,
    DefaultKeyedStateStore<PublicKey>,
    T,
    NoOpMiddleware<<T as Clock>::Instant>,
>;

/// Aggregates `FollowChange` events by merging redundant follow/unfollow actions
/// for the same follower-followee pair, managing message batching, and compiling
/// the results into `FollowChangeBatch` instances per followee.
pub struct FollowChangeAggregator<T: Clock = DefaultClock> {
    followee_maps: OrderMap<Followee, OrderMap<Follower, (T::Instant, FollowChange)>>,
    rate_limiter: FollowChangeRateLimiter<T>,
    max_retention: Duration,
    clock: T,
}

impl<T: Clock> FollowChangeAggregator<T> {
    pub fn new(max_follows_per_hour: u32, max_retention_minutes: i64, clock: T) -> Result<Self> {
        let quota = Quota::per_hour(NonZero::new(max_follows_per_hour).ok_or(anyhow::anyhow!(
            "max_follows_per_hour must be greater than zero"
        ))?);

        let rate_limiter = RateLimiter::dashmap_with_clock(quota, &clock);

        Ok(Self {
            followee_maps: OrderMap::with_capacity(100_000),
            rate_limiter,
            max_retention: Duration::from_secs(max_retention_minutes as u64 * 60),
            clock,
        })
    }

    pub fn insert(&mut self, follow_change: FollowChange) {
        let follow_changes_map = self
            .followee_maps
            .entry(follow_change.followee)
            .or_default();

        if let Some((_, existing_change)) = follow_changes_map.get(&follow_change.follower) {
            // If the new follow_change is older, do nothing
            if follow_change.at < existing_change.at {
                return;
            }

            // If the new change is of a different type, remove the existing
            // entry, they cancel each other
            if follow_change.change_type != existing_change.change_type {
                follow_changes_map.remove(&follow_change.follower);
                return;
            }
        }

        follow_changes_map.insert(follow_change.follower, (self.clock.now(), follow_change));
    }

    /// Collects follow/unfollow changes per followee into FollowChangeBatch objects,
    /// which essentially map to push notifications. Rate-limited changes are retained
    /// for later so they can be included in a batch rather than sent individually and immediately.
    /// Follow changes retained for over an hour, or those that fit within the current
    /// batch, bypass the rate limit and are included immediately.
    pub fn drain_into_batches(&mut self) -> Vec<FollowChangeBatch> {
        let initial_follow_changes_len = self.follow_changes_len();
        let initial_followees_len = self.followees_len();

        let mut messages_map: OrderMap<PublicKey, Vec<FollowChangeBatch>> =
            OrderMap::with_capacity(self.followee_maps.len() / MAX_FOLLOWERS_PER_MESSAGE);

        self.followee_maps.retain(|_followee, follow_change_map| {
            let mut rate_limited_followee = false;

            // TODO: extract_if would have been great here, keep an eye on nightly
            let max_retention = &self.max_retention;
            let rate_limiter = &mut self.rate_limiter;
            follow_change_map.retain(|_follower, (inserted_at, follow_change)| {
                let (should_retain, is_followee_rate_limited) = collect_follow_change(
                    max_retention,
                    rate_limiter,
                    inserted_at,
                    &mut messages_map,
                    rate_limited_followee,
                    follow_change,
                    &self.clock,
                );

                rate_limited_followee = is_followee_rate_limited;

                should_retain
            });

            !follow_change_map.is_empty()
        });

        let messages = messages_map
            .into_values()
            .flatten()
            .filter(|m| !m.is_empty())
            .collect::<Vec<FollowChangeBatch>>();

        if messages.is_empty() {
            debug!(
                "Processed {} follow changes for {} followees, no messages created.",
                initial_follow_changes_len, initial_followees_len
            );
        } else {
            record_metrics(&messages, self.follow_changes_len());

            info!(
                "Processed {} follow changes for {} followees, retaining {} changes for {} followees. {} messages created, wrapping {} follow changes.",
                initial_follow_changes_len,
                initial_followees_len,
                self.follow_changes_len(),
                self.followees_len(),
                messages.len(),
                // We can calculate this indirectly through a substraction but for a
                // debug message it's better to be direct
                messages.iter().map(|batch| batch.len()).sum::<usize>(),
            );
        }

        messages
    }

    pub fn is_empty(&self) -> bool {
        self.followee_maps.is_empty()
    }

    pub fn follow_changes_len(&self) -> usize {
        self.followee_maps.values().map(|m| m.len()).sum()
    }

    pub fn followees_len(&self) -> usize {
        self.followee_maps.len()
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
///   MAX_FOLLOWERS_PER_MESSAGE changes. If it didn't we'd hit the APNS max
///   payload limit.
fn collect_follow_change<T: Clock>(
    max_retention: &Duration,
    rate_limiter: &mut FollowChangeRateLimiter<T>,
    inserted_at: &mut T::Instant,
    messages_map: &mut OrderMap<PublicKey, Vec<FollowChangeBatch>>,
    is_followee_rate_limited: bool,
    follow_change: &mut FollowChange,
    clock: &T,
) -> (bool, bool) {
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

    let current_message_has_room = latest_batch_for_followee.len() < MAX_FOLLOWERS_PER_MESSAGE;
    let rate_limited = is_followee_rate_limited
        || (!retained_for_too_long && rate_limiter.check_key(&followee).is_err());

    if !rate_limited || retained_for_too_long {
        let batch = if latest_batch_for_followee.is_empty()
            || (current_message_has_room && retained_for_too_long)
        {
            latest_batch_for_followee
        } else {
            followee_batches.push(FollowChangeBatch::new(followee));
            followee_batches
                .last_mut()
                .expect("New batch should be available")
        };

        batch.add(follow_change.clone());
        return (false, rate_limited);
    }

    (true, rate_limited)
}

fn record_metrics(messages: &[FollowChangeBatch], retained_follow_changes: usize) {
    let mut individual_follow_changes = 0;
    let mut aggregated_follow_changes = 0;
    let mut total_followers = 0;
    let mut total_unfollowers = 0;

    for message in messages {
        let message_len = message.len();

        if message_len == 1 {
            individual_follow_changes += 1;
        } else {
            aggregated_follow_changes += 1;
            total_followers += message.follows().len();
            total_unfollowers += message.unfollows().len();
        }
    }

    counter!("individual_follow_messages").increment(individual_follow_changes as u64);
    counter!("aggregated_follow_messages").increment(aggregated_follow_changes as u64);
    histogram!("followers_per_aggregated_message").record(total_followers as f64);
    histogram!("unfollowers_per_aggregated_message").record(total_unfollowers as f64);
    gauge!("retained_follow_changes").set(retained_follow_changes as f64);
}

#[cfg(test)]
mod tests {
    use super::*;
    use assertables::*;
    use chrono::{DateTime, Utc};
    use governor::clock::FakeRelativeClock;
    use nostr_sdk::prelude::Keys;
    use std::time::{Duration, UNIX_EPOCH};

    fn create_follow_change(
        follower: PublicKey,
        followee: PublicKey,
        at: DateTime<Utc>,
    ) -> FollowChange {
        FollowChange::new_followed(at, follower, followee)
    }

    fn create_unfollow_change(
        follower: PublicKey,
        followee: PublicKey,
        at: DateTime<Utc>,
    ) -> FollowChange {
        FollowChange::new_unfollowed(at, follower, followee)
    }

    #[test]
    fn test_insert_unique_follow_change() {
        let mut unique_changes =
            FollowChangeAggregator::new(10, 10, FakeRelativeClock::default()).unwrap();

        let follower = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        let change1 = create_follow_change(follower, followee, seconds_to_datetime(1));
        unique_changes.insert(change1);

        let change2 = create_follow_change(follower, followee, seconds_to_datetime(1));
        unique_changes.insert(change2.clone());

        // When they share the same time, the last change added should be kept
        let messages = unique_changes.drain_into_batches();
        assert_eq!(messages.len(), 1);
        let message = &messages[0];
        assert_message_eq(message, &followee, [follower], &[]);
    }

    #[test]
    fn test_does_not_replace_with_older_change() {
        let mut unique_changes =
            FollowChangeAggregator::new(10, 10, FakeRelativeClock::default()).unwrap();

        let follower = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        let newer_change = create_follow_change(follower, followee, seconds_to_datetime(2));
        unique_changes.insert(newer_change.clone());

        let older_change = create_unfollow_change(follower, followee, seconds_to_datetime(1));
        unique_changes.insert(older_change);

        let messages = unique_changes.drain_into_batches();
        assert_eq!(messages.len(), 1);
        assert_message_eq(&messages[0], &followee, [follower], &[]);
    }

    #[test]
    fn test_insert_same_follower_different_followee() {
        let mut unique_changes =
            FollowChangeAggregator::new(10, 10, FakeRelativeClock::default()).unwrap();

        let follower = Keys::generate().public_key();
        let followee1 = Keys::generate().public_key();
        let followee2 = Keys::generate().public_key();

        let change1 = create_follow_change(follower, followee1, seconds_to_datetime(2));
        let change2 = create_follow_change(follower, followee2, seconds_to_datetime(1));

        unique_changes.insert(change1.clone());
        unique_changes.insert(change2.clone());

        let messages = unique_changes.drain_into_batches();
        // Both changes should be kept since they have different followees
        assert_bag_eq!(
            messages,
            [
                FollowChangeBatch::from(change1.clone()),
                FollowChangeBatch::from(change2.clone())
            ]
        );
    }

    #[test]
    fn test_an_unfollow_cancels_a_follow() {
        let mut unique_changes =
            FollowChangeAggregator::new(10, 10, FakeRelativeClock::default()).unwrap();

        let follower = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        let follow_change = create_follow_change(follower, followee, seconds_to_datetime(1));
        let unfollow_change = create_unfollow_change(follower, followee, seconds_to_datetime(2));

        unique_changes.insert(follow_change.clone());
        unique_changes.insert(unfollow_change.clone());

        // The unfollow should cancel the follow
        assert_eq!(unique_changes.drain_into_batches(), []);
    }

    #[test]
    fn test_a_follow_cancels_an_unfollow() {
        let mut unique_changes =
            FollowChangeAggregator::new(10, 10, FakeRelativeClock::default()).unwrap();

        let follower = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        let unfollow_change = create_unfollow_change(follower, followee, seconds_to_datetime(1));
        let follow_change = create_follow_change(follower, followee, seconds_to_datetime(2));

        unique_changes.insert(unfollow_change.clone());
        unique_changes.insert(follow_change.clone());

        // The follow should cancel the unfollow
        assert_eq!(unique_changes.drain_into_batches(), []);
    }

    #[test]
    fn test_single_item_batch_before_rate_limit_is_hit() {
        let max_follows_per_hour = 10;
        let max_retention_minutes = 10;

        let mut aggregator = FollowChangeAggregator::new(
            max_follows_per_hour,
            max_retention_minutes,
            FakeRelativeClock::default(),
        )
        .unwrap();

        let follower1 = Keys::generate().public_key();
        let follower2 = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        let change1 = create_follow_change(follower1, followee, seconds_to_datetime(1));
        aggregator.insert(change1.clone());

        let change2 = create_follow_change(follower2, followee, seconds_to_datetime(1));
        aggregator.insert(change2.clone());

        let messages = aggregator.drain_into_batches();
        // Both changes are in separate batches for the same followee because we didn't hit a rate limit
        assert_batches_eq(&messages, &[(followee, &[change1]), (followee, &[change2])]);
    }

    #[test]
    fn test_no_message_after_rate_limit_is_hit_but_retention_not_elapsed() {
        // After one single follow change the rate limit will be hit
        let max_follows_per_hour = 1;
        let max_retention_minutes = 10;

        let clock = FakeRelativeClock::default();
        let mut aggregator =
            FollowChangeAggregator::new(max_follows_per_hour, max_retention_minutes, clock.clone())
                .unwrap();

        let follower1 = Keys::generate().public_key();
        let follower2 = Keys::generate().public_key();
        let follower3 = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        let change1 = create_follow_change(follower1, followee, seconds_to_datetime(1));
        aggregator.insert(change1.clone());

        let change2 = create_follow_change(follower2, followee, seconds_to_datetime(1));
        aggregator.insert(change2.clone());

        let change3 = create_follow_change(follower3, followee, seconds_to_datetime(1));
        aggregator.insert(change3.clone());

        // We passed the rate limit, but the retention time hasn't elapsed yet.
        // The rate is one follow per hour, so we only get one message, the
        // other one is retained.
        let messages = aggregator.drain_into_batches();
        assert_batches_eq(&messages, &[(followee, &[change1])]);

        // We hit the limit so the rest of the messages are retained
        let messages = aggregator.drain_into_batches();
        assert_batches_eq(&messages, &[]);

        // We pass the max retention time, the rest of the messages are packed
        // together as they are less than MAX_FOLLOWERS_PER_MESSAGE
        clock.advance(Duration::from_secs((max_retention_minutes as u64 + 1) * 60));

        let messages = aggregator.drain_into_batches();
        assert_batches_eq(&messages, &[(followee, &[change2, change3])]);
    }

    #[test]
    fn test_batch_sizes_after_rate_limit_and_retention_period() {
        let max_follows_per_hour = 1; // After one single follow change, the rate limit will be hit
        let max_retention_minutes = 10;
        const MAX_FOLLOWERS_TRIPLED: u64 = 3 * MAX_FOLLOWERS_PER_MESSAGE as u64; // The number of messages we will send for testing

        let clock = FakeRelativeClock::default();
        let mut aggregator =
            FollowChangeAggregator::new(max_follows_per_hour, max_retention_minutes, clock.clone())
                .unwrap();

        let followee = Keys::generate().public_key();

        let mut changes = Vec::new();
        for i in 0..MAX_FOLLOWERS_TRIPLED {
            let follower = Keys::generate().public_key();
            let change = create_follow_change(follower, followee, seconds_to_datetime(i));
            aggregator.insert(change.clone());
            changes.push(change);
        }

        // After inserting MAX_FOLLOWERS_TRIPLED changes, we hit the rate limit immediately after the first message.
        // The first message will be sent immediately, while the rest should be retained.
        let messages = aggregator.drain_into_batches();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].len(), 1);

        // All other messages are retained due to rate limiting
        let messages = aggregator.drain_into_batches();
        assert_eq!(messages.len(), 0);

        // Just before the max_retention time elapses..
        clock.advance(Duration::from_secs((max_retention_minutes as u64 - 1) * 60));

        // .. we insert another change
        let follower = Keys::generate().public_key();
        let change = create_follow_change(
            follower,
            followee,
            seconds_to_datetime(MAX_FOLLOWERS_TRIPLED + 1),
        );
        aggregator.insert(change.clone());

        // After the max retention time elapses, all retained changes should be sent, in batches.
        clock.advance(Duration::from_secs(2 * 60));

        let messages = aggregator.drain_into_batches();

        assert_eq!(messages.len(), 3);
        // First couple should contain MAX_FOLLOWERS_PER_MESSAGE changes
        assert_eq!(messages[0].len(), MAX_FOLLOWERS_PER_MESSAGE);
        assert_eq!(messages[1].len(), MAX_FOLLOWERS_PER_MESSAGE);
        assert_eq!(
            messages[2].len() as u64,
            MAX_FOLLOWERS_TRIPLED - 2u64 * MAX_FOLLOWERS_PER_MESSAGE as u64 - 1u64
        ); // Thirds batch should contain the remaining changes

        // And another change arrives
        let follower = Keys::generate().public_key();
        let change = create_follow_change(
            follower,
            followee,
            seconds_to_datetime(MAX_FOLLOWERS_TRIPLED),
        );
        aggregator.insert(change.clone());

        let messages = aggregator.drain_into_batches();

        // Nothing is sent, the user just receive the messages so this one gets into next batch
        assert_eq!(messages.len(), 0);

        // The max retention time elapses again, all retained changes should be sent, in batches.
        clock.advance(Duration::from_secs((max_retention_minutes as u64 + 1) * 60));

        let messages = aggregator.drain_into_batches();

        // This one has a single item
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].len(), 2);
    }

    #[test]
    fn test_is_empty_and_len() {
        let mut unique_changes =
            FollowChangeAggregator::new(10, 10, FakeRelativeClock::default()).unwrap();

        let follower1 = Keys::generate().public_key();
        let follower2 = Keys::generate().public_key();
        let followee1 = Keys::generate().public_key();
        let followee2 = Keys::generate().public_key();

        assert!(unique_changes.is_empty());
        assert_eq!(unique_changes.follow_changes_len(), 0);
        assert_eq!(unique_changes.followees_len(), 0);

        let change1 = create_follow_change(follower1, followee1, seconds_to_datetime(1));
        let change2 = create_follow_change(follower1, followee2, seconds_to_datetime(1));
        let change3 = create_follow_change(follower2, followee2, seconds_to_datetime(1));

        unique_changes.insert(change1);
        unique_changes.insert(change2);
        unique_changes.insert(change3);

        assert!(!unique_changes.is_empty());
        assert_eq!(unique_changes.follow_changes_len(), 3);
        assert_eq!(unique_changes.followees_len(), 2);
    }

    #[test]
    fn test_drain_clears_map() {
        let mut unique_changes =
            FollowChangeAggregator::new(10, 10, FakeRelativeClock::default()).unwrap();

        let follower = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        let change1 = create_follow_change(follower, followee, seconds_to_datetime(2));
        unique_changes.insert(change1);

        let change2 = create_follow_change(follower, followee, seconds_to_datetime(1));
        unique_changes.insert(change2);

        let changes = unique_changes.drain_into_batches();
        assert_eq!(changes.len(), 1);
        assert_message_eq(&changes[0], &followee, [follower], &[])
    }

    fn assert_message_eq(
        message: &FollowChangeBatch,
        followee: &PublicKey,
        follows: impl AsRef<[PublicKey]>,
        unfollows: impl AsRef<[PublicKey]>,
    ) {
        assert_eq!(message.followee(), followee);

        let follows_vec: Vec<PublicKey> = message.follows().iter().cloned().collect();
        let unfollows_vec: Vec<PublicKey> = message.unfollows().iter().cloned().collect();

        assert_bag_eq!(follows_vec, follows.as_ref());
        assert_bag_eq!(unfollows_vec, unfollows.as_ref());
    }

    fn seconds_to_datetime(seconds: u64) -> DateTime<Utc> {
        DateTime::<Utc>::from(UNIX_EPOCH + Duration::from_secs(seconds))
    }

    fn assert_batches_eq(actual: &[FollowChangeBatch], expected: &[(PublicKey, &[FollowChange])]) {
        let mut expected_batches = Vec::new();

        for (followee, changes) in expected {
            let mut batch = FollowChangeBatch::new(*followee);
            for change in *changes {
                batch.add(change.clone());
            }
            expected_batches.push(batch);
        }

        assert_bag_eq!(actual, expected_batches);
    }
}
