use super::{FollowChangeBatch, MAX_FOLLOWERS_PER_BATCH};
use crate::domain::{FollowChange, FolloweeAggregator};
use anyhow::Result;
use governor::clock::Clock;
use governor::clock::DefaultClock;
use metrics::{counter, gauge, histogram};
use nostr_sdk::PublicKey;
use ordermap::OrderMap;
use std::time::Duration;
use tracing::{debug, info};

type Followee = PublicKey;

/// Aggregates `FollowChange` events by merging redundant follow/unfollow actions
/// for the same follower-followee pair, managing message batching, and compiling
/// the results into `FollowChangeBatch` instances per followee.
pub struct FollowChangeAggregator<T: Clock = DefaultClock> {
    followee_maps: OrderMap<Followee, FolloweeAggregator<T>>,
    max_retention: Duration,
    max_messages_per_hour: u32,
    clock: T,
}

impl<T: Clock> FollowChangeAggregator<T> {
    pub fn new(max_messages_per_hour: u32, max_retention_minutes: i64, clock: T) -> Result<Self> {
        Ok(Self {
            followee_maps: OrderMap::with_capacity(100_000),
            max_retention: Duration::from_secs(max_retention_minutes as u64 * 60),
            max_messages_per_hour,
            clock,
        })
    }

    pub fn insert(&mut self, follow_change: FollowChange) {
        let followee_info = self
            .followee_maps
            .entry(follow_change.followee)
            .or_insert_with_key(|_| {
                FolloweeAggregator::new(self.max_messages_per_hour, self.clock.clone())
            });

        followee_info.add_follower_change(follow_change)
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
            OrderMap::with_capacity(self.followee_maps.len() / MAX_FOLLOWERS_PER_BATCH);

        self.followee_maps.retain(|_followee, followee_changes| {
            followee_changes.drain_into_batches(&self.max_retention, &mut messages_map);
            followee_changes.is_deletable()
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
        self.followee_maps
            .values()
            .map(|m| m.follow_changes.len())
            .sum()
    }

    pub fn followees_len(&self) -> usize {
        self.followee_maps.len()
    }
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
        let mut aggregator =
            FollowChangeAggregator::new(10, 10, FakeRelativeClock::default()).unwrap();

        let follower = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        let change1 = create_follow_change(follower, followee, seconds_to_datetime(1));
        aggregator.insert(change1);

        let change2 = create_follow_change(follower, followee, seconds_to_datetime(1));
        aggregator.insert(change2.clone());

        // When they share the same time, the last change added should be kept
        let messages = aggregator.drain_into_batches();
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
        let max_messages_per_hour = 1;
        let max_retention_minutes = 10;

        let clock = FakeRelativeClock::default();
        let mut aggregator = FollowChangeAggregator::new(
            max_messages_per_hour,
            max_retention_minutes,
            clock.clone(),
        )
        .unwrap();

        let follower1 = Keys::generate().public_key();
        let follower2 = Keys::generate().public_key();
        let follower3 = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        let change1 = create_follow_change(follower1, followee, seconds_to_datetime(1));
        aggregator.insert(change1.clone());

        // We hit the rate limit, but the retention time hasn't elapsed yet.
        // The rate is one follow per hour, so we only get one message, the
        // other one is retained.
        let messages = aggregator.drain_into_batches();
        assert_batches_eq(&messages, &[(followee, &[change1])]);

        let change2 = create_follow_change(follower2, followee, seconds_to_datetime(1));
        aggregator.insert(change2.clone());

        let change3 = create_follow_change(follower3, followee, seconds_to_datetime(1));
        aggregator.insert(change3.clone());

        // We hit the limit so the rest of the messages are retained
        let messages = aggregator.drain_into_batches();
        assert_batches_eq(&messages, &[]);
        assert_eq!(aggregator.follow_changes_len(), 2);

        // We pass the max retention time, but we still are under the rate limit so we get nothing
        clock.advance(Duration::from_secs((max_retention_minutes as u64) * 60));
        let messages = aggregator.drain_into_batches();
        assert_batches_eq(&messages, &[]);

        // We clear the rate limit
        clock.advance(Duration::from_secs(50 * 60));
        let messages = aggregator.drain_into_batches();
        assert_batches_eq(&messages, &[(followee, &[change2, change3])]);
    }

    #[test]
    fn test_batch_sizes_after_rate_limit_and_retention_period() {
        let max_messages_per_hour = 1; // After one single follow change, the rate limit will be hit
        let max_retention_minutes = 10;
        const MAX_FOLLOWERS_TRIPLED: usize = 3 * MAX_FOLLOWERS_PER_BATCH as usize; // The number of messages we will send for testing

        let clock = FakeRelativeClock::default();
        let mut aggregator = FollowChangeAggregator::new(
            max_messages_per_hour,
            max_retention_minutes,
            clock.clone(),
        )
        .unwrap();

        let followee = Keys::generate().public_key();

        let mut changes = Vec::new();
        for i in 0..MAX_FOLLOWERS_TRIPLED {
            let follower = Keys::generate().public_key();
            let change = create_follow_change(follower, followee, seconds_to_datetime(i));
            aggregator.insert(change.clone());

            clock.advance(Duration::from_secs(1));
            changes.push(change);
        }

        // After inserting MAX_FOLLOWERS_TRIPLED changes, we hit the rate limit immediately after the first message.
        // The first message will be sent immediately, while the rest should be retained.
        let messages = aggregator.drain_into_batches();
        assert_eq!(
            messages.len(),
            1,
            "Expected a single message, got {:?}, changes: {:?}",
            messages,
            messages.iter().map(|m| m.len()).sum::<usize>()
        );
        assert_eq!(messages[0].len(), MAX_FOLLOWERS_PER_BATCH);

        // All other messages are retained due to rate limiting
        let messages = aggregator.drain_into_batches();
        assert_eq!(messages.len(), 0);
        assert_eq!(aggregator.follow_changes_len(), 2 * MAX_FOLLOWERS_PER_BATCH,);

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

        assert_eq!(
            aggregator.follow_changes_len(),
            2 * MAX_FOLLOWERS_PER_BATCH + 1
        );
        // After the max retention time elapses, all retained changes should be sent, in batches.
        clock.advance(Duration::from_secs(2 * 60));

        let messages = aggregator.drain_into_batches();

        assert_eq!(messages.len(), 2);
        // First couple should contain MAX_FOLLOWERS_BATCH changes, they surpassed the maximum retention time, so they are sent regardless of being rate limited
        assert_eq!(messages[0].len(), MAX_FOLLOWERS_PER_BATCH);
        assert_eq!(messages[1].len(), MAX_FOLLOWERS_PER_BATCH);

        // The last change added is not sent, is too new and we already sent the maximum amount of messages for the period
        assert_eq!(aggregator.follow_changes_len(), 1);

        // And another change arrives
        let follower = Keys::generate().public_key();
        let change = create_follow_change(
            follower,
            followee,
            seconds_to_datetime(MAX_FOLLOWERS_TRIPLED),
        );
        aggregator.insert(change.clone());

        // And another one for a different followee
        let followee2 = Keys::generate().public_key();
        let follower = Keys::generate().public_key();
        let change = create_follow_change(
            follower,
            followee2,
            seconds_to_datetime(MAX_FOLLOWERS_TRIPLED),
        );
        aggregator.insert(change.clone());

        let messages = aggregator.drain_into_batches();

        // Only the one for the new followee is sent as it's not rate limited, the rest already hit the limit.
        assert_eq!(messages.len(), 1);
        assert_eq!(aggregator.follow_changes_len(), 2);

        // The max retention time elapses again, all retained changes should be sent, in batches.
        clock.advance(Duration::from_secs((max_retention_minutes as u64 + 1) * 60));

        let messages = aggregator.drain_into_batches();

        // This one has a single item
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].len(), 2);
        assert_eq!(aggregator.follow_changes_len(), 0);
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

    fn seconds_to_datetime(seconds: usize) -> DateTime<Utc> {
        DateTime::<Utc>::from(UNIX_EPOCH + Duration::from_secs(seconds as u64))
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
