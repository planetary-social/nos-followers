use super::followee_notification_factory::SendableFollowChange;
use super::{NotificationMessage, MAX_FOLLOWERS_PER_BATCH};
use crate::domain::{FollowChange, FolloweeNotificationFactory};
use crate::metrics;
use anyhow::Result;
use governor::clock::Clock;
use governor::clock::DefaultClock;
use nostr_sdk::PublicKey;
use ordermap::OrderMap;
use std::collections::HashSet;
use std::time::Duration;
use tracing::info;

type Followee = PublicKey;

/// Aggregates `FollowChange` events by merging redundant follow/unfollow actions
/// for the same follower-followee pair, managing message batching, and compiling
/// the results into `NotificationMessage` instances per followee.
pub struct NotificationFactory<T: Clock = DefaultClock> {
    followee_maps: OrderMap<Followee, FolloweeNotificationFactory<T>>,
    max_retention: Duration,
    max_messages_per_hour: usize,
    clock: T,
}

impl<T: Clock> NotificationFactory<T> {
    pub fn new(max_messages_per_hour: usize, max_retention_minutes: i64, clock: T) -> Result<Self> {
        assert!(max_messages_per_hour > 0);
        info!(
            "After {} messages per hour, additional messages will be retained for {} minutes.",
            max_messages_per_hour, max_retention_minutes
        );

        Ok(Self {
            followee_maps: OrderMap::with_capacity(1_000),
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
                FolloweeNotificationFactory::new(
                    self.max_messages_per_hour,
                    self.max_retention,
                    self.clock.clone(),
                )
            });

        followee_info.add_follower_change(follow_change)
    }

    #[cfg(test)]
    pub fn expired_retains_count(&self) -> usize {
        self.followee_maps
            .values()
            .map(|followee_info| followee_info.expired_retains_count())
            .sum()
    }

    /// Collects follow/unfollow changes per followee into NotificationMessage
    /// objects, which essentially map to push notifications. Rate-limited
    /// changes are retained for later so they can be included in a batch rather
    /// than sent individually and immediately.  Those retained for
    /// over an hour, or those that fit within the current batch, bypass the
    /// rate limit and are included immediately. When there are not rate limits
    /// and no retained messages, notifications are not batched.
    pub fn drain_into_messages(&mut self) -> Vec<NotificationMessage> {
        let initial_follow_changes_len = self.follow_changes_len();
        let initial_followees_len = self.followees_len();

        let prepared_follow_changes = self.drain_sendable_follow_changes();

        let (mut total_changes, mut at_least_one_batch, mut at_least_one_single) =
            (0, false, false);
        for (_, changes) in &prepared_follow_changes {
            total_changes += changes.len();
            at_least_one_batch |= changes.iter().any(|change| change.is_batchable());
            at_least_one_single |= changes.iter().any(|change| !change.is_batchable());
        }

        let messages = construct_messages(prepared_follow_changes);

        // Call follow_changes_len() once and reuse in both places
        let follow_changes_len = self.follow_changes_len();
        record_metrics(&messages, follow_changes_len);
        self.log_report(&messages, initial_follow_changes_len, initial_followees_len);

        // Check if a mix of batchable and single messages were sent
        if at_least_one_batch && at_least_one_single {
            assert!(
            messages.iter().any(|m| m.len() > 1),
            "A mix of batchable and single messages should send at least one message with more than one item, none was found"
        );
        }

        // Ensure all changes were sent
        let total_sent = messages.iter().map(|m| m.len()).sum::<usize>();
        assert!(
            total_changes == total_sent,
            "{} changes were processed, but only {} were sent",
            total_changes,
            total_sent
        );

        messages
    }

    fn drain_sendable_follow_changes(
        &mut self,
    ) -> OrderMap<PublicKey, Vec<SendableFollowChange<T>>> {
        let mut sendable_follow_changes: OrderMap<PublicKey, Vec<SendableFollowChange<T>>> =
            OrderMap::with_capacity(self.followee_maps.len() / MAX_FOLLOWERS_PER_BATCH);

        self.followee_maps.retain(|followee, followee_changes| {
            let sendables_for_followee = sendable_follow_changes.entry(*followee).or_default();
            followee_changes.drain_into_sendables(sendables_for_followee);
            !followee_changes.is_deletable()
        });
        sendable_follow_changes
    }

    pub fn log_report(
        &self,
        messages: &[NotificationMessage],
        initial_follow_changes_len: usize,
        initial_followees_len: usize,
    ) {
        info!(
            "Processed a total of {} follow changes for {} followees.",
            initial_follow_changes_len, initial_followees_len
        );

        info!(
            "    Retained {} follow changes for {} followees.",
            self.follow_changes_len(),
            self.followees_len()
        );

        info!(
            "    Created {} notification messages for {} followees, wrapping a total of {} follow changes.",
            messages.len(),
            messages.iter().map(|m| m.followee()).collect::<HashSet<&PublicKey>>().len(),
            messages.iter().map(|batch| batch.len()).sum::<usize>()
        );

        info!(
            "    {} singles and {} batched follow changes were sent.",
            messages.iter().filter(|m| m.is_single()).count(),
            messages.iter().filter(|m| !m.is_single()).count()
        );
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

fn construct_messages<T: Clock>(
    sendable_follow_changes: OrderMap<PublicKey, Vec<SendableFollowChange<T>>>,
) -> Vec<NotificationMessage> {
    let mut messages = Vec::new();
    for (followee, follow_changes) in sendable_follow_changes.into_iter() {
        let mut followee_messages: Vec<NotificationMessage> = Vec::new();
        let mut singles = Vec::new();
        let mut batchables = Vec::new();

        for collected_change in follow_changes {
            match collected_change {
                SendableFollowChange::Single(change) => {
                    assert_eq!(followee, change.follow_change.followee);
                    singles.push(change);
                }
                SendableFollowChange::Batchable(change) => {
                    assert_eq!(followee, change.follow_change.followee);
                    batchables.push(change);
                }
            }
        }

        if batchables.is_empty() {
            let singles_len = singles.len();
            for change in singles {
                followee_messages.push(change.follow_change.into());
            }

            assert!(followee_messages.len() == singles_len);
        } else {
            for batch in batchables.chunks(MAX_FOLLOWERS_PER_BATCH) {
                followee_messages.push(
                    batch
                        .iter()
                        .map(|collected| collected.follow_change.clone())
                        .collect::<Vec<FollowChange>>()
                        .into(),
                );
            }

            // Merge singles into batchables, we don't want to have both
            if let Some(last) = followee_messages.last_mut() {
                last.drain_from(
                    &mut singles
                        .iter()
                        .cloned()
                        .map(|change| change.follow_change)
                        .collect(),
                );
            }

            for single in singles.drain(..) {
                if let Some(last) = followee_messages.last_mut() {
                    if last.has_room() {
                        last.add(single.follow_change);
                    } else {
                        followee_messages.push(single.follow_change.into());
                    }
                }
            }

            assert!(
                singles.is_empty(),
                "All singles should have been merged into batches"
            );
        }

        messages.extend(followee_messages);
    }
    messages
}

fn record_metrics(messages: &[NotificationMessage], retained_follow_changes: usize) {
    let mut individual_follow_changes = 0;
    let mut aggregated_follow_changes = 0;

    for message in messages {
        if message.is_single() {
            individual_follow_changes += 1;
        } else {
            aggregated_follow_changes += 1;
        }

        metrics::followers_per_message().record(message.follows().len() as f64);
        metrics::unfollowers_per_message().record(message.unfollows().len() as f64);
    }

    metrics::individual_follow_messages().increment(individual_follow_changes as u64);
    metrics::aggregated_follow_messages().increment(aggregated_follow_changes as u64);
    metrics::retained_follow_changes().set(retained_follow_changes as f64);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::followee_notification_factory::ONE_HOUR;
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
        let mut notification_factory =
            NotificationFactory::new(10, 10, FakeRelativeClock::default()).unwrap();

        let follower = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        let change1 = create_follow_change(follower, followee, seconds_to_datetime(1));
        notification_factory.insert(change1);

        let change2 = create_follow_change(follower, followee, seconds_to_datetime(1));
        notification_factory.insert(change2.clone());

        // When they share the same time, the last change added should be kept
        let messages = notification_factory.drain_into_messages();
        assert_eq!(messages.len(), 1);
        let message = &messages[0];
        assert_message_eq(message, &followee, [follower], &[]);
    }

    #[test]
    fn test_does_not_replace_with_older_change() {
        let mut unique_changes =
            NotificationFactory::new(10, 10, FakeRelativeClock::default()).unwrap();

        let follower = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        let newer_change = create_follow_change(follower, followee, seconds_to_datetime(2));
        unique_changes.insert(newer_change.clone());

        let older_change = create_unfollow_change(follower, followee, seconds_to_datetime(1));
        unique_changes.insert(older_change);

        let messages = unique_changes.drain_into_messages();
        assert_eq!(messages.len(), 1);
        assert_message_eq(&messages[0], &followee, [follower], &[]);
    }

    #[test]
    fn test_insert_same_follower_different_followee() {
        let mut unique_changes =
            NotificationFactory::new(10, 10, FakeRelativeClock::default()).unwrap();

        let follower = Keys::generate().public_key();
        let followee1 = Keys::generate().public_key();
        let followee2 = Keys::generate().public_key();

        let change1 = create_follow_change(follower, followee1, seconds_to_datetime(2));
        let change2 = create_follow_change(follower, followee2, seconds_to_datetime(1));

        unique_changes.insert(change1.clone());
        unique_changes.insert(change2.clone());

        let mut messages = unique_changes.drain_into_messages();
        // Both changes should be kept since they have different followees
        assert_eq!(
            messages.sort(),
            [
                NotificationMessage::from(change1.clone()),
                NotificationMessage::from(change2.clone())
            ]
            .sort()
        );
    }

    #[test]
    fn test_an_unfollow_cancels_a_follow() {
        let mut unique_changes =
            NotificationFactory::new(10, 10, FakeRelativeClock::default()).unwrap();

        let follower = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        let follow_change = create_follow_change(follower, followee, seconds_to_datetime(1));
        let unfollow_change = create_unfollow_change(follower, followee, seconds_to_datetime(2));

        unique_changes.insert(follow_change.clone());
        unique_changes.insert(unfollow_change.clone());

        // The unfollow should cancel the follow
        assert_eq!(unique_changes.drain_into_messages(), []);
    }

    #[test]
    fn test_a_follow_cancels_an_unfollow() {
        let mut unique_changes =
            NotificationFactory::new(10, 10, FakeRelativeClock::default()).unwrap();

        let follower = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        let unfollow_change = create_unfollow_change(follower, followee, seconds_to_datetime(1));
        let follow_change = create_follow_change(follower, followee, seconds_to_datetime(2));

        unique_changes.insert(unfollow_change.clone());
        unique_changes.insert(follow_change.clone());

        // The follow should cancel the unfollow
        assert_eq!(unique_changes.drain_into_messages(), []);
    }

    #[test]
    fn test_single_item_batch_before_rate_limit_is_hit() {
        let max_follows_per_hour = 2;
        let max_retention_minutes = 10;

        let mut notification_factory = NotificationFactory::new(
            max_follows_per_hour,
            max_retention_minutes,
            FakeRelativeClock::default(),
        )
        .unwrap();

        let follower1 = Keys::generate().public_key();
        let follower2 = Keys::generate().public_key();
        let follower3 = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        let change1 = create_follow_change(follower1, followee, seconds_to_datetime(1));
        notification_factory.insert(change1.clone());

        let change2 = create_follow_change(follower2, followee, seconds_to_datetime(1));
        notification_factory.insert(change2.clone());

        let change3 = create_follow_change(follower3, followee, seconds_to_datetime(1));
        notification_factory.insert(change3.clone());

        let messages = notification_factory.drain_into_messages();
        // First couple are in separate messages for the same followee because we didn't hit a rate limit
        assert_batches_eq(&messages, &[(followee, &[change1]), (followee, &[change2])]);
        assert_eq!(
            notification_factory.follow_changes_len(),
            1,
            "Expected one follow change to be retained",
        );
    }

    #[test]
    fn test_no_message_after_rate_limit_is_hit_but_retention_not_elapsed() {
        // After one single follow change the rate limit will be hit
        let max_messages_per_hour = 1;
        let max_retention_minutes = 10;

        let clock = FakeRelativeClock::default();
        let mut notification_factory =
            NotificationFactory::new(max_messages_per_hour, max_retention_minutes, clock.clone())
                .unwrap();

        let follower1 = Keys::generate().public_key();
        let follower2 = Keys::generate().public_key();
        let follower3 = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        let change1 = create_follow_change(follower1, followee, seconds_to_datetime(1));
        notification_factory.insert(change1.clone());

        // We hit the rate limit, but the retention time hasn't elapsed yet.
        // The rate is one follow per hour, so we only get one message, the
        // other one is retained.
        let messages = notification_factory.drain_into_messages();
        assert_batches_eq(&messages, &[(followee, &[change1])]);

        let change2 = create_follow_change(follower2, followee, seconds_to_datetime(1));
        notification_factory.insert(change2.clone());

        let change3 = create_follow_change(follower3, followee, seconds_to_datetime(1));
        notification_factory.insert(change3.clone());

        // We hit the limit so the rest of the messages are retained
        let messages = notification_factory.drain_into_messages();
        assert_batches_eq(&messages, &[]);
        assert_eq!(notification_factory.follow_changes_len(), 2);

        // We pass the max retention time, but we still are under the rate limit so we get nothing
        clock.advance(Duration::from_secs((max_retention_minutes as u64) * 60));
        let messages = notification_factory.drain_into_messages();
        assert_batches_eq(&messages, &[]);

        // We clear the rate limit
        clock.advance(Duration::from_secs(50 * 60));
        let messages = notification_factory.drain_into_messages();
        assert_batches_eq(&messages, &[(followee, &[change2, change3])]);
    }

    #[test]
    fn test_batch_sizes_after_rate_limit_and_retention_period() {
        let max_messages_per_hour = 1; // After one single follow change, the rate limit will be hit
        let max_retention_minutes = 10;
        const MAX_FOLLOWERS_TRIPLED: usize = 3 * MAX_FOLLOWERS_PER_BATCH as usize; // The number of messages we will send for testing

        let clock = FakeRelativeClock::default();
        let mut notification_factory =
            NotificationFactory::new(max_messages_per_hour, max_retention_minutes, clock.clone())
                .unwrap();

        let followee = Keys::generate().public_key();

        for i in 0..MAX_FOLLOWERS_TRIPLED {
            let follower = Keys::generate().public_key();
            let change = create_follow_change(follower, followee, seconds_to_datetime(i));
            notification_factory.insert(change.clone());
        }

        // After inserting MAX_FOLLOWERS_TRIPLED changes, we hit the rate limit immediately after the first message.
        // The first message will be sent immediately, while the rest should be retained.
        let messages = notification_factory.drain_into_messages();
        assert_eq!(
            messages.len(),
            1,
            "Expected a single message, got {:?}, changes: {:?}",
            messages,
            messages.iter().map(|m| m.len()).sum::<usize>()
        );
        assert!(
            messages[0].is_single(),
            "Expected a single follow change in the message"
        );
        assert_eq!(
            notification_factory.follow_changes_len(),
            MAX_FOLLOWERS_TRIPLED - 1,
        );

        // All other messages are retained due to rate limiting
        let messages = notification_factory.drain_into_messages();
        assert_eq!(messages.len(), 0);
        assert_eq!(notification_factory.expired_retains_count(), 0);

        // Just after before the max_retention time elapses..
        clock.advance(Duration::from_secs((max_retention_minutes as u64 + 1) * 60));

        // .. we insert another change
        let follower = Keys::generate().public_key();
        let change = create_follow_change(
            follower,
            followee,
            seconds_to_datetime(MAX_FOLLOWERS_TRIPLED + 1),
        );
        notification_factory.insert(change.clone());

        assert_eq!(
            notification_factory.follow_changes_len(),
            MAX_FOLLOWERS_TRIPLED
        );

        // Which is not included in the expired retains
        assert_eq!(
            notification_factory.expired_retains_count(),
            MAX_FOLLOWERS_TRIPLED - 1
        );

        let messages = notification_factory.drain_into_messages();

        // But it will be included in the next batch anyways, regardless of the rate limit
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].len(), MAX_FOLLOWERS_PER_BATCH);
        assert_eq!(messages[1].len(), MAX_FOLLOWERS_PER_BATCH);
        assert_eq!(messages[2].len(), MAX_FOLLOWERS_PER_BATCH);

        // And another change arrives
        let follower = Keys::generate().public_key();
        let change = create_follow_change(
            follower,
            followee,
            seconds_to_datetime(MAX_FOLLOWERS_TRIPLED),
        );
        notification_factory.insert(change.clone());

        // And another one for a different followee
        let followee2 = Keys::generate().public_key();
        let follower = Keys::generate().public_key();
        let change = create_follow_change(
            follower,
            followee2,
            seconds_to_datetime(MAX_FOLLOWERS_TRIPLED),
        );
        notification_factory.insert(change.clone());

        let messages = notification_factory.drain_into_messages();
        // Only the one for the new followee is sent as it's not rate limited, the other one hit the limit so it's retained.
        assert_eq!(messages.len(), 1);
        assert_eq!(notification_factory.follow_changes_len(), 1);

        // The max retention time elapses again, all retained changes should be sent, in batches.
        clock.advance(Duration::from_secs((max_retention_minutes as u64 + 1) * 60));

        let messages = notification_factory.drain_into_messages();
        // This one has a single item
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].len(), 1);
        // We keep the followee info for the time the rate limit counter can
        // calculate the rate which is one hour. This is in case new changes
        // arrive so we remember the rate limit for one more period
        assert_eq!(notification_factory.followees_len(), 2);
        assert_eq!(notification_factory.follow_changes_len(), 0);

        clock.advance(ONE_HOUR + Duration::from_secs(1));
        let messages = notification_factory.drain_into_messages();

        // Now all is cleared
        assert_eq!(notification_factory.followees_len(), 0);
        assert_eq!(notification_factory.follow_changes_len(), 0);
        assert_eq!(messages.len(), 0);
    }

    #[test]
    fn test_is_empty_and_len() {
        let mut unique_changes =
            NotificationFactory::new(10, 10, FakeRelativeClock::default()).unwrap();

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
            NotificationFactory::new(10, 10, FakeRelativeClock::default()).unwrap();

        let follower = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        let change1 = create_follow_change(follower, followee, seconds_to_datetime(2));
        unique_changes.insert(change1);

        let change2 = create_follow_change(follower, followee, seconds_to_datetime(1));
        unique_changes.insert(change2);

        let changes = unique_changes.drain_into_messages();
        assert_eq!(changes.len(), 1);
        assert_message_eq(&changes[0], &followee, [follower], &[])
    }

    fn assert_message_eq(
        message: &NotificationMessage,
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

    fn assert_batches_eq(
        actual: &[NotificationMessage],
        expected: &[(PublicKey, &[FollowChange])],
    ) {
        let mut expected_batches = Vec::new();

        for (_, changes) in expected {
            let batch: NotificationMessage = (*changes).to_vec().into();
            expected_batches.push(batch);
        }

        assert_bag_eq!(actual, expected_batches);
    }
}
