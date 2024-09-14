use super::NotificationMessage;
use crate::domain::{FollowChange, FolloweeNotificationFactory};
use crate::metrics;
use nostr_sdk::PublicKey;
use ordermap::OrderMap;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use tokio::time::Duration;
use tracing::info;

type Followee = PublicKey;

/// Aggregates `FollowChange` events by merging redundant follow/unfollow actions
/// for the same follower-followee pair, managing message batching, and compiling
/// the results into `NotificationMessage` instances per followee.
pub struct NotificationFactory {
    followee_maps: OrderMap<Followee, FolloweeNotificationFactory>,
    min_time_between_messages: Duration,
}

impl NotificationFactory {
    pub fn new(min_seconds_between_messages: NonZeroUsize) -> Self {
        info!(
            "One message in {} seconds allowed",
            min_seconds_between_messages
        );

        Self {
            followee_maps: OrderMap::with_capacity(1_000),
            min_time_between_messages: Duration::from_secs(
                min_seconds_between_messages.get() as u64
            ),
        }
    }

    pub fn insert(&mut self, follow_change: Box<FollowChange>) {
        let followee_info = self
            .followee_maps
            .entry(*follow_change.followee())
            .or_insert_with_key(|_| {
                FolloweeNotificationFactory::new(self.min_time_between_messages)
            });

        followee_info.insert(follow_change);
    }

    /// Collects follow/unfollow changes per followee into NotificationMessage
    /// objects, which essentially map to push notifications. We don't send more
    /// often than min_time_between_messages unless pending changes don't fit in
    /// a single batched message.
    pub fn flush(&mut self) -> Vec<NotificationMessage> {
        let initial_follow_changes_len = self.follow_changes_len();
        let initial_followees_len = self.followees_len();

        let messages = self
            .followee_maps
            .iter_mut()
            .flat_map(|(_, followee_factory)| followee_factory.flush())
            .collect::<Vec<_>>();

        self.followee_maps
            .retain(|_, followee_factory| !followee_factory.should_delete());

        let follow_changes_len = self.follow_changes_len();
        record_metrics(&messages, follow_changes_len);
        self.log_report(&messages, initial_follow_changes_len, initial_followees_len);

        messages
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
    }

    metrics::individual_follow_messages().increment(individual_follow_changes as u64);
    metrics::aggregated_follow_messages().increment(aggregated_follow_changes as u64);
    metrics::retained_follow_changes().set(retained_follow_changes as f64);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::notification_message::MAX_FOLLOWERS_PER_BATCH;
    use assertables::*;
    use chrono::{DateTime, Utc};
    use nonzero_ext::nonzero;
    use nostr_sdk::prelude::Keys;
    use std::iter::repeat;
    use std::sync::LazyLock;
    use std::time::Duration;
    use tokio::time::advance;

    static NOW: LazyLock<DateTime<Utc>> = LazyLock::new(|| {
        DateTime::<Utc>::from_timestamp(Utc::now().timestamp() as i64, 0).unwrap()
    });

    #[test]
    fn test_insert_follow_change() {
        let mut notification_factory = NotificationFactory::new(nonzero!(60usize));

        let follower = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        let change1 = create_follow_change(follower, followee, seconds_to_datetime(1));
        notification_factory.insert(change1.into());

        let change2 = create_follow_change(follower, followee, seconds_to_datetime(1));
        notification_factory.insert(change2.into());

        // When they share the same time, the last change added should be kept
        let messages = notification_factory.flush();
        assert_eq!(messages.len(), 1);
        let message = &messages[0];
        assert_message_eq(message, &followee, [follower]);
    }

    #[test]
    fn test_does_not_replace_with_older_change() {
        let mut notification_factory = NotificationFactory::new(nonzero!(60usize));

        let follower = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        let newer_change = create_follow_change(follower, followee, seconds_to_datetime(2));
        notification_factory.insert(newer_change.into());

        let older_change = create_unfollow_change(follower, followee, seconds_to_datetime(1));
        notification_factory.insert(older_change.into());

        let messages = notification_factory.flush();
        assert_eq!(messages.len(), 1);
        assert_message_eq(&messages[0], &followee, [follower]);
    }

    #[test]
    fn test_insert_same_follower_different_followee() {
        let mut notification_factory = NotificationFactory::new(nonzero!(60usize));

        let follower = Keys::generate().public_key();
        let followee1 = Keys::generate().public_key();
        let followee2 = Keys::generate().public_key();

        let change1 = create_follow_change(follower, followee1, seconds_to_datetime(2));
        let change2 = create_follow_change(follower, followee2, seconds_to_datetime(1));

        notification_factory.insert(change1.clone().into());
        notification_factory.insert(change2.clone().into());

        let mut messages = notification_factory.flush();
        // Both changes should be kept since they have different followees
        assert_eq!(
            messages.sort(),
            [
                NotificationMessage::from(Box::new(change1)),
                NotificationMessage::from(Box::new(change2))
            ]
            .sort()
        );
    }

    #[test]
    fn test_an_unfollow_cancels_a_follow() {
        let mut notification_factory = NotificationFactory::new(nonzero!(60usize));

        let follower = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        let follow_change = create_follow_change(follower, followee, seconds_to_datetime(1));
        let unfollow_change = create_unfollow_change(follower, followee, seconds_to_datetime(2));

        notification_factory.insert(follow_change.into());
        notification_factory.insert(unfollow_change.into());

        // The unfollow should cancel the follow
        assert_eq!(notification_factory.flush(), []);
    }

    #[test]
    fn test_a_follow_cancels_an_unfollow() {
        let mut notification_factory = NotificationFactory::new(nonzero!(60usize));
        let follower = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        let unfollow_change = create_unfollow_change(follower, followee, seconds_to_datetime(1));
        let follow_change = create_follow_change(follower, followee, seconds_to_datetime(2));

        notification_factory.insert(unfollow_change.into());
        notification_factory.insert(follow_change.into());

        // The follow should cancel the unfollow
        assert_eq!(notification_factory.flush(), []);
    }

    #[test]
    fn test_single_item_batch_before_rate_limit_is_hit() {
        let min_seconds_between_messages = nonzero!(3600usize);

        let mut notification_factory = NotificationFactory::new(min_seconds_between_messages);

        let followee = Keys::generate().public_key();

        let change1 = insert_new_follower(&mut notification_factory, followee);
        let change2 = insert_new_follower(&mut notification_factory, followee);
        let change3 = insert_new_follower(&mut notification_factory, followee);

        let messages = notification_factory.flush();

        assert_batches_eq(&messages, &[(followee, &[change1, change2, change3])]);
        assert_eq!(notification_factory.follow_changes_len(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn test_no_message_after_rate_limit_is_hit_but_retention_not_elapsed() {
        // After one single follow change the rate limit will be hit
        let min_seconds_between_messages = nonzero!(3600usize);

        let mut notification_factory = NotificationFactory::new(min_seconds_between_messages);

        let followee = Keys::generate().public_key();

        let change1 = insert_new_follower(&mut notification_factory, followee);

        let messages = notification_factory.flush();
        assert_batches_eq(&messages, &[(followee, &[change1])]);

        let change2 = insert_new_follower(&mut notification_factory, followee);
        let change3 = insert_new_follower(&mut notification_factory, followee);

        advance(Duration::from_secs(1)).await;

        // We hit the limit so the rest of the messages are retained
        let messages = notification_factory.flush();
        assert_batches_eq(&messages, &[]);
        assert_eq!(notification_factory.follow_changes_len(), 2);

        // We pass the number of minimum seconds between messages so all are sent
        advance(Duration::from_secs(
            min_seconds_between_messages.get() as u64 * 60,
        ))
        .await;
        let messages = notification_factory.flush();
        assert_batches_eq(&messages, &[(followee, &[change2, change3])]);
    }

    #[tokio::test(start_paused = true)]
    async fn test_batch_sizes_after_rate_limit_and_retention_period() {
        // After one single follow change, the rate limit will be hit
        let min_seconds_between_messages = nonzero!(3600usize);
        const MAX_FOLLOWERS_TRIPLED: usize = 3 * MAX_FOLLOWERS_PER_BATCH;

        let mut notification_factory = NotificationFactory::new(min_seconds_between_messages);

        let followee = Keys::generate().public_key();
        insert_new_follower(&mut notification_factory, followee);
        let messages = notification_factory.flush();
        advance(Duration::from_secs(1)).await;
        // Sent and hit the rate limit
        assert_eq!(messages.len(), 1,);
        assert!(messages[0].is_single());

        repeat(()).take(MAX_FOLLOWERS_TRIPLED).for_each(|_| {
            insert_new_follower(&mut notification_factory, followee);
        });

        // All inserted MAX_FOLLOWERS_TRIPLED changes wait for next available flush time
        let messages = notification_factory.flush();
        assert_eq!(messages.len(), 0);
        assert_eq!(
            notification_factory.follow_changes_len(),
            MAX_FOLLOWERS_TRIPLED,
        );

        // Before the max_retention time elapses..
        advance(Duration::from_secs(
            (min_seconds_between_messages.get() - 5) as u64 * 60,
        ))
        .await;

        // .. we insert another change
        insert_new_follower(&mut notification_factory, followee);

        assert_eq!(
            notification_factory.follow_changes_len(),
            MAX_FOLLOWERS_TRIPLED + 1,
        );

        let messages = notification_factory.flush();

        // But it will be included in the next batch anyways, regardless of the rate limit
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[0].len(), MAX_FOLLOWERS_PER_BATCH);
        assert_eq!(messages[1].len(), MAX_FOLLOWERS_PER_BATCH);
        assert_eq!(messages[2].len(), MAX_FOLLOWERS_PER_BATCH);
        assert_eq!(messages[3].len(), 1);

        // And another change arrives
        insert_new_follower(&mut notification_factory, followee);

        // And another one for a different followee
        insert_new_follower(&mut notification_factory, Keys::generate().public_key());

        let messages = notification_factory.flush();
        // The new one is flushed, the old one is retained
        assert_eq!(messages.len(), 1);
        assert_eq!(notification_factory.follow_changes_len(), 1);

        advance(Duration::from_secs(
            min_seconds_between_messages.get() as u64 * 60 + 1,
        ))
        .await;
        let messages = notification_factory.flush();

        // Now all are flushed
        assert_eq!(messages.len(), 1);
        assert_eq!(notification_factory.follow_changes_len(), 0);

        // But we keep the followee info for the time calculations for one more
        // period, event if no changes are pending
        assert_eq!(notification_factory.followees_len(), 1);
        assert_eq!(notification_factory.follow_changes_len(), 0);

        advance(Duration::from_secs(
            min_seconds_between_messages.get() as u64 * 60 + 1,
        ))
        .await;
        let messages = notification_factory.flush();

        // Now all is cleared
        assert_eq!(notification_factory.follow_changes_len(), 0);
        assert_eq!(notification_factory.followees_len(), 0);
        assert_eq!(messages.len(), 0);
    }

    #[test]
    fn test_is_empty_and_len() {
        let mut notification_factory = NotificationFactory::new(nonzero!(60usize));

        let followee1 = Keys::generate().public_key();
        let followee2 = Keys::generate().public_key();

        assert!(notification_factory.is_empty());
        assert_eq!(notification_factory.follow_changes_len(), 0);
        assert_eq!(notification_factory.followees_len(), 0);

        insert_new_follower(&mut notification_factory, followee1);
        insert_new_follower(&mut notification_factory, followee2);
        insert_new_follower(&mut notification_factory, followee2);

        assert!(!notification_factory.is_empty());
        assert_eq!(notification_factory.follow_changes_len(), 3);
        assert_eq!(notification_factory.followees_len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn test_unfollows_are_not_sent() {
        let min_seconds_between_messages = nonzero!(60usize);
        let mut notification_factory = NotificationFactory::new(min_seconds_between_messages);

        let followee = Keys::generate().public_key();

        insert_new_follower(&mut notification_factory, followee.clone());

        advance(Duration::from_secs(
            min_seconds_between_messages.get() as u64 * 60 + 1,
        ))
        .await;

        let messages = notification_factory.flush();
        assert_eq!(messages.len(), 1);

        insert_new_unfollower(&mut notification_factory, followee);

        advance(Duration::from_secs(
            min_seconds_between_messages.get() as u64 * 60 + 1,
        ))
        .await;

        let messages = notification_factory.flush();
        assert_eq!(messages.len(), 0);
    }

    #[test]
    fn test_flush_clears_map() {
        let mut notification_factory = NotificationFactory::new(nonzero!(60usize));

        let follower = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        insert_follower(&mut notification_factory, followee, follower);
        insert_follower(&mut notification_factory, followee, follower);

        let messages = notification_factory.flush();

        assert_eq!(messages.len(), 1);
        assert_message_eq(&messages[0], &followee, [follower])
    }

    fn insert_follower(
        notification_factory: &mut NotificationFactory,
        followee: PublicKey,
        follower: PublicKey,
    ) -> FollowChange {
        let change = create_follow_change(follower, followee, seconds_to_datetime(1));
        notification_factory.insert(change.clone().into());
        change
    }

    fn insert_new_follower(
        notification_factory: &mut NotificationFactory,
        followee: PublicKey,
    ) -> FollowChange {
        let follower = Keys::generate().public_key();
        insert_follower(notification_factory, followee, follower)
    }

    fn insert_new_unfollower<'a>(
        notification_factory: &mut NotificationFactory,
        followee: PublicKey,
    ) -> FollowChange {
        let follower = Keys::generate().public_key();
        let change = create_unfollow_change(follower, followee, seconds_to_datetime(1));
        notification_factory.insert(change.clone().into());
        change
    }

    fn assert_message_eq(
        message: &NotificationMessage,
        followee: &PublicKey,
        follows: impl AsRef<[PublicKey]>,
    ) {
        assert_eq!(message.followee(), followee);

        let follows_vec: Vec<PublicKey> = message.follows().iter().cloned().collect();

        assert_bag_eq!(follows_vec, follows.as_ref());
    }

    fn seconds_to_datetime(seconds: usize) -> DateTime<Utc> {
        NOW.to_utc() + Duration::from_secs(seconds as u64)
    }

    fn assert_batches_eq(
        actual: &[NotificationMessage],
        expected: &[(PublicKey, &[FollowChange])],
    ) {
        let mut expected_batches = Vec::new();

        for (_, changes) in expected {
            let batch: NotificationMessage =
                (*changes).to_vec().into_iter().map(|fc| fc.into()).into();
            expected_batches.push(batch);
        }

        assert_bag_eq!(actual, expected_batches);
    }

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
}
