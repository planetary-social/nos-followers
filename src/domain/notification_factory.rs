use super::NotificationMessage;
use crate::domain::{FollowChange, FolloweeNotificationFactory};
use crate::metrics;
use heavykeeper::TopK;
use nostr_sdk::PublicKey;
use ordermap::OrderMap;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::time::Duration;
use tracing::info;

type Followee = PublicKey;

/// Aggregates `FollowChange` events by merging redundant follow/unfollow actions
/// for the same follower-followee pair, managing message batching, and compiling
/// the results into `NotificationMessage` instances per followee.
pub struct NotificationFactory {
    followee_maps: OrderMap<Followee, FolloweeNotificationFactory>,
    burst: u16,
    min_seconds_between_messages: NonZeroUsize,
    top_followees: TopK<Vec<u8>>,
}

impl NotificationFactory {
    pub fn new(burst: u16, min_seconds_between_messages: usize) -> Self {
        info!(
            "One message in {} seconds allowed",
            min_seconds_between_messages
        );

        let min_seconds_between_messages = NonZeroUsize::new(min_seconds_between_messages).unwrap();

        // Initialize TopK with parameters:
        // k=20 (top 20 followees)
        // width=1000 (internal width for accuracy)
        // depth=5 (internal depth for accuracy)
        // decay=0.925 (decay factor for aging out less frequent items)
        let top_followees = TopK::new(20, 1000, 5, 0.925);

        Self {
            followee_maps: OrderMap::with_capacity(1_000),
            burst,
            min_seconds_between_messages,
            top_followees,
        }
    }

    pub fn insert(&mut self, follow_change: Box<FollowChange>) {
        let followee_info = self
            .followee_maps
            .entry(*follow_change.followee())
            .or_insert_with_key(|_| {
                FolloweeNotificationFactory::new(self.burst, self.min_seconds_between_messages)
            });

        // Add to TopK tracking when it's a follow (not unfollow)
        if follow_change.is_follower() {
            // Convert friendly_followee to bytes for TopK tracking
            let friendly_id = follow_change.friendly_followee().to_string();
            self.top_followees.add(friendly_id.as_bytes().to_vec());
        }

        followee_info.insert(follow_change);
    }

    /// Collects follow/unfollow changes per followee into NotificationMessage
    /// objects, which essentially map to push notifications. We don't send more
    /// often than min_time_between_messages unless pending changes don't fit in
    /// a single batched message.
    pub fn flush(&mut self) -> Vec<NotificationMessage> {
        let initial_follow_changes_len = self.follow_changes_len();
        let initial_followees_len = self.followees_len();

        // First try normal flush
        let mut messages = self
            .followee_maps
            .iter_mut()
            .flat_map(|(_, followee_factory)| followee_factory.flush())
            .collect::<Vec<_>>();

        // Force flush changes older than 2 days
        let force_flushed = self
            .followee_maps
            .iter_mut()
            .flat_map(|(_, followee_factory)| {
                followee_factory.force_flush_old_changes(Duration::from_secs(2 * 24 * 3600))
            })
            .collect::<Vec<_>>();

        messages.extend(force_flushed);

        // More aggressive cleanup
        self.followee_maps.retain(|_, followee_factory| {
            !followee_factory.should_delete() && followee_factory.follow_changes.len() < 1000
            // Add size limit per followee
        });

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

        // Log top 20 most followed accounts
        info!("Top 20 most followed accounts:");
        for node in self.top_followees.list() {
            if let Ok(friendly_id) = String::from_utf8(node.item.to_vec()) {
                info!("    {}: {} follows", friendly_id, node.count);
            }
        }
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
    use nostr_sdk::prelude::Keys;
    use std::iter::repeat;
    use std::sync::LazyLock;
    use std::time::Duration;
    use tokio::time::advance;

    static NOW: LazyLock<DateTime<Utc>> =
        LazyLock::new(|| DateTime::<Utc>::from_timestamp(Utc::now().timestamp(), 0).unwrap());

    #[test]
    fn test_insert_follow_change() {
        let mut notification_factory = NotificationFactory::new(10, 60);

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
        let mut notification_factory = NotificationFactory::new(10, 60);

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
        let mut notification_factory = NotificationFactory::new(10, 60);

        let follower = Keys::generate().public_key();
        let followee1 = Keys::generate().public_key();
        let followee2 = Keys::generate().public_key();

        let change1 = create_follow_change(follower, followee1, seconds_to_datetime(2));
        let change2 = create_follow_change(follower, followee2, seconds_to_datetime(1));

        notification_factory.insert(change1.clone().into());
        notification_factory.insert(change2.clone().into());

        let messages = notification_factory.flush();
        // Both changes should be kept since they have different followees
        assert_bag_eq!(
            messages,
            [
                NotificationMessage::from(Box::new(change1.clone())),
                NotificationMessage::from(Box::new(change2.clone()))
            ]
        );
    }

    #[test]
    fn test_an_unfollow_cancels_a_follow() {
        let mut notification_factory = NotificationFactory::new(10, 60);

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
        let mut notification_factory = NotificationFactory::new(10, 60);
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
    fn test_first_single_item_batch() {
        let min_seconds_between_messages = 3600;

        let mut notification_factory = NotificationFactory::new(10, min_seconds_between_messages);

        let followee = Keys::generate().public_key();

        let change1 = insert_new_follower(&mut notification_factory, followee);
        let change2 = insert_new_follower(&mut notification_factory, followee);
        let change3 = insert_new_follower(&mut notification_factory, followee);

        let messages = notification_factory.flush();

        assert_batches_eq(&messages, &[(followee, &[change1, change2, change3])]);
        assert_eq!(notification_factory.follow_changes_len(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn test_no_more_messages_after_burst_and_before_next_token() {
        // After one single follow change, we need to wait min period
        let min_seconds_between_messages = 12 * 3600; // 12 hours

        let mut notification_factory = NotificationFactory::new(1, min_seconds_between_messages);

        let followee = Keys::generate().public_key();

        let change1 = insert_new_follower(&mut notification_factory, followee);

        let messages = notification_factory.flush();
        assert_batches_eq(&messages, &[(followee, &[change1])]);

        let change2 = insert_new_follower(&mut notification_factory, followee);
        let change3 = insert_new_follower(&mut notification_factory, followee);

        advance(Duration::from_secs(1)).await;

        // We didn't wait min seconds, and the burst is set to 1, so we need to
        // wait a full min_seconds_between_messages period to get the message
        let messages = notification_factory.flush();
        assert_batches_eq(&messages, &[]);
        assert_eq!(notification_factory.follow_changes_len(), 2);

        // We pass the number of minimum seconds between messages so all are sent
        advance(Duration::from_secs(min_seconds_between_messages as u64 + 1)).await;
        let messages = notification_factory.flush();
        assert_batches_eq(&messages, &[(followee, &[change2, change3])]);
    }

    #[tokio::test(start_paused = true)]
    async fn test_batch_sizes_after_min_seconds() {
        // After one single follow change, we need to wait min period
        let min_seconds_between_messages = 12 * 3600; // 12 hours
        const MAX_FOLLOWERS_TIMES_TEN: usize = 10 * MAX_FOLLOWERS_PER_BATCH;

        let mut notification_factory = NotificationFactory::new(10, min_seconds_between_messages);

        let followee = Keys::generate().public_key();
        insert_new_follower(&mut notification_factory, followee);
        let messages = notification_factory.flush();
        advance(Duration::from_secs(1)).await;
        // One is accepted
        assert_eq!(messages.len(), 1,);
        assert!(messages[0].is_single());

        repeat(()).take(MAX_FOLLOWERS_TIMES_TEN).for_each(|_| {
            insert_new_follower(&mut notification_factory, followee);
        });

        // All inserted followers use the capacity of the rate limiter, so they are all sent
        let messages = notification_factory.flush();
        assert_eq!(messages.len(), 10);
        assert_eq!(notification_factory.follow_changes_len(), 0);

        // Before the max_retention time elapses..
        advance(Duration::from_secs(
            (min_seconds_between_messages - 1) as u64,
        ))
        .await;

        // .. we insert another change
        insert_new_follower(&mut notification_factory, followee);

        assert_eq!(notification_factory.follow_changes_len(), 1);

        let messages = notification_factory.flush();
        assert_eq!(messages.len(), 0);

        // And we finally hit the min period so we get one token
        advance(Duration::from_secs(6u64)).await;
        let messages = notification_factory.flush();

        // The token is used by the last change
        assert_eq!(messages.len(), 1);
        assert_eq!(notification_factory.follow_changes_len(), 0);

        // And another change arrives
        insert_new_follower(&mut notification_factory, followee);

        // And another one for a different followee
        insert_new_follower(&mut notification_factory, Keys::generate().public_key());

        let messages = notification_factory.flush();
        // The new one is flushed, the old one is retained
        assert_eq!(messages.len(), 1);
        assert_eq!(notification_factory.follow_changes_len(), 1);

        advance(Duration::from_secs(min_seconds_between_messages as u64 + 1)).await;
        let messages = notification_factory.flush();

        // Now all are flushed
        assert_eq!(messages.len(), 1);
        assert_eq!(notification_factory.follow_changes_len(), 0);

        // But we keep the followee info for the time calculations for a day,
        // event if no changes are pending
        assert_eq!(notification_factory.followees_len(), 2);
        assert_eq!(notification_factory.follow_changes_len(), 0);

        advance(Duration::from_secs(24 * 60 * 60_u64 + 1)).await;
        let messages = notification_factory.flush();

        // Now all is cleared, no followee info is retained and next messages
        // replenished the burst capacity
        assert_eq!(notification_factory.follow_changes_len(), 0);
        assert_eq!(notification_factory.followees_len(), 0);
        assert_eq!(messages.len(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn test_after_burst_of_10_messages_wait_12_hours() {
        let min_seconds_between_messages = 12 * 3600; // 12 hours
        let mut notification_factory = NotificationFactory::new(10, min_seconds_between_messages);

        let followee = Keys::generate().public_key();

        // After 10 messages
        for _ in 0..10 {
            insert_new_follower(&mut notification_factory, followee);
            let messages = notification_factory.flush();
            assert_eq!(messages.len(), 1);
            assert_eq!(notification_factory.follow_changes_len(), 0);
            assert_eq!(notification_factory.followees_len(), 1);
        }

        // We used all initial credit from the rate limiter
        insert_new_follower(&mut notification_factory, followee);

        let messages = notification_factory.flush();
        assert_eq!(messages.len(), 0);
        assert_eq!(notification_factory.follow_changes_len(), 1);
        assert_eq!(notification_factory.followees_len(), 1);

        // We wait 12 hours and a new token is available
        advance(Duration::from_secs(12 * 3600)).await;
        let messages = notification_factory.flush();
        assert_eq!(messages.len(), 1);
        assert_eq!(notification_factory.follow_changes_len(), 0);
        assert_eq!(notification_factory.followees_len(), 1);

        // After 24 hours with no messages the follower entry is removed with its rated limiter
        advance(Duration::from_secs(24 * 3600)).await;
        let messages = notification_factory.flush();
        assert_eq!(messages.len(), 0);
        assert_eq!(notification_factory.follow_changes_len(), 0);
        assert_eq!(notification_factory.followees_len(), 0);

        // And again, we have the initial credit of 10 messages
        advance(Duration::from_secs(24 * 3600)).await;
        for _ in 0..10 {
            insert_new_follower(&mut notification_factory, followee);
            advance(Duration::from_secs(min_seconds_between_messages as u64)).await;
            let messages = notification_factory.flush();
            assert_eq!(messages.len(), 1);
            assert_eq!(notification_factory.follow_changes_len(), 0);
            assert_eq!(notification_factory.followees_len(), 1);
        }
    }

    #[test]
    fn test_is_empty_and_len() {
        let mut notification_factory = NotificationFactory::new(10, 60);

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
        let min_seconds_between_messages = 60;
        let mut notification_factory = NotificationFactory::new(10, min_seconds_between_messages);

        let followee = Keys::generate().public_key();

        insert_new_follower(&mut notification_factory, followee);

        advance(Duration::from_secs(min_seconds_between_messages as u64 + 1)).await;

        let messages = notification_factory.flush();
        assert_eq!(messages.len(), 1);

        insert_new_unfollower(&mut notification_factory, followee);

        advance(Duration::from_secs(min_seconds_between_messages as u64 + 1)).await;

        let messages = notification_factory.flush();
        assert_eq!(messages.len(), 0);
    }

    #[test]
    fn test_flush_clears_map() {
        let mut notification_factory = NotificationFactory::new(10, 60);

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

    fn insert_new_unfollower(
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
            let batch: NotificationMessage = (*changes).iter().cloned().map(|fc| fc.into()).into();
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
