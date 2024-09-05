use super::FollowChange;
use crate::rate_counter::RateCounter;
use governor::{clock::Clock, clock::Reference};
use nostr_sdk::PublicKey;
use ordermap::OrderMap;
use std::fmt::Debug;
use std::time::Duration;

type Follower = PublicKey;
type Followee = PublicKey;

pub const ONE_HOUR: Duration = Duration::from_secs(60 * 60);

pub enum SendableFollowChange<T: Clock> {
    Single(RetainedFollowChange<T>),
    Batchable(RetainedFollowChange<T>),
}

impl<T: Clock> SendableFollowChange<T> {
    pub fn is_batchable(&self) -> bool {
        matches!(self, SendableFollowChange::Batchable(_))
    }
}

#[derive(Clone)]
pub struct RetainedFollowChange<T: Clock> {
    pub follow_change: FollowChange,
    pub inserted_at: T::Instant,
}

impl<T: Clock> RetainedFollowChange<T> {
    fn new(follow_change: FollowChange, inserted_at: T::Instant) -> Self {
        Self {
            follow_change,
            inserted_at,
        }
    }

    fn has_expired(&self, now: T::Instant, max_retention: Duration) -> bool {
        assert!(now >= self.inserted_at);
        assert!(max_retention > Duration::from_secs(0));

        now.duration_since(self.inserted_at) > max_retention.into()
    }
}

impl<T> Debug for RetainedFollowChange<T>
where
    T: Clock,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CollectedFollowChange")
            .field("follow_change", &self.follow_change)
            .field("inserted_at", &self.inserted_at)
            .finish()
    }
}

pub struct FolloweeNotificationFactory<T: Clock> {
    pub rate_counter: RateCounter<T>,
    pub follow_changes: OrderMap<Follower, RetainedFollowChange<T>>,
    pub followee: Option<Followee>,
    max_retention: Duration,
    clock: T,
}

impl<T: Clock> FolloweeNotificationFactory<T> {
    pub fn new(max_messages_per_hour: usize, max_retention: Duration, clock: T) -> Self {
        let rate_counter = RateCounter::new(max_messages_per_hour, ONE_HOUR, clock.clone());

        Self {
            rate_counter,
            follow_changes: OrderMap::with_capacity(100),
            followee: None,
            max_retention,
            clock,
        }
    }

    pub fn add_follower_change(&mut self, follow_change: FollowChange) {
        match &self.followee {
            Some(followee) => {
                assert_eq!(
                    followee, &follow_change.followee,
                    "Followee mismatch in add_follower_change"
                );
            }
            None => {
                self.followee = Some(follow_change.followee);
            }
        }

        let follower = follow_change.follower;

        if let Some(existing_change) = self.follow_changes.get(&follower) {
            if follow_change.followed_at < existing_change.follow_change.followed_at {
                return;
            }

            if follow_change.change_type != existing_change.follow_change.change_type {
                self.follow_changes.remove(&follower);
                return;
            }
        }

        self.follow_changes.insert(
            follower,
            RetainedFollowChange::new(follow_change, self.clock.now()),
        );
    }

    pub fn is_deletable(&mut self) -> bool {
        // We want to retain the followee info even if there are no
        // changes so we remember the rate limit for one more period in
        // case new changes arrive
        self.follow_changes.is_empty() && !self.rate_counter.is_full()
    }

    pub fn expired_retains_count(&self) -> usize {
        let now = self.clock.now();
        self.follow_changes
            .values()
            .filter(|f| f.has_expired(now, self.max_retention))
            .count()
    }

    pub fn drain_into_sendables(&mut self, sendables: &mut Vec<SendableFollowChange<T>>) {
        let expired_count = self.expired_retains_count();
        let some_retains_expired = expired_count > 0;

        // TODO: extract_if would have been great here, keep an eye on nightly
        self.follow_changes
            .retain(|_follower, collected_follow_change| {
                collect_sendables(
                    &self.max_retention,
                    sendables,
                    collected_follow_change,
                    &self.clock,
                    &mut self.rate_counter,
                    some_retains_expired,
                )
            });

        let there_are_single_messages = sendables.iter().any(|f| !f.is_batchable());

        if expired_count == 0 {
            assert!(
                sendables.len() <= self.rate_counter.limit(),
                "Published messages should not exceed the rate limit when no retained messages have expired"
            );
        } else {
            assert!(
                !there_are_single_messages,
                "All messages should be batchable when there are expired retained messages"
            );

            assert!(
                self.follow_changes.is_empty(),
                "The existence of some expired retains should drain them all"
            );

            assert!(
                !sendables.is_empty(),
                "Messages should be published when retained messages have expired"
            );
        }
    }
}

impl<T: Clock> Debug for FolloweeNotificationFactory<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FolloweeNotificationFactory")
            .field("follow_changes", &self.follow_changes)
            .finish()
    }
}

fn collect_sendables<T: Clock>(
    max_retention: &Duration,
    follow_changes_to_publish: &mut Vec<SendableFollowChange<T>>,
    collected_follow_change: &mut RetainedFollowChange<T>,
    clock: &T,
    rate_counter: &mut RateCounter<T>,
    some_retains_expired: bool,
) -> bool {
    if collected_follow_change.has_expired(clock.now(), *max_retention) {
        send(
            collected_follow_change,
            follow_changes_to_publish,
            rate_counter,
            some_retains_expired,
        );
        return false;
    }

    if rate_counter.is_full() && !some_retains_expired {
        return true;
    }

    send(
        collected_follow_change,
        follow_changes_to_publish,
        rate_counter,
        some_retains_expired,
    );
    false
}

fn send<T: Clock>(
    collected_follow_change: &RetainedFollowChange<T>,
    follow_changes_to_publish: &mut Vec<SendableFollowChange<T>>,
    rate_counter: &mut RateCounter<T>,
    batchable: bool,
) {
    let collected_follow_change = if batchable {
        SendableFollowChange::Batchable(collected_follow_change.clone())
    } else {
        SendableFollowChange::Single(collected_follow_change.clone())
    };
    follow_changes_to_publish.push(collected_follow_change);
    rate_counter.bump();
}
