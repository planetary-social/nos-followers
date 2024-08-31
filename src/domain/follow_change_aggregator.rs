use crate::domain::FollowChange;
use indexmap::IndexMap;
use nostr_sdk::PublicKey;

/// A struct that reduces noise by collapsing multiple follow/unfollow actions
/// for the same follower-followee pair into a single `FollowChange`. Only the
/// most recent change is kept, preventing unnecessary toggles.
pub struct FollowChangeAggregator {
    // IndexMap is used to preserve insertion order
    unique_follow_changes: IndexMap<PublicKey, FollowChange>,
}

impl FollowChangeAggregator {
    pub fn new(size_threshold: usize) -> Self {
        Self {
            unique_follow_changes: IndexMap::with_capacity(size_threshold),
        }
    }
    pub fn insert(&mut self, follow_change: FollowChange) {
        let key = follow_change.followee;

        if let Some(existing_change) = self.unique_follow_changes.get(&key) {
            // Replace only if the new follow_change is more recent
            if follow_change.at >= existing_change.at {
                // ...and the same type
                if existing_change.change_type == follow_change.change_type {
                    self.unique_follow_changes.insert(key, follow_change);
                } else {
                    // A follow followed by and unfollow is a no-op
                    // An unfollow followed by a follow is a no-op
                    // So remove it
                    self.unique_follow_changes.shift_remove(&key);
                }
            }
        } else {
            self.unique_follow_changes.insert(key, follow_change);
        }
    }

    pub fn drain(&mut self) -> Vec<FollowChange> {
        self.unique_follow_changes
            .drain(..)
            .map(|(_, v)| v)
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.unique_follow_changes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.unique_follow_changes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Duration, Utc};
    use nostr_sdk::prelude::Keys;
    use std::time::UNIX_EPOCH;

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
        let mut unique_changes = FollowChangeAggregator::new(10);

        let follower = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        let change1 = create_follow_change(follower, followee, seconds_to_datetime(1));
        unique_changes.insert(change1);

        let change2 = create_follow_change(follower, followee, seconds_to_datetime(1));
        unique_changes.insert(change2.clone());

        // When they share the same time, the last change added should be kept
        assert_eq!(unique_changes.drain(), [change2]);
    }

    #[test]
    fn test_does_not_replace_with_older_change() {
        let mut unique_changes = FollowChangeAggregator::new(10);

        let follower = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        let newer_change = create_follow_change(follower, followee, seconds_to_datetime(2));
        unique_changes.insert(newer_change.clone());

        let older_change = create_unfollow_change(follower, followee, seconds_to_datetime(1));
        unique_changes.insert(older_change);

        // The older change should not replace the newer one, insertion order doesn't matter
        assert_eq!(unique_changes.drain(), [newer_change]);
    }

    #[test]
    fn test_insert_different_followee() {
        let mut unique_changes = FollowChangeAggregator::new(10);

        let follower = Keys::generate().public_key();
        let followee1 = Keys::generate().public_key();
        let followee2 = Keys::generate().public_key();

        let change1 = create_follow_change(follower, followee1, seconds_to_datetime(2));
        let change2 = create_follow_change(follower, followee2, seconds_to_datetime(1));

        unique_changes.insert(change1.clone());
        unique_changes.insert(change2.clone());

        // Both changes should be kept since they have different followees
        assert_eq!(unique_changes.drain(), [change1, change2]);
    }

    #[test]
    fn test_an_unfollow_cancels_a_follow() {
        let mut unique_changes = FollowChangeAggregator::new(10);

        let follower = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        let follow_change = create_follow_change(follower, followee, seconds_to_datetime(1));
        let unfollow_change = create_unfollow_change(follower, followee, seconds_to_datetime(2));

        unique_changes.insert(follow_change.clone());
        unique_changes.insert(unfollow_change.clone());

        // The unfollow should cancel the follow
        assert_eq!(unique_changes.drain(), []);
    }

    #[test]
    fn test_a_follow_cancels_an_unfollow() {
        let mut unique_changes = FollowChangeAggregator::new(10);

        let follower = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        let unfollow_change = create_unfollow_change(follower, followee, seconds_to_datetime(1));
        let follow_change = create_follow_change(follower, followee, seconds_to_datetime(2));

        unique_changes.insert(unfollow_change.clone());
        unique_changes.insert(follow_change.clone());

        // The follow should cancel the unfollow
        assert_eq!(unique_changes.drain(), []);
    }

    #[test]
    fn test_is_empty_and_len() {
        let mut unique_changes = FollowChangeAggregator::new(10);

        let follower = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        assert!(unique_changes.is_empty());
        assert_eq!(unique_changes.len(), 0);

        let change = create_follow_change(follower, followee, seconds_to_datetime(1));
        unique_changes.insert(change);

        assert!(!unique_changes.is_empty());
        assert_eq!(unique_changes.len(), 1);
    }

    #[test]
    fn test_drain_clears_map() {
        let mut unique_changes = FollowChangeAggregator::new(10);

        let follower = Keys::generate().public_key();
        let followee = Keys::generate().public_key();

        let change1 = create_follow_change(follower, followee, seconds_to_datetime(2));
        unique_changes.insert(change1);

        let change2 = create_follow_change(follower, followee, seconds_to_datetime(1));
        unique_changes.insert(change2);

        let changes = unique_changes.drain();
        assert_eq!(changes.len(), 1);
        assert!(unique_changes.is_empty());
    }

    fn seconds_to_datetime(seconds: i64) -> DateTime<Utc> {
        DateTime::<Utc>::from(UNIX_EPOCH + Duration::seconds(seconds).to_std().unwrap())
    }
}
