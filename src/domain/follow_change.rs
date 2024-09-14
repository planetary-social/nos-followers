use crate::account_info::FriendlyId;
use crate::metrics;
use chrono::{DateTime, Utc};
use nostr_sdk::prelude::*;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ChangeType {
    Followed,
    Unfollowed,
}

/// A change in the follow relationship between two users.
#[derive(Clone, PartialOrd, Ord)]
pub struct FollowChange {
    change_type: ChangeType,
    followed_at: DateTime<Utc>,
    follower: PublicKey,
    friendly_follower: FriendlyId,
    followee: PublicKey,
    friendly_followee: FriendlyId,
}

impl PartialEq for FollowChange {
    fn eq(&self, other: &Self) -> bool {
        self.change_type == other.change_type
            && self.followed_at == other.followed_at
            && self.follower == other.follower
            && self.followee == other.followee
    }
}

impl Eq for FollowChange {}

impl FollowChange {
    pub fn new_followed(at: DateTime<Utc>, follower: PublicKey, followee: PublicKey) -> Self {
        metrics::follows().increment(1);

        Self {
            change_type: ChangeType::Followed,
            followed_at: at,
            follower,
            friendly_follower: FriendlyId::PublicKey(follower.to_hex()),
            followee,
            friendly_followee: FriendlyId::PublicKey(followee.to_hex()),
        }
    }

    pub fn new_unfollowed(at: DateTime<Utc>, follower: PublicKey, followee: PublicKey) -> Self {
        metrics::unfollows().increment(1);

        Self {
            change_type: ChangeType::Unfollowed,
            followed_at: at,
            follower,
            friendly_follower: FriendlyId::PublicKey(follower.to_hex()),
            followee,
            friendly_followee: FriendlyId::PublicKey(followee.to_hex()),
        }
    }

    pub fn follower(&self) -> &PublicKey {
        &self.follower
    }

    pub fn followee(&self) -> &PublicKey {
        &self.followee
    }

    pub fn friendly_follower(&self) -> &FriendlyId {
        &self.friendly_follower
    }

    pub fn set_friendly_follower(&mut self, name: FriendlyId) {
        self.friendly_follower = name;
    }

    pub fn friendly_followee(&self) -> &FriendlyId {
        &self.friendly_followee
    }

    pub fn set_friendly_followee(&mut self, name: FriendlyId) {
        self.friendly_followee = name;
    }

    pub fn is_notifiable(&self) -> bool {
        matches!(self.change_type, ChangeType::Followed)
    }

    pub fn is_older_than(&self, other: &Self) -> bool {
        assert!(self.follower == other.follower);
        assert!(self.followee == other.followee);

        self.followed_at < other.followed_at
    }

    pub fn is_reverse_of(&self, other: &Self) -> bool {
        assert!(self.follower == other.follower);
        assert!(self.followee == other.followee);

        self.change_type != other.change_type
    }

    #[cfg(test)]
    pub fn with_friendly_follower(mut self, name: FriendlyId) -> Self {
        self.friendly_follower = name;
        self
    }
}

impl fmt::Display for FollowChange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: {}({}) -> {}({}) at {}",
            match self.change_type {
                ChangeType::Followed => "Followed",
                ChangeType::Unfollowed => "Unfollowed",
            },
            self.follower,
            self.friendly_follower,
            self.followee,
            self.friendly_followee,
            self.followed_at.to_rfc3339(),
        )
    }
}

impl fmt::Debug for FollowChange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}{} at {}",
            &self.follower.to_hex()[..3],
            match self.change_type {
                ChangeType::Followed => "--->",
                ChangeType::Unfollowed => "-x->",
            },
            &self.followee.to_hex()[..3],
            self.followed_at.to_rfc3339(),
        )
    }
}
