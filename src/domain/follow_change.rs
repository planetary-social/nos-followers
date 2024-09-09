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
    pub change_type: ChangeType,
    pub followed_at: DateTime<Utc>,
    pub follower: PublicKey,
    pub friendly_follower: FriendlyId,
    pub followee: PublicKey,
    pub friendly_followee: FriendlyId,
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
