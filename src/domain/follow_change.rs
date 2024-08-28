use chrono::{DateTime, Utc};
use nostr_sdk::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
pub enum ChangeType {
    Followed,
    Unfollowed,
}

#[derive(Clone, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
pub struct FollowChange {
    pub change_type: ChangeType,
    pub at: DateTime<Utc>,
    pub previous_at: Option<DateTime<Utc>>,
    pub follower: PublicKey,
    pub friendly_follower: Option<String>,
    pub followee: PublicKey,
    pub friendly_followee: Option<String>,
}
impl PartialEq for FollowChange {
    fn eq(&self, other: &Self) -> bool {
        self.change_type == other.change_type
            && self.at == other.at
            && self.follower == other.follower
            && self.followee == other.followee
    }
}

impl Eq for FollowChange {}

impl FollowChange {
    pub fn new_followed(at: DateTime<Utc>, follower: PublicKey, followee: PublicKey) -> Self {
        Self {
            change_type: ChangeType::Followed,
            at,
            previous_at: None,
            follower,
            friendly_follower: None,
            followee,
            friendly_followee: None,
        }
    }

    pub fn new_unfollowed(at: DateTime<Utc>, follower: PublicKey, followee: PublicKey) -> Self {
        Self {
            change_type: ChangeType::Unfollowed,
            at,
            previous_at: None,
            follower,
            friendly_follower: None,
            followee,
            friendly_followee: None,
        }
    }

    pub fn with_last_seen_contact_list_at(mut self, maybe_at: Option<DateTime<Utc>>) -> Self {
        self.previous_at = maybe_at;
        self
    }

    pub fn with_friendly_follower(mut self, name: String) -> Self {
        self.friendly_follower = Some(name);
        self
    }

    pub fn with_friendly_followee(mut self, name: String) -> Self {
        self.friendly_followee = Some(name);
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
            self.friendly_follower
                .clone()
                .unwrap_or_else(|| "N/A".to_string()),
            self.followee,
            self.friendly_followee
                .clone()
                .unwrap_or_else(|| "N/A".to_string()),
            self.at.to_rfc3339(),
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
            self.at,
        )
    }
}
