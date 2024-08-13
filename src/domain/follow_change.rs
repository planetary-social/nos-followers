use nostr_sdk::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ChangeType {
    Followed,
    Unfollowed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FollowChange {
    pub change_type: ChangeType,
    pub at: Timestamp,
    pub follower: PublicKey,
    pub friendly_follower: Option<String>,
    pub followee: PublicKey,
    pub friendly_followee: Option<String>,
}

impl FollowChange {
    pub fn new_followed(at: Timestamp, follower: PublicKey, followee: PublicKey) -> Self {
        Self {
            change_type: ChangeType::Followed,
            at,
            follower,
            friendly_follower: None,
            followee,
            friendly_followee: None,
        }
    }

    pub fn new_unfollowed(at: Timestamp, follower: PublicKey, followee: PublicKey) -> Self {
        Self {
            change_type: ChangeType::Unfollowed,
            at,
            follower,
            friendly_follower: None,
            followee,
            friendly_followee: None,
        }
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
            self.at.to_human_datetime()
        )
    }
}
