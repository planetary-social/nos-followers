use chrono::{DateTime, Utc};
use metrics::counter;
use nostr_sdk::prelude::*;
use serde::{Deserialize, Serialize, Serializer};
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
    #[serde(serialize_with = "serialize_at_as_i64")]
    pub at: DateTime<Utc>,
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
        counter!("follows").increment(1);

        Self {
            change_type: ChangeType::Followed,
            at,
            follower,
            friendly_follower: None,
            followee,
            friendly_followee: None,
        }
    }

    pub fn new_unfollowed(at: DateTime<Utc>, follower: PublicKey, followee: PublicKey) -> Self {
        counter!("unfollows").increment(1);

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

fn serialize_at_as_i64<S>(at: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_i64(at.timestamp())
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
