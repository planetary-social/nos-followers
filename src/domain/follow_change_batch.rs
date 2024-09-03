use super::{ChangeType, FollowChange};
use crate::account_info::FriendlyId;
use nostr_sdk::prelude::*;
use ordermap::OrderSet;
use serde::ser::SerializeSeq;
use serde::{Serialize, Serializer};
use std::fmt::Debug;

// This is the maximum total of followers and unfollowers we can have in a single message based on the APNS limit of 4096 bytes.
// See tests done to discover this number in the notifications server:
// https://github.com/planetary-social/nos-notification-service-go/blob/4728744c6125909375478ec5ddae5934f1d7e1f7/service/adapters/apns/apns_test.go#L162-L243
pub const MAX_FOLLOWERS_PER_BATCH: usize = 58;

/// An serializable message containing follow changes for a single followee.
#[derive(Clone, Serialize, Eq, PartialEq, Ord, PartialOrd)]
#[serde(rename_all = "camelCase")]
pub struct FollowChangeBatch {
    #[serde(serialize_with = "serialize_as_vec_of_npubs")]
    follows: OrderSet<PublicKey>,
    #[serde(serialize_with = "serialize_as_vec_of_npubs")]
    unfollows: OrderSet<PublicKey>,
    #[serde(serialize_with = "serialize_as_npub")]
    followee: PublicKey,
    friendly_follower: Option<FriendlyId>,
}

impl FollowChangeBatch {
    pub fn new(followee: PublicKey) -> Self {
        Self {
            follows: OrderSet::new(),
            unfollows: OrderSet::new(),
            friendly_follower: None,
            followee,
        }
    }

    pub fn follows(&self) -> &OrderSet<PublicKey> {
        &self.follows
    }

    pub fn unfollows(&self) -> &OrderSet<PublicKey> {
        &self.unfollows
    }

    pub fn followee(&self) -> &PublicKey {
        &self.followee
    }

    pub fn add(&mut self, follow_change: FollowChange) {
        assert!(self.followee == follow_change.followee, "Followee mismatch");

        if follow_change.change_type == ChangeType::Followed {
            self.follows.insert(follow_change.follower);
        } else {
            self.unfollows.insert(follow_change.follower);
        }

        if self.len() == 1 {
            self.friendly_follower = Some(follow_change.friendly_follower);
        } else {
            self.friendly_follower = None;
        }
    }

    pub fn is_empty(&self) -> bool {
        self.follows.is_empty() && self.unfollows.is_empty()
    }

    pub fn len(&self) -> usize {
        self.follows.len() + self.unfollows.len()
    }
}

fn serialize_as_vec_of_npubs<S>(
    pubkeys: &OrderSet<PublicKey>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut seq = serializer.serialize_seq(Some(pubkeys.len()))?;

    for pk in pubkeys {
        let npub = pk
            .to_bech32()
            .map_err(|_| serde::ser::Error::custom("Failed to serialize to npub"))?;

        seq.serialize_element(&npub)?;
    }

    seq.end()
}

fn serialize_as_npub<S>(pk: &PublicKey, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let npub = pk
        .to_bech32()
        .map_err(|_| serde::ser::Error::custom("Failed to serialize to npub"))?;

    serializer.serialize_str(&npub)
}

impl Debug for FollowChangeBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FollowChangeBatch")
            .field(
                "follows",
                &self
                    .follows
                    .iter()
                    .map(|pk| pk.to_hex().chars().take(10).collect::<String>())
                    .collect::<Vec<String>>(),
            )
            .field(
                "unfollows",
                &self
                    .unfollows
                    .iter()
                    .map(|pk| pk.to_hex().chars().take(10).collect::<String>())
                    .collect::<Vec<String>>(),
            )
            .field(
                "followee",
                &self.followee.to_hex().chars().take(10).collect::<String>(),
            )
            .field("friendly_follower", &self.friendly_follower)
            .finish()
    }
}

impl From<FollowChange> for FollowChangeBatch {
    fn from(val: FollowChange) -> Self {
        let mut message = FollowChangeBatch::new(val.followee);
        message.add(val);
        message
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use nostr_sdk::prelude::Keys;

    #[test]
    fn test_follow_change_message_with_one_follower() {
        let followee1 = Keys::generate().public_key();
        let follower1 = Keys::generate().public_key();

        let follower1_follow = FollowChange::new_followed(Utc::now(), follower1, followee1)
            .with_friendly_follower(FriendlyId::Name("Alice".to_string()));

        let mut message = FollowChangeBatch::new(followee1);

        message.add(follower1_follow);

        assert_eq!(
            serde_json::to_string(&message).unwrap(),
            format!(
                r#"{{"follows":["{}"],"unfollows":[],"followee":"{}","friendlyFollower":"Alice"}}"#,
                follower1.to_bech32().unwrap(), // Follow
                followee1.to_bech32().unwrap(),
            )
        );
    }
    #[test]
    fn test_follow_change_message_with_many_followers() {
        let followee1 = Keys::generate().public_key();
        let followee2 = Keys::generate().public_key();

        let follower1 = Keys::generate().public_key();
        let follower2 = Keys::generate().public_key();
        let follower3 = Keys::generate().public_key();
        let follower4 = Keys::generate().public_key();

        let follower1_follow = FollowChange::new_followed(Utc::now(), follower1, followee1)
            .with_friendly_follower(FriendlyId::Name("Alice".to_string()));
        let follower2_follow = FollowChange::new_followed(Utc::now(), follower2, followee1);
        let follower2_follow2 = FollowChange::new_followed(Utc::now(), follower2, followee1);
        let follower3_follow = FollowChange::new_followed(Utc::now(), follower3, followee1);
        let follower4_unfollow = FollowChange::new_unfollowed(Utc::now(), follower4, followee1);
        let wrong_followee_change = FollowChange::new_followed(Utc::now(), follower4, followee2);

        let mut message = FollowChangeBatch::new(followee1);

        message.add(follower1_follow);
        message.add(follower2_follow);
        message.add(follower2_follow2);
        message.add(follower3_follow);
        message.add(follower4_unfollow);

        // TODO: This panics on github CI, but not locally. Investigate.
        #[cfg(not(feature = "ci"))]
        {
            let result = std::panic::catch_unwind(|| {
                FollowChangeBatch::new(followee1).add(wrong_followee_change)
            });
            assert!(result.is_err());
        }

        assert_eq!(
            serde_json::to_string(&message).unwrap(),
            format!(
                r#"{{"follows":["{}","{}","{}"],"unfollows":["{}"],"followee":"{}","friendlyFollower":null}}"#,
                follower1.to_bech32().unwrap(), // Follow
                follower2.to_bech32().unwrap(), // Follow
                follower3.to_bech32().unwrap(), // Follow
                follower4.to_bech32().unwrap(), // Unfollow
                followee1.to_bech32().unwrap(),
            )
        );
    }
}
