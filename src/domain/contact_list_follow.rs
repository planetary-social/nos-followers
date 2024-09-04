use chrono::{DateTime, Utc};
use nostr_sdk::prelude::*;

/// Used as the DB record. It will be stored as the corresponding nodes and edge
#[derive(Debug, Clone)]
pub struct ContactListFollow {
    pub followee: PublicKey,
    pub follower: PublicKey,
    pub updated_at: DateTime<Utc>, // A new contact list that keeps the followee will just update this value, not the created_at
    #[allow(unused)]
    pub created_at: DateTime<Utc>, // It is stored, currently unused
                                   // TODO fetch friendly id here too
                                   // TODO fetch last_contact_list_at too
}
