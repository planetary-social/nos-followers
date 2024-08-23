use chrono::{DateTime, Utc};
use nostr_sdk::prelude::*;

#[derive(Debug, Clone)]
pub struct Follow {
    pub followee: PublicKey,
    pub follower: PublicKey,
    pub updated_at: DateTime<Utc>,
    #[allow(unused)]
    pub created_at: DateTime<Utc>,
}
