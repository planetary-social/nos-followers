use ::time::PrimitiveDateTime;
use nostr_sdk::prelude::*;
use sqlx::{postgres::PgRow, FromRow, Row};

#[derive(Debug, Clone)]
pub struct Follow {
    pub followee: PublicKey,
    pub follower: PublicKey,
    pub updated_at: Timestamp,
    #[allow(unused)]
    pub created_at: Timestamp,
}

impl FromRow<'_, PgRow> for Follow {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        let followee_str: String = row.try_get("followee")?;
        let follower_str: String = row.try_get("follower")?;
        let updated_at: PrimitiveDateTime = row.try_get("updated_at")?;
        let created_at: PrimitiveDateTime = row.try_get("created_at")?;

        Ok(Self {
            followee: PublicKey::from_hex(&followee_str).unwrap(),
            follower: PublicKey::from_hex(&follower_str).unwrap(),
            updated_at: (updated_at.assume_utc().unix_timestamp() as u64).into(),
            created_at: (created_at.assume_utc().unix_timestamp() as u64).into(),
        })
    }
}
