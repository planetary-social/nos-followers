use crate::domain::follow::Follow;
use ::time::{OffsetDateTime, PrimitiveDateTime};
use nostr_sdk::prelude::*;
use sqlx::postgres::Postgres;
use sqlx::Transaction;
use tracing::debug;

pub struct Repo {
    pool: sqlx::PgPool,
}

impl Repo {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    pub async fn upsert_follow(
        &self,
        follow: &Follow,
        maybe_tx: Option<Transaction<'static, Postgres>>,
    ) -> Result<()> {
        let mut tx = match maybe_tx {
            Some(tx) => tx,
            None => self.pool.begin().await?,
        };

        let updated_at =
            OffsetDateTime::from_unix_timestamp(follow.updated_at.as_u64() as i64).unwrap();
        let updated_at_dt = PrimitiveDateTime::new(updated_at.date(), updated_at.time());
        let followee_hex = follow.followee.to_hex();
        let follower_hex = follow.follower.to_hex();

        debug!(
            "Upserting follow: followee={}, follower={}, updated_at={}",
            followee_hex, follower_hex, updated_at_dt
        );
        sqlx::query(
            r#"
            INSERT INTO follows (followee, follower, updated_at, created_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (followee, follower) DO UPDATE SET updated_at = $3
            "#,
        )
        .bind(followee_hex)
        .bind(follower_hex)
        .bind(updated_at_dt)
        .bind(updated_at_dt)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn delete_follow(
        &self,
        followee: &PublicKey,
        follower: &PublicKey,
        maybe_tx: Option<Transaction<'static, Postgres>>,
    ) -> Result<()> {
        let mut tx = match maybe_tx {
            Some(tx) => tx,
            None => self.pool.begin().await?,
        };

        let followee_hex = followee.to_hex();
        let follower_hex = follower.to_hex();

        debug!(
            "Deleting follow: followee={}, follower={}",
            followee_hex, follower_hex
        );
        sqlx::query(
            r#"
            DELETE FROM follows
            WHERE followee = $1 AND follower = $2
            "#,
        )
        .bind(followee_hex)
        .bind(follower_hex)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn get_follows(&self, follower: &PublicKey) -> Result<Vec<Follow>> {
        let follower_hex = follower.to_hex();
        let follows = sqlx::query_as::<_, Follow>(
            r#"
            SELECT followee, follower, updated_at, created_at
            FROM follows
            WHERE follower = $1
            "#,
        )
        .bind(follower_hex)
        .fetch_all(&self.pool)
        .await?;

        Ok(follows)
    }
}
