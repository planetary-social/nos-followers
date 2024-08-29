use core::panic;

use crate::domain::follow::Follow;
use chrono::{DateTime, NaiveDateTime, Utc};
use neo4rs::{query, Graph};
use nostr_sdk::prelude::PublicKey;
use thiserror::Error;

pub struct Repo {
    graph: Graph,
}

impl Repo {
    pub fn new(graph: Graph) -> Self {
        Self { graph }
    }
}

// Default trait raises not implemented just to ease testing
pub trait RepoTrait {
    /// Set the last contact list date seen for a user if it's newer than the stored value. Returns the previous value
    fn maybe_update_last_contact_list_at(
        &self,
        _public_key: &PublicKey,
        _at: &DateTime<Utc>,
    ) -> impl std::future::Future<Output = Result<Option<DateTime<Utc>>, RepoError>> + std::marker::Send
    {
        async { panic!("Not implemented") }
    }

    /// Get the friendly_id for a user
    fn get_friendly_id(
        &self,
        _public_key: &PublicKey,
    ) -> impl std::future::Future<Output = Result<Option<String>, RepoError>> + std::marker::Send
    {
        async { panic!("Not implemented") }
    }

    /// Set the friendly_id for a user
    fn set_friendly_id(
        &self,
        _public_key: &PublicKey,
        _friendly_id: &str,
    ) -> impl std::future::Future<Output = Result<(), RepoError>> + std::marker::Send {
        async { panic!("Not implemented") }
    }

    /// Upsert a follow relationship
    fn upsert_follow(
        &self,
        _follow: &Follow,
    ) -> impl std::future::Future<Output = Result<(), RepoError>> + std::marker::Send {
        async { panic!("Not implemented") }
    }

    /// Delete a follow relationship
    fn delete_follow(
        &self,
        _followee: &PublicKey,
        _follower: &PublicKey,
    ) -> impl std::future::Future<Output = Result<(), RepoError>> + std::marker::Send {
        async { panic!("Not implemented") }
    }

    /// Get all follows for a user
    fn get_follows(
        &self,
        _follower: &PublicKey,
    ) -> impl std::future::Future<Output = Result<Vec<Follow>, RepoError>> + std::marker::Send {
        async { panic!("Not implemented") }
    }
}

impl RepoTrait for Repo {
    /// Set the last contact list date seen for a user if it's newer than the stored value.
    /// Returns `Some(previous_value)` if there was a previous value, or `None` if there was no previous value.
    async fn maybe_update_last_contact_list_at(
        &self,
        public_key: &PublicKey,
        at: &DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>, RepoError> {
        let statement = r#"
            MERGE (user:User {pubkey: $pubkey_val})
            WITH user, user.last_contact_list_at AS previous_value
            SET user.last_contact_list_at = CASE
                WHEN previous_value IS NULL OR previous_value < $last_contact_list_at
                THEN $last_contact_list_at
                ELSE previous_value
            END
            RETURN previous_value
            "#;

        let query = query(statement)
            .param("pubkey_val", public_key.to_hex())
            .param("last_contact_list_at", (at.naive_utc(), "Etc/UTC"));

        let mut records = self
            .graph
            .execute(query)
            .await
            .map_err(RepoError::MaybeSetLastContactListAt)?;

        if let Some(row) = records
            .next()
            .await
            .map_err(RepoError::MaybeSetLastContactListAt)?
        {
            let previous_value = parse_datetime(&row, "previous_value")?;
            Ok(previous_value)
        } else {
            Ok(None)
        }
    }

    async fn get_friendly_id(&self, public_key: &PublicKey) -> Result<Option<String>, RepoError> {
        let statement = r#"
            MATCH (user:User {pubkey: $pubkey_val})
            RETURN user.friendly_id AS friendly_id
            "#;

        let query = query(statement).param("pubkey_val", public_key.to_hex());

        let mut records = self
            .graph
            .execute(query)
            .await
            .map_err(RepoError::GetFriendlyId)?;

        if let Ok(Some(row)) = records.next().await {
            let friendly_id = row.get::<String>("friendly_id").map_err(|e| {
                RepoError::deserialization_with_context(e, "deserializing 'friendly_id' field")
            })?;
            Ok(Some(friendly_id))
        } else {
            Ok(None)
        }
    }

    async fn set_friendly_id(
        &self,
        public_key: &PublicKey,
        friendly_id: &str,
    ) -> Result<(), RepoError> {
        let statement = r#"
            MERGE (user:User {pubkey: $pubkey_val})
            ON CREATE SET user.friendly_id = $friendly_id_val
            ON MATCH SET user.friendly_id = $friendly_id_val
            "#;

        let query = query(statement)
            .param("pubkey_val", public_key.to_hex())
            .param("friendly_id_val", friendly_id);

        self.graph
            .run(query)
            .await
            .map_err(RepoError::SetFriendlyId)?;

        Ok(())
    }

    async fn upsert_follow(&self, follow: &Follow) -> Result<(), RepoError> {
        let statement = r#"
            MERGE (followee:User {pubkey: $followee_val})
            MERGE (follower:User {pubkey: $follower_val})
            MERGE (follower)-[r:FOLLOWS]->(followee)
            ON CREATE SET r.created_at = $updated_at, r.updated_at = $updated_at
            ON MATCH SET r.updated_at = $updated_at
            "#;

        let query = query(statement)
            .param("updated_at", (follow.updated_at.naive_utc(), "Etc/UTC"))
            .param("followee_val", follow.followee.to_hex())
            .param("follower_val", follow.follower.to_hex());

        self.graph
            .run(query)
            .await
            .map_err(RepoError::UpsertFollow)?;

        Ok(())
    }

    async fn delete_follow(
        &self,
        followee: &PublicKey,
        follower: &PublicKey,
    ) -> Result<(), RepoError> {
        let statement = r#"
            MATCH (follower:User {pubkey: $follower_val})-[r:FOLLOWS]->(followee:User {pubkey: $followee_val})
            DELETE r
            "#;

        let query = query(statement)
            .param("followee_val", followee.to_hex())
            .param("follower_val", follower.to_hex());

        self.graph
            .run(query)
            .await
            .map_err(RepoError::DeleteFollow)?;
        Ok(())
    }

    async fn get_follows(&self, follower: &PublicKey) -> Result<Vec<Follow>, RepoError> {
        let statement = r#"
            MATCH (follower:User {pubkey: $follower_val})-[r:FOLLOWS]->(followee:User)
            RETURN followee.pubkey AS followee, follower.pubkey AS follower, r.created_at AS created_at, r.updated_at AS updated_at
            "#;

        let query = query(statement).param("follower_val", follower.to_hex());

        let mut records = self
            .graph
            .execute(query)
            .await
            .map_err(RepoError::GetFollows)?;

        let mut follows: Vec<Follow> = Vec::new();
        while let Ok(Some(row)) = records.next().await {
            let followee = row.get::<String>("followee").map_err(|e| {
                RepoError::deserialization_with_context(e, "deserializing 'followee' field")
            })?;
            let follower = row.get::<String>("follower").map_err(|e| {
                RepoError::deserialization_with_context(e, "deserializing 'follower' field")
            })?;
            let Some(updated_at) = parse_datetime(&row, "updated_at")? else {
                return Err(RepoError::GetFollows(neo4rs::Error::DeserializationError(
                    neo4rs::DeError::PropertyMissingButRequired,
                )));
            };
            let Some(created_at) = parse_datetime(&row, "created_at")? else {
                return Err(RepoError::GetFollows(neo4rs::Error::DeserializationError(
                    neo4rs::DeError::PropertyMissingButRequired,
                )));
            };

            follows.push(Follow {
                followee: PublicKey::from_hex(&followee).map_err(RepoError::GetFollowsPubkey)?,
                follower: PublicKey::from_hex(&follower).map_err(RepoError::GetFollowsPubkey)?,
                updated_at,
                created_at,
            });
        }

        Ok(follows)
    }
}

/// A function to read as DateTime<Utc> a value stored either as LocalDatetime or DateTime<Utc>
fn parse_datetime(row: &neo4rs::Row, field: &str) -> Result<Option<DateTime<Utc>>, RepoError> {
    row.get::<Option<DateTime<Utc>>>(field)
        .or_else(|_| {
            row.get::<Option<NaiveDateTime>>(field)
                .map(|naive| naive.map(|n| n.and_utc()))
        })
        .map_err(|e| {
            RepoError::deserialization_with_context(e, format!("deserializing '{}' field", field))
        })
}

#[derive(Error, Debug)]
pub enum RepoError {
    #[error("Failed to set first contact list date: {0}")]
    MaybeSetLastContactListAt(neo4rs::Error),
    #[error("Failed to add friendly_id: {0}")]
    SetFriendlyId(neo4rs::Error),
    #[error("Failed to upsert follow: {0}")]
    UpsertFollow(neo4rs::Error),
    #[error("Failed to delete follow: {0}")]
    DeleteFollow(neo4rs::Error),
    #[error("Failed to get follows: {0}")]
    GetFollows(neo4rs::Error),
    #[error("Failed to get follows: {0}")]
    GetFollowsPubkey(nostr_sdk::key::Error),
    #[error("Failed to get friendly_id: {0}")]
    GetFriendlyId(neo4rs::Error),
    #[error("Failed to deserialize: {source} ({context})")]
    Deserialization {
        source: neo4rs::DeError,
        context: String,
    },
}
impl RepoError {
    pub fn deserialization_with_context<S: Into<String>>(
        source: neo4rs::DeError,
        context: S,
    ) -> Self {
        RepoError::Deserialization {
            source,
            context: context.into(),
        }
    }
}
