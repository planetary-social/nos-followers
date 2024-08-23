use core::panic;

use crate::domain::follow::Follow;
use chrono::{DateTime, Utc};
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

pub trait RepoTrait {
    async fn get_friendly_id(&self, _public_key: &PublicKey) -> Result<Option<String>, RepoError> {
        panic!("Not implemented")
    }
    async fn add_friendly_id(
        &self,
        _public_key: &PublicKey,
        _friendly_id: &str,
    ) -> Result<(), RepoError> {
        panic!("Not implemented")
    }
    fn upsert_follow(
        &self,
        _follow: &Follow,
    ) -> impl std::future::Future<Output = Result<(), RepoError>> + std::marker::Send {
        async { panic!("Not implemented") }
    }

    fn delete_follow(
        &self,
        _followee: &PublicKey,
        _follower: &PublicKey,
    ) -> impl std::future::Future<Output = Result<(), RepoError>> + std::marker::Send {
        async { panic!("Not implemented") }
    }
    fn get_follows(
        &self,
        _follower: &PublicKey,
    ) -> impl std::future::Future<Output = Result<Vec<Follow>, RepoError>> + std::marker::Send {
        async { panic!("Not implemented") }
    }
}

impl RepoTrait for Repo {
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
            let friendly_id = row
                .get::<String>("friendly_id")
                .map_err(RepoError::Deserialization)?;
            Ok(Some(friendly_id))
        } else {
            Ok(None)
        }
    }

    async fn add_friendly_id(
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
            .map_err(RepoError::AddFriendlyId)?;

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
            .param("updated_at", follow.updated_at.naive_utc())
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
            let followee = row
                .get::<String>("followee")
                .map_err(RepoError::Deserialization)?;
            let follower = row
                .get::<String>("follower")
                .map_err(RepoError::Deserialization)?;
            let updated_at = row
                .get::<DateTime<Utc>>("updated_at")
                .map_err(RepoError::Deserialization)?;
            let created_at = row
                .get::<DateTime<Utc>>("created_at")
                .map_err(RepoError::Deserialization)?;

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

#[derive(Error, Debug)]
pub enum RepoError {
    #[error("Failed to add friendly_id: {0}")]
    AddFriendlyId(neo4rs::Error),
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
    #[error("Failed to deserialize: {0}")]
    Deserialization(neo4rs::DeError),
}
