use crate::domain::follow::Follow;
use anyhow::Result;
use chrono::{DateTime, FixedOffset};
use neo4rs::{query, Graph};
use nostr_sdk::prelude::PublicKey;

pub struct Repo {
    graph: Graph,
}

impl Repo {
    pub fn new(graph: Graph) -> Self {
        Self { graph }
    }

    pub async fn add_friendly_id(&self, public_key: &PublicKey, friendly_id: &str) -> Result<()> {
        let statement = r#"
            MERGE (user:User {pubkey: $pubkey_val})
            ON CREATE SET user.friendly_id = $friendly_id_val
            ON MATCH SET user.friendly_id = $friendly_id_val
            "#;

        let query = query(statement)
            .param("pubkey_val", public_key.to_hex())
            .param("friendly_id_val", friendly_id);

        self.graph.run(query).await?;

        Ok(())
    }

    pub async fn upsert_follow(&self, follow: &Follow) -> Result<()> {
        let statement = r#"
            MERGE (followee:User {pubkey: $followee_val})
            MERGE (follower:User {pubkey: $follower_val})
            MERGE (follower)-[r:FOLLOWS]->(followee)
            ON CREATE SET r.created_at = $updated_at, r.updated_at = $updated_at
            ON MATCH SET r.updated_at = $updated_at
            "#;

        let query = query(statement)
            .param("updated_at", follow.updated_at)
            .param("followee_val", follow.followee.to_hex())
            .param("follower_val", follow.follower.to_hex());

        self.graph.run(query).await?;

        Ok(())
    }

    pub async fn delete_follow(&self, followee: &PublicKey, follower: &PublicKey) -> Result<()> {
        let statement = r#"
            MATCH (follower:User {pubkey: $follower_val})-[r:FOLLOWS]->(followee:User {pubkey: $followee_val})
            DELETE r
            "#;

        let query = query(statement)
            .param("followee_val", followee.to_hex())
            .param("follower_val", follower.to_hex());

        self.graph.run(query).await?;
        Ok(())
    }

    pub async fn get_follows(&self, follower: &PublicKey) -> Result<Vec<Follow>> {
        let statement = r#"
            MATCH (follower:User {pubkey: $follower_val})-[r:FOLLOWS]->(followee:User)
            RETURN followee.pubkey AS followee, follower.pubkey AS follower, r.created_at AS created_at, r.updated_at AS updated_at
            "#;

        let query = query(statement).param("follower_val", follower.to_hex());

        let mut records = self.graph.execute(query).await?;

        let mut follows: Vec<Follow> = Vec::new();
        while let Ok(Some(row)) = records.next().await {
            let followee = row.get::<String>("followee")?;
            let follower = row.get::<String>("follower")?;
            let updated_at = row.get::<DateTime<FixedOffset>>("updated_at")?;
            let created_at = row.get::<DateTime<FixedOffset>>("created_at")?;

            follows.push(Follow {
                followee: PublicKey::from_hex(&followee)?,
                follower: PublicKey::from_hex(&follower)?,
                updated_at,
                created_at,
            });
        }

        Ok(follows)
    }
}
