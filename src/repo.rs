use crate::domain::{AccountInfo, ContactListFollow, FriendlyId};
use chrono::{DateTime, NaiveDateTime, Utc};
use core::panic;
use neo4rs::{query, Graph};
use nostr_sdk::prelude::PublicKey;
use serde::Serialize;
use thiserror::Error;
use tracing::{error, info};

pub struct Repo {
    graph: Graph,
}

impl Repo {
    pub fn new(graph: Graph) -> Self {
        Self { graph }
    }
}

// Default trait raises not implemented just to ease testing
pub trait RepoTrait: Sync + Send + 'static {
    /// Set the last contact list date seen for a user if it's newer than the stored value
    fn update_last_contact_list_at(
        &self,
        _public_key: &PublicKey,
        _at: &DateTime<Utc>,
    ) -> impl std::future::Future<Output = Result<(), RepoError>> + std::marker::Send {
        async { panic!("Not implemented") }
    }

    /// Check if a user is trusted based on their account information
    fn check_if_trusted(
        &self,
        _public_key: &PublicKey,
    ) -> impl std::future::Future<Output = Result<bool, RepoError>> + std::marker::Send {
        async { panic!("Not implemented") }
    }

    /// Get the friendly_id for a user
    fn get_friendly_id(
        &self,
        _public_key: &PublicKey,
    ) -> impl std::future::Future<Output = Result<Option<FriendlyId>, RepoError>> + std::marker::Send
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
        _follow: &ContactListFollow,
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
    ) -> impl std::future::Future<Output = Result<Vec<ContactListFollow>, RepoError>> + std::marker::Send
    {
        async { panic!("Not implemented") }
    }

    /// Refreshes the in-memory graph for users with more than one followee and at least one follower.
    fn update_memory_graph(
        &self,
    ) -> impl std::future::Future<Output = Result<(), RepoError>> + std::marker::Send {
        async { panic!("Not implemented") }
    }

    /// Filters users, calculates pagerank, sets the value to each node, and drops the temporary graph.
    fn update_pagerank(
        &self,
    ) -> impl std::future::Future<Output = Result<(), RepoError>> + std::marker::Send {
        async { panic!("Not implemented") }
    }

    fn log_neo4j_details(
        &self,
    ) -> impl std::future::Future<Output = Result<(), RepoError>> + std::marker::Send {
        async { panic!("Not implemented") }
    }

    fn get_recommendations(
        &self,
        _pubkey: &PublicKey,
    ) -> impl std::future::Future<Output = Result<Vec<Recommendation>, RepoError>> + std::marker::Send
    {
        async { panic!("Not implemented") }
    }

    fn get_account_info(
        &self,
        _public_key: &PublicKey,
    ) -> impl std::future::Future<Output = Result<Option<AccountInfo>, RepoError>> + std::marker::Send
    {
        async { panic!("Not implemented") }
    }

    fn remove_pubkey(
        &self,
        _public_key: &PublicKey,
    ) -> impl std::future::Future<Output = Result<(), RepoError>> + std::marker::Send {
        async { panic!("Not implemented") }
    }
}

impl RepoTrait for Repo {
    /// Set the last contact list date seen for a user if it's newer than the stored value.
    async fn update_last_contact_list_at(
        &self,
        public_key: &PublicKey,
        at: &DateTime<Utc>,
    ) -> Result<(), RepoError> {
        let statement = r#"
            MERGE (user:User {pubkey: $pubkey_val})
            SET user.last_contact_list_at = CASE
                WHEN user.last_contact_list_at IS NULL OR user.last_contact_list_at < $last_contact_list_at
                THEN $last_contact_list_at
                ELSE user.last_contact_list_at
            END
            "#;

        let query = query(statement)
            .param("pubkey_val", public_key.to_hex())
            .param("last_contact_list_at", (at.naive_utc(), "Etc/UTC"));

        self.graph
            .run(query)
            .await
            .map_err(RepoError::MaybeSetLastContactListAt)?;

        Ok(())
    }

    async fn get_friendly_id(
        &self,
        public_key: &PublicKey,
    ) -> Result<Option<FriendlyId>, RepoError> {
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
            Ok(Some(FriendlyId::DB(friendly_id)))
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

    async fn upsert_follow(&self, follow: &ContactListFollow) -> Result<(), RepoError> {
        // Notice we only increment follower_count and followee_count if they
        // are not null.  This allows an external script to initialize the
        // counts with fresh, non-cached values.
        let statement = r#"
            // Ensure the followee exists and initialize follower_count to 0 if created
            MERGE (followee:User {pubkey: $followee_val})
            ON CREATE SET followee.follower_count = 0

            // Ensure the follower exists and initialize followee_count to 0 if created
            MERGE (follower:User {pubkey: $follower_val})
            ON CREATE SET follower.followee_count = 0

            // Create the FOLLOWS relationship between follower and followee
            MERGE (follower)-[r:FOLLOWS]->(followee)
            ON CREATE SET
                r.created_at = $updated_at,
                r.updated_at = $updated_at,
                followee.follower_count = CASE
                    WHEN followee.follower_count IS NOT NULL THEN followee.follower_count + 1
                    ELSE followee.follower_count
                END,
                follower.followee_count = CASE
                    WHEN follower.followee_count IS NOT NULL THEN follower.followee_count + 1
                    ELSE follower.followee_count
                END
            ON MATCH SET
                r.updated_at = $updated_at
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
            WITH followee, follower
            SET followee.follower_count = CASE
                WHEN followee.follower_count > 0 THEN followee.follower_count - 1
                ELSE 0
            END,
            follower.followee_count = CASE
                WHEN follower.followee_count > 0 THEN follower.followee_count - 1
                ELSE 0
            END
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

    async fn get_follows(&self, follower: &PublicKey) -> Result<Vec<ContactListFollow>, RepoError> {
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

        let mut follows: Vec<ContactListFollow> = Vec::new();
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

            follows.push(ContactListFollow {
                followee: PublicKey::from_hex(&followee).map_err(RepoError::GetFollowsPubkey)?,
                follower: PublicKey::from_hex(&follower).map_err(RepoError::GetFollowsPubkey)?,
                updated_at,
                created_at,
            });
        }

        Ok(follows)
    }

    async fn update_memory_graph(&self) -> Result<(), RepoError> {
        let graph_name = "filteredGraph";

        let statements = r#"
        CALL {
            CALL gds.graph.exists($graph_name)
            YIELD exists
            WITH exists
            WHERE exists
            CALL gds.graph.drop($graph_name)
            YIELD graphName
            RETURN graphName
            UNION ALL
            RETURN $graph_name AS graphName
        }
        WITH graphName
        MATCH (n:User)
        WHERE n.followee_count > 1 AND n.follower_count > 0
        OPTIONAL MATCH (n)-[r:FOLLOWS]->(m:User)
        WHERE m.followee_count > 1 AND m.follower_count > 0
        WITH gds.graph.project(
            graphName,
            n,
            m,
            {
                relationshipType: 'FOLLOWS',
                relationshipProperties: {}
            }
        ) AS graphProjection
        CALL gds.graph.list() YIELD graphName, nodeCount, relationshipCount
        WHERE graphName = $graph_name
        RETURN graphName, nodeCount, relationshipCount;
    "#;

        let q = query(statements).param("graph_name", graph_name);

        let mut graph_list = self.graph.execute(q).await.map_err(RepoError::General)?;

        loop {
            match graph_list.next().await {
                Ok(Some(record)) => {
                    let graph_name: String = record.get("graphName").unwrap_or_default();
                    let node_count: i64 = record.get("nodeCount").unwrap_or(0);
                    let relationship_count: i64 = record.get("relationshipCount").unwrap_or(0);

                    info!(
                        "Graph '{}' created with {} nodes and {} relationships",
                        graph_name, node_count, relationship_count
                    );
                }
                Ok(None) => {
                    info!("No more graph records found.");
                    break;
                }
                Err(e) => {
                    error!("Error retrieving graph list: {:?}", e);
                    break;
                }
            }
        }

        info!("Memory graph updated and verified successfully.");
        Ok(())
    }

    /// Executes the PageRank algorithm on the in-memory graph that was previously created.
    /// It assumes the graph is already projected in memory.
    async fn update_pagerank(&self) -> Result<(), RepoError> {
        let graph_name = "filteredGraph";

        let pagerank_statement = r#"
        CALL gds.pageRank.write(
            $graph_name,
            {
                writeProperty: 'pagerank',
                maxIterations: 20,
                dampingFactor: 0.85
            }
        )
        YIELD nodePropertiesWritten
        RETURN nodePropertiesWritten;
    "#;

        let query = query(pagerank_statement).param("graph_name", graph_name);

        self.graph
            .run(query)
            .await
            .map_err(RepoError::ExecutePageRank)?;

        Ok(())
    }

    async fn log_neo4j_details(&self) -> Result<(), RepoError> {
        let current_db_query = query("CALL dbms.showCurrentUser()");
        let server_info_query = query("CALL dbms.components()");
        let databases_query = query("SHOW DATABASES");

        let mut current_db_result = self
            .graph
            .execute(current_db_query)
            .await
            .map_err(RepoError::General)?;

        if let Ok(Some(record)) = current_db_result.next().await {
            let username: String = record.get("username").unwrap_or_default();
            let roles: Vec<String> = record.get("roles").unwrap_or_default();
            info!("Current Neo4j user: {}, roles: {:?}", username, roles);
        }

        let mut server_info_result = self
            .graph
            .execute(server_info_query)
            .await
            .map_err(RepoError::General)?;

        while let Ok(Some(record)) = server_info_result.next().await {
            let name: String = record.get("name").unwrap_or_default();
            let version: String = record.get("versions").unwrap_or_default();
            info!("Neo4j Component: {}, Version: {}", name, version);
        }

        let mut databases_result = self
            .graph
            .execute(databases_query)
            .await
            .map_err(RepoError::General)?;

        while let Ok(Some(record)) = databases_result.next().await {
            let db_name: String = record.get("name").unwrap_or_default();
            let current_status: String = record.get("currentStatus").unwrap_or_default();
            info!("Database: {}, Status: {}", db_name, current_status);
        }

        Ok(())
    }
    async fn get_recommendations(
        &self,
        pubkey: &PublicKey,
    ) -> Result<Vec<Recommendation>, RepoError> {
        let statement = r#"
            // Step 1: Get valid target nodes
            MATCH (source:User {pubkey: $pubkey_val})
            MATCH (target:User)
            WHERE target.pagerank >= 0.3
            AND NOT EXISTS {
                MATCH (source)-[:FOLLOWS]->(target)
            }
            WITH source, collect(id(target)) AS targetNodeIds

            // Step 2: Run the similarity algorithm using the filtered target nodes
            CALL gds.nodeSimilarity.filtered.stream('filteredGraph', {
                sourceNodeFilter: [id(source)],
                targetNodeFilter: targetNodeIds,
                topK: 10,
                similarityCutoff: 0.1
            })
            YIELD node1, node2, similarity
            WITH gds.util.asNode(node2) AS targetUser, similarity
            RETURN targetUser.pubkey AS target_pubkey,
                   targetUser.friendly_id AS friendly_id,
                   similarity,
                   targetUser.pagerank AS pagerank
            ORDER BY similarity DESC, pagerank DESC;
        "#;

        let query = query(statement).param("pubkey_val", pubkey.to_hex());

        let mut records = self
            .graph
            .execute(query)
            .await
            .map_err(RepoError::GetRecommendations)?;

        let mut recommendations: Vec<Recommendation> = Vec::new();
        while let Ok(Some(row)) = records.next().await {
            let pubkey = row.get::<String>("target_pubkey").map_err(|e| {
                RepoError::deserialization_with_context(e, "deserializing 'target_pubkey' field")
            })?;
            let friendly_id = row.get::<String>("friendly_id").map_err(|e| {
                RepoError::deserialization_with_context(e, "deserializing 'friendly_id' field")
            })?;
            let similarity: f64 = row.get::<f64>("similarity").map_err(|e| {
                RepoError::deserialization_with_context(e, "deserializing 'similarity' field")
            })?;
            let pagerank: f64 = row.get::<f64>("pagerank").map_err(|e| {
                RepoError::deserialization_with_context(e, "deserializing 'pagerank' field")
            })?;

            recommendations.push(Recommendation {
                pubkey: PublicKey::from_hex(&pubkey)
                    .map_err(RepoError::GetRecommendationsPubkey)?,
                friendly_id,
                similarity,
                pagerank,
            });
        }

        Ok(recommendations)
    }

    async fn get_account_info(
        &self,
        public_key: &PublicKey,
    ) -> Result<Option<AccountInfo>, RepoError> {
        let statement = r#"
        MATCH (user:User {pubkey: $pubkey_val})
        RETURN user.pagerank AS pagerank,
               user.follower_count AS follower_count,
               user.followee_count AS followee_count,
               user.last_contact_list_at AS last_contact_list_at,
               user.friendly_id AS friendly_id
        "#;

        let query = query(statement).param("pubkey_val", public_key.to_hex());

        let mut records = self
            .graph
            .execute(query)
            .await
            .map_err(RepoError::GetPageRank)?;

        match records.next().await {
            Ok(Some(row)) => {
                let pagerank = row.get::<Option<f64>>("pagerank").map_err(|e| {
                    RepoError::deserialization_with_context(e, "deserializing 'pagerank' field")
                })?;

                let follower_count = row.get::<Option<u64>>("follower_count").map_err(|e| {
                    RepoError::deserialization_with_context(
                        e,
                        "deserializing 'follower_count' field",
                    )
                })?;

                let followee_count = row.get::<Option<u64>>("followee_count").map_err(|e| {
                    RepoError::deserialization_with_context(
                        e,
                        "deserializing 'followee_count' field",
                    )
                })?;

                let last_contact_list_at = parse_datetime(&row, "last_contact_list_at")?;

                let friendly_id = row
                    .get::<Option<String>>("friendly_id")
                    .map_err(|e| {
                        RepoError::deserialization_with_context(
                            e,
                            "deserializing 'friendly_id' field",
                        )
                    })?
                    .map(FriendlyId::DB);

                let account_info = AccountInfo::new(*public_key)
                    .with_pagerank(pagerank)
                    .with_followee_count(followee_count)
                    .with_follower_count(follower_count)
                    .with_last_contact_list_at(last_contact_list_at)
                    .with_friendly_id(friendly_id);

                Ok(Some(account_info))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(RepoError::General(e)),
        }
    }

    async fn remove_pubkey(&self, public_key: &PublicKey) -> Result<(), RepoError> {
        let statement = r#"
            MATCH (user:User {pubkey: $pubkey_val})

            // Decrement follower_count of followees
            OPTIONAL MATCH (user)-[:FOLLOWS]->(followee:User)
            FOREACH (f IN CASE WHEN followee IS NOT NULL THEN [followee] ELSE [] END |
                SET f.follower_count = CASE
                    WHEN f.follower_count > 0 THEN f.follower_count - 1
                    ELSE 0
                END
            )

            // Decrement followee_count of followers
            WITH user
            OPTIONAL MATCH (follower:User)-[:FOLLOWS]->(user)
            FOREACH (f IN CASE WHEN follower IS NOT NULL THEN [follower] ELSE [] END |
                SET f.followee_count = CASE
                    WHEN f.followee_count > 0 THEN f.followee_count - 1
                    ELSE 0
                END
            )

            WITH user
            DETACH DELETE user
        "#;

        let query = query(statement).param("pubkey_val", public_key.to_hex());

        self.graph
            .run(query)
            .await
            .map_err(RepoError::RemovePubkey)?;

        Ok(())
    }

    async fn check_if_trusted(&self, public_key: &PublicKey) -> Result<bool, RepoError> {
        let maybe_account_info = self.get_account_info(public_key).await?;

        info!(
            "Checking if {}is trusted. Account: {:?}",
            public_key, maybe_account_info
        );

        // If we have account info, use its is_trusted method
        // Otherwise, the account doesn't exist, so it's not trusted
        let trusted = maybe_account_info.map_or(false, |account| account.is_trusted());

        Ok(trusted)
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
    #[error("Failed to set last contact list date: {0}")]
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

    #[error("Failed to create in-memory graph for PageRank: {0}")]
    CreateGraph(neo4rs::Error),

    #[error("Failed to execute PageRank: {0}")]
    ExecutePageRank(neo4rs::Error),

    #[error("Failed to drop in-memory graph: {0}")]
    DropGraph(neo4rs::Error),

    #[error("Failed to drop existing in-memory graph: {0}")]
    DropExistingGraph(neo4rs::Error),

    #[error("Deserialization failed: {source} ({context})")]
    Deserialization {
        source: neo4rs::DeError,
        context: String,
    },

    #[error("Failed to update memory graph: {0}")]
    UpdateMemoryGraph(neo4rs::Error),

    #[error("General error: {0}")]
    General(neo4rs::Error),

    #[error("Failed to get recommendations: {0}")]
    GetRecommendations(neo4rs::Error),

    #[error("Failed to get recommendations pubkey: {0}")]
    GetRecommendationsPubkey(nostr_sdk::key::Error),

    #[error("Failed to get pagerank: {0}")]
    GetPageRank(neo4rs::Error),

    #[error("Failed to remove pubkey: {0}")]
    RemovePubkey(neo4rs::Error),
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

#[derive(Debug, Serialize, Clone)]
pub struct Recommendation {
    pubkey: PublicKey,
    friendly_id: String,
    similarity: f64,
    pagerank: f64,
}
