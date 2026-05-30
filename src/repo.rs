use crate::domain::{AccountInfo, ContactListFollow, FriendlyId};
use chrono::{DateTime, NaiveDateTime, Utc};
use core::panic;
use neo4rs::{query, Graph};
use nostr_sdk::prelude::PublicKey;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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

/// A page of a pubkey's top followers ranked by PageRank (descending), paired
/// with the total number of follower edges for that user. The total is `Some`
/// for any known user (including `Some(0)` when they have no followers) and is
/// `None` only when the user is unknown to the provider.
pub type TopFollowers = (Vec<(PublicKey, f64)>, Option<u64>);

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
        _min_pagerank: Option<f64>,
        _limit: Option<usize>,
    ) -> impl std::future::Future<Output = Result<Vec<Recommendation>, RepoError>> + std::marker::Send
    {
        async { panic!("Not implemented") }
    }

    /// Fetch the stored PageRank for each of the given pubkeys. Pubkeys absent
    /// from the result are unknown to the provider and should be treated as 0.
    fn get_pageranks(
        &self,
        _pubkeys: &[PublicKey],
    ) -> impl std::future::Future<Output = Result<HashMap<PublicKey, f64>, RepoError>> + std::marker::Send
    {
        async { panic!("Not implemented") }
    }

    /// Fetch up to `limit` of a pubkey's followers ranked by PageRank desc, plus
    /// the total number of follower edges (counted from the graph, so a known
    /// user always reports `Some`; `None` only when the user is unknown).
    fn get_top_followers(
        &self,
        _pubkey: &PublicKey,
        _limit: usize,
    ) -> impl std::future::Future<Output = Result<TopFollowers, RepoError>> + std::marker::Send
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
        min_pagerank: Option<f64>,
        limit: Option<usize>,
    ) -> Result<Vec<Recommendation>, RepoError> {
        // Validate and set defaults
        let min_pagerank = min_pagerank.unwrap_or(0.3);
        if min_pagerank < 0.0 {
            return Err(RepoError::InvalidParameter(
                "min_pagerank must be non-negative".into(),
            ));
        }

        let limit = limit.unwrap_or(10).min(10); // Cap at 10
        if limit == 0 {
            return Err(RepoError::InvalidParameter(
                "limit must be greater than 0".into(),
            ));
        }

        let statement = r#"
            // Get source user and ensure they follow someone
            MATCH (source:User {pubkey: $pubkey_val})
            WHERE source.followee_count > 0

            // Collect source's followees first to reduce repeated traversals
            MATCH (source)-[:FOLLOWS]->(followee:User)
            WITH source, collect(DISTINCT followee) as sourceFollowees, source.followee_count AS source_followees

            // Find targets through the collected followees
            UNWIND sourceFollowees as commonFollowee
            MATCH (target:User)-[:FOLLOWS]->(commonFollowee)
            WHERE target.pagerank >= $min_pagerank
              AND target <> source
              AND target.followee_count > 0
              AND NOT EXISTS {
                  MATCH (source)-[:FOLLOWS]->(target)
              }

            // Group and count common followees
            WITH source_followees, target, COUNT(DISTINCT commonFollowee) AS common_count

            // Calculate Jaccard similarity using cached counts
            WITH target,
                 common_count,
                 source_followees,
                 target.followee_count AS target_followees,
                 toFloat(common_count) / (source_followees + target.followee_count - common_count) AS similarity
            WHERE similarity > 0.1

            RETURN target.pubkey AS target_pubkey,
                   target.friendly_id AS friendly_id,
                   similarity,
                   target.pagerank AS pagerank,
                   common_count AS mutualFollowCount
            ORDER BY similarity DESC, pagerank DESC
            LIMIT $limit;
        "#;

        let query = query(statement)
            .param("pubkey_val", pubkey.to_hex())
            .param("min_pagerank", min_pagerank)
            .param("limit", limit as i64);

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
            let mutual_follow_count: i64 = row.get::<i64>("mutualFollowCount").unwrap_or(0);

            // Determine recommendation reason based on realistic Nostr network thresholds
            let reason_type = if similarity > 0.3 {
                RecommendationReason::HighSimilarity
            } else if similarity > 0.15 {
                RecommendationReason::ModerateSimilarity
            } else if mutual_follow_count >= 5 {
                RecommendationReason::SharedInterests
            } else if pagerank > 0.5 && similarity > 0.1 {
                RecommendationReason::PopularAccount
            } else {
                RecommendationReason::LowSimilarity
            };

            recommendations.push(Recommendation {
                pubkey: PublicKey::from_hex(&pubkey)
                    .map_err(RepoError::GetRecommendationsPubkey)?,
                friendly_id,
                similarity,
                pagerank,
                reason_type,
            });
        }

        Ok(recommendations)
    }

    async fn get_pageranks(
        &self,
        pubkeys: &[PublicKey],
    ) -> Result<HashMap<PublicKey, f64>, RepoError> {
        if pubkeys.is_empty() {
            return Ok(HashMap::new());
        }

        let statement = r#"
            UNWIND $pubkeys AS pubkey
            MATCH (user:User {pubkey: pubkey})
            WHERE user.pagerank IS NOT NULL
            RETURN user.pubkey AS pubkey, user.pagerank AS pagerank
        "#;

        let pubkeys_hex: Vec<String> = pubkeys.iter().map(PublicKey::to_hex).collect();
        let query = query(statement).param("pubkeys", pubkeys_hex);

        let mut records = self
            .graph
            .execute(query)
            .await
            .map_err(RepoError::GetPageRank)?;

        let mut pageranks = HashMap::new();
        while let Ok(Some(row)) = records.next().await {
            let pubkey = row.get::<String>("pubkey").map_err(|e| {
                RepoError::deserialization_with_context(e, "deserializing 'pubkey' field")
            })?;
            let pagerank = row.get::<f64>("pagerank").map_err(|e| {
                RepoError::deserialization_with_context(e, "deserializing 'pagerank' field")
            })?;
            let pubkey = PublicKey::from_hex(&pubkey).map_err(RepoError::GetPageRankPubkey)?;
            pageranks.insert(pubkey, pagerank);
        }

        Ok(pageranks)
    }

    async fn get_top_followers(
        &self,
        pubkey: &PublicKey,
        limit: usize,
    ) -> Result<TopFollowers, RepoError> {
        // Count `total` from the matched FOLLOWS edges rather than the cached
        // `follower_count` property: that counter is null for users first seen
        // as a follower (upsert_follow only increments a non-null counter), which
        // would otherwise make a known user with real followers report total =
        // None. Grouping by `target` keeps an unknown user at zero rows (total
        // stays None), while a known user with no followers still yields one row
        // carrying total = 0 (its follower_pubkey is null and skipped below).
        let statement = r#"
            MATCH (target:User {pubkey: $pubkey})
            OPTIONAL MATCH (follower:User)-[:FOLLOWS]->(target)
            WITH target, follower
            ORDER BY coalesce(follower.pagerank, 0.0) DESC
            WITH target,
                 count(follower) AS total,
                 collect(follower)[..$limit] AS top_followers
            UNWIND (CASE WHEN size(top_followers) = 0 THEN [null] ELSE top_followers END) AS follower
            RETURN total,
                   follower.pubkey AS follower_pubkey,
                   coalesce(follower.pagerank, 0.0) AS pagerank
        "#;

        let query = query(statement)
            .param("pubkey", pubkey.to_hex())
            .param("limit", limit as i64);

        let mut records = self
            .graph
            .execute(query)
            .await
            .map_err(RepoError::GetTopFollowers)?;

        let mut followers = Vec::new();
        let mut total: Option<u64> = None;
        while let Ok(Some(row)) = records.next().await {
            if total.is_none() {
                total = row.get::<Option<u64>>("total").map_err(|e| {
                    RepoError::deserialization_with_context(e, "deserializing 'total' field")
                })?;
            }

            let follower_pubkey = row.get::<Option<String>>("follower_pubkey").map_err(|e| {
                RepoError::deserialization_with_context(e, "deserializing 'follower_pubkey' field")
            })?;
            let Some(follower_pubkey) = follower_pubkey else {
                continue;
            };

            let pagerank = row.get::<f64>("pagerank").map_err(|e| {
                RepoError::deserialization_with_context(e, "deserializing 'pagerank' field")
            })?;
            let follower_pubkey =
                PublicKey::from_hex(&follower_pubkey).map_err(RepoError::GetTopFollowersPubkey)?;
            followers.push((follower_pubkey, pagerank));
        }

        Ok((followers, total))
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
        let trusted = maybe_account_info.is_some_and(|account| account.is_trusted());

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

    #[error("Failed to parse pubkey from pagerank query: {0}")]
    GetPageRankPubkey(nostr_sdk::key::Error),

    #[error("Failed to get top followers: {0}")]
    GetTopFollowers(neo4rs::Error),

    #[error("Failed to parse pubkey from top followers query: {0}")]
    GetTopFollowersPubkey(nostr_sdk::key::Error),

    #[error("Failed to remove pubkey: {0}")]
    RemovePubkey(neo4rs::Error),

    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),
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

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum RecommendationReason {
    HighSimilarity,     // Similarity score > 0.3
    ModerateSimilarity, // Similarity score 0.15-0.3
    SharedInterests,    // Many common followees (>= 5)
    PopularAccount,     // High pagerank (> 0.5) with some similarity
    LowSimilarity,      // Default for lower similarity matches
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Recommendation {
    pub pubkey: PublicKey,
    pub friendly_id: String,
    pub similarity: f64,
    pub pagerank: f64,
    pub reason_type: RecommendationReason,
}

/// Integration tests for the raw Cypher of the OpenRanking repo queries
/// (`get_top_followers`, `get_pageranks`). They need a live Neo4j and are
/// excluded from CI (which runs `--features ci`); run them locally against the
/// `db` service in `compose.yaml` (`docker compose up -d db`). Override the
/// connection with `TEST_NEO4J_URI` / `TEST_NEO4J_USER` / `TEST_NEO4J_PASSWORD`.
#[cfg(all(test, not(feature = "ci")))]
mod integration_tests {
    use super::*;
    use nostr_sdk::Keys;

    async fn test_graph() -> Graph {
        let uri = std::env::var("TEST_NEO4J_URI").unwrap_or_else(|_| "127.0.0.1:7687".to_string());
        let user = std::env::var("TEST_NEO4J_USER").unwrap_or_else(|_| "neo4j".to_string());
        let password =
            std::env::var("TEST_NEO4J_PASSWORD").unwrap_or_else(|_| "password".to_string());
        Graph::new(&uri, &user, &password)
            .await
            .expect("connect to a test Neo4j (try `docker compose up -d db`)")
    }

    fn fresh_pubkey() -> PublicKey {
        Keys::generate().public_key()
    }

    /// Create a FOLLOWS edge without touching `follower_count`, reproducing a
    /// followee first seen as a follower (its cached counter stays null).
    async fn seed_follow(graph: &Graph, follower: &PublicKey, followee: &PublicKey, pagerank: f64) {
        let statement = r#"
            MERGE (followee:User {pubkey: $followee})
            MERGE (follower:User {pubkey: $follower})
            SET follower.pagerank = $pagerank
            MERGE (follower)-[:FOLLOWS]->(followee)
        "#;
        graph
            .run(
                query(statement)
                    .param("followee", followee.to_hex())
                    .param("follower", follower.to_hex())
                    .param("pagerank", pagerank),
            )
            .await
            .expect("seed follow edge");
    }

    async fn seed_user(graph: &Graph, pubkey: &PublicKey) {
        graph
            .run(query("MERGE (u:User {pubkey: $pubkey})").param("pubkey", pubkey.to_hex()))
            .await
            .expect("seed user");
    }

    async fn seed_user_with_pagerank(graph: &Graph, pubkey: &PublicKey, pagerank: f64) {
        graph
            .run(
                query("MERGE (u:User {pubkey: $pubkey}) SET u.pagerank = $pagerank")
                    .param("pubkey", pubkey.to_hex())
                    .param("pagerank", pagerank),
            )
            .await
            .expect("seed user with pagerank");
    }

    #[tokio::test]
    async fn get_top_followers_counts_real_edges_when_follower_count_is_null() {
        let graph = test_graph().await;
        let target = fresh_pubkey();
        let high = fresh_pubkey();
        let low = fresh_pubkey();
        seed_follow(&graph, &high, &target, 0.9).await;
        seed_follow(&graph, &low, &target, 0.5).await;
        let repo = Repo::new(graph);

        let (followers, total) = repo
            .get_top_followers(&target, 10)
            .await
            .expect("query succeeds");

        assert_eq!(
            total,
            Some(2),
            "total counts matched FOLLOWS edges, not the (null) cached follower_count"
        );
        let pubkeys: Vec<String> = followers.iter().map(|(p, _)| p.to_hex()).collect();
        assert_eq!(
            pubkeys,
            vec![high.to_hex(), low.to_hex()],
            "followers ranked by pagerank desc"
        );
        assert_eq!(followers[0].1, 0.9);
    }

    #[tokio::test]
    async fn get_top_followers_reports_zero_total_for_known_user_without_followers() {
        let graph = test_graph().await;
        let target = fresh_pubkey();
        seed_user(&graph, &target).await;
        let repo = Repo::new(graph);

        let (followers, total) = repo
            .get_top_followers(&target, 10)
            .await
            .expect("query succeeds");

        assert!(followers.is_empty());
        assert_eq!(
            total,
            Some(0),
            "a known user with no followers reports 0, not unknown"
        );
    }

    #[tokio::test]
    async fn get_top_followers_reports_no_total_for_unknown_user() {
        let graph = test_graph().await;
        let unknown = fresh_pubkey();
        let repo = Repo::new(graph);

        let (followers, total) = repo
            .get_top_followers(&unknown, 10)
            .await
            .expect("query succeeds");

        assert!(followers.is_empty());
        assert_eq!(total, None, "an unknown user reports no total");
    }

    #[tokio::test]
    async fn get_top_followers_honours_limit_but_total_reflects_all_edges() {
        let graph = test_graph().await;
        let target = fresh_pubkey();
        let a = fresh_pubkey();
        let b = fresh_pubkey();
        let c = fresh_pubkey();
        seed_follow(&graph, &a, &target, 0.9).await;
        seed_follow(&graph, &b, &target, 0.5).await;
        seed_follow(&graph, &c, &target, 0.2).await;
        let repo = Repo::new(graph);

        let (followers, total) = repo
            .get_top_followers(&target, 2)
            .await
            .expect("query succeeds");

        assert_eq!(followers.len(), 2, "page respects the limit");
        assert_eq!(total, Some(3), "total reflects every follower edge");
        let pubkeys: Vec<String> = followers.iter().map(|(p, _)| p.to_hex()).collect();
        assert_eq!(
            pubkeys,
            vec![a.to_hex(), b.to_hex()],
            "the page keeps the top followers by pagerank desc"
        );
    }

    #[tokio::test]
    async fn get_pageranks_returns_known_and_omits_null_and_unknown() {
        let graph = test_graph().await;
        let with_rank = fresh_pubkey();
        let null_rank = fresh_pubkey();
        let unknown = fresh_pubkey();
        seed_user_with_pagerank(&graph, &with_rank, 0.7).await;
        seed_user(&graph, &null_rank).await;
        let repo = Repo::new(graph);

        let ranks = repo
            .get_pageranks(&[with_rank, null_rank, unknown])
            .await
            .expect("query succeeds");

        assert_eq!(ranks.get(&with_rank).copied(), Some(0.7));
        assert!(
            !ranks.contains_key(&null_rank),
            "a null pagerank is omitted (the caller coalesces it to 0)"
        );
        assert!(
            !ranks.contains_key(&unknown),
            "an unknown pubkey is omitted"
        );
    }
}
