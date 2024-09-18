use anyhow::{Context, Result};
use log::{error, info};
use neo4rs::Graph;
use nos_followers::{
    config::{Config, Settings},
    repo::{Repo, RepoTrait},
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    info!("PageRank updater started");

    let config = Config::new("config").context("Loading configuration failed")?;
    let settings = config
        .get::<Settings>()
        .context("Retrieving settings from configuration failed")?;

    info!("Connecting to Neo4j at {}", settings.neo4j_uri);
    let graph = Graph::new(
        &settings.neo4j_uri,
        &settings.neo4j_user,
        &settings.neo4j_password,
    )
    .await
    .context("Failed to connect to Neo4j")?;

    let repo = Arc::new(Repo::new(graph));
    repo.log_neo4j_details().await?;

    info!("Update memory graph");
    if let Err(e) = repo.update_memory_graph().await {
        error!("Memory graph update failed: {:?}", e);
        return Err(e).context("Memory graph update encountered an error");
    }

    info!("Executing PageRank update");
    if let Err(e) = repo.update_pagerank().await {
        error!("PageRank update failed: {:?}", e);
        return Err(e).context("PageRank update encountered an error");
    }

    info!("PageRank update completed successfully");

    log::logger().flush();
    Ok(())
}
