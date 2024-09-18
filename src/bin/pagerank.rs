use anyhow::{Context, Result};
use neo4rs::Graph;
use nos_followers::{
    config::{Config, Settings},
    repo::{Repo, RepoTrait},
};
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

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

    info!("Executing PageRank update");
    if let Err(e) = repo.update_pagerank().await {
        error!("PageRank update failed: {:?}", e);
        return Err(e).context("PageRank update encountered an error");
    }

    info!("PageRank update completed successfully");
    Ok(())
}
