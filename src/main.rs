mod config;
mod contacts_differ;
mod domain;
mod relay_subscriber;
mod repo;
mod worker_pool;

use crate::config::Config;
use anyhow::Result;
use contacts_differ::ContactsDiffer;
use contacts_differ::FollowChange;
use nostr_sdk::prelude::*;
use relay_subscriber::start_nostr_subscription;
use repo::Repo;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};
use worker_pool::WorkerPool;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let config = Config::new("config")?;

    let connection_string = get_connection_string(&config);
    let db_pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&connection_string)
        .await?;

    let (event_tx, event_rx) = mpsc::channel::<Box<Event>>(100);

    let repo = Repo::new(db_pool.clone());
    let (follow_change_tx, mut follow_change_rx) = mpsc::channel::<FollowChange>(100);
    let contacts_differ = ContactsDiffer::new(repo, follow_change_tx);
    let cancellation_token = CancellationToken::new();

    let worker_tracker =
        WorkerPool::start(4, event_rx, cancellation_token.clone(), contacts_differ)?;

    let follow_change_task = tokio::spawn(async move {
        while let Some(follow_change) = follow_change_rx.recv().await {
            match follow_change {
                FollowChange::Followed {
                    at,
                    follower,
                    followee,
                } => {
                    debug!(
                        "Followed: {} -> {} at {}",
                        follower,
                        followee,
                        at.to_human_datetime()
                    );
                }
                FollowChange::Unfollowed {
                    at,
                    follower,
                    followee,
                } => {
                    debug!(
                        "Unfollowed: {} -> {} at {}",
                        follower,
                        followee,
                        at.to_human_datetime()
                    )
                }
            }
        }
    });

    let relay = config.get_by_key::<String>("relay")?;
    let filters = vec![Filter::new().limit(0).kind(Kind::ContactList)];

    start_nostr_subscription(&[relay], filters, event_tx, cancellation_token.clone()).await?;
    worker_tracker.wait().await;
    follow_change_task.await?;

    Ok(())
}

fn get_connection_string(config: &Config) -> String {
    let user = config.get_by_key::<String>("PG_USER").unwrap();
    let password = config.get_by_key::<String>("PG_PASSWORD").unwrap();
    let host = config.get_by_key::<String>("PG_HOST").unwrap();
    let db_name = config.get_by_key::<String>("PG_DBNAME").unwrap();

    let redacted = get_connection_string_from_parts(&user, "*****", &host, &db_name);
    info!("Connecting to {}", redacted);

    get_connection_string_from_parts(&user, &password, &host, &db_name)
}

fn get_connection_string_from_parts(
    user: &str,
    password: &str,
    host: &str,
    dbname: &str,
) -> String {
    format!("postgres://{}:{}@{}/{}", user, password, host, dbname)
}
