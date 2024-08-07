mod config;
mod domain;
mod follows_differ;
mod relay_subscriber;
mod repo;
mod worker_pool;

use crate::config::Config;
use anyhow::Result;
use cached::proc_macro::cached;
use cached::TimedSizedCache;
use follows_differ::FollowChange;
use follows_differ::FollowsDiffer;
use nostr_sdk::prelude::*;
use relay_subscriber::{create_client, start_nostr_subscription};
use repo::Repo;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};
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
    let follows_differ = FollowsDiffer::new(repo, follow_change_tx);
    let cancellation_token = CancellationToken::new();

    let worker_tracker =
        WorkerPool::start(4, event_rx, cancellation_token.clone(), follows_differ)?;

    let nostr_client = Arc::new(create_client());

    let nostr_client_clone = nostr_client.clone();
    let follow_change_task = tokio::spawn(async move {
        while let Some(follow_change) = follow_change_rx.recv().await {
            match follow_change {
                FollowChange::Followed {
                    at,
                    follower,
                    followee,
                } => {
                    let (friendly_follower, friendly_followee) = tokio::join!(
                        fetch_friendly_id(&nostr_client_clone, &follower),
                        fetch_friendly_id(&nostr_client_clone, &followee)
                    );

                    debug!(
                        "Followed: {}({}) -> {}({}) at {}",
                        follower,
                        friendly_follower,
                        followee,
                        friendly_followee,
                        at.to_human_datetime()
                    );
                    // TODO: Send to pubsub
                }
                FollowChange::Unfollowed {
                    at,
                    follower,
                    followee,
                } => {
                    let (friendly_follower, friendly_followee) = tokio::join!(
                        fetch_friendly_id(&nostr_client_clone, &follower),
                        fetch_friendly_id(&nostr_client_clone, &followee)
                    );

                    debug!(
                        "Unfollowed: {}({}) -> {}({}) at {}",
                        follower,
                        friendly_follower,
                        followee,
                        friendly_followee,
                        at.to_human_datetime()
                    )
                    // TODO: Send to pubsub
                }
            }
        }
    });

    let relay = config.get_by_key::<String>("relay")?;
    let five_minutes_ago = Timestamp::now() - 60 * 5;
    let filters = vec![Filter::new()
        .since(five_minutes_ago)
        .kind(Kind::ContactList)];

    start_nostr_subscription(
        nostr_client,
        &[relay],
        filters,
        event_tx,
        cancellation_token.clone(),
    )
    .await?;
    worker_tracker.wait().await;
    follow_change_task.await?;

    Ok(())
}

// Try to return an identifier that is not the public key.// Define a cached async function with a 5-minute expiration
// We cache 10000 entries and each entry expires after 5 minutes
#[cached(
    ty = "TimedSizedCache<String, String>",
    create = "{ TimedSizedCache::with_size_and_lifespan(10000, 300) }",
    convert = r#"{ public_key.to_hex() }"#
)]
async fn fetch_friendly_id(client: &Client, public_key: &PublicKey) -> String {
    let npub_or_pubkey = match public_key.to_bech32() {
        Ok(npub) => npub,
        Err(_) => return public_key.to_hex(),
    };

    let Some(metadata) = client.metadata(*public_key).await.ok() else {
        debug!("Failed to get metadata for public key: {}", public_key);
        return npub_or_pubkey;
    };

    let name_or_npub_or_pubkey = metadata.name.unwrap_or_else(|| npub_or_pubkey);

    if let Some(nip05_value) = metadata.nip05 {
        let Ok(verified) = nip05::verify(&public_key, &nip05_value, None).await else {
            debug!("Failed to verify Nip05 for public key: {}", public_key);
            return name_or_npub_or_pubkey;
        };

        if !verified {
            debug!("Nip05 for public key: {} is not verified", public_key);
            return name_or_npub_or_pubkey;
        }

        debug!("Nip05 for public key: {} is: {}", public_key, nip05_value);
        return nip05_value;
    }

    name_or_npub_or_pubkey
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
