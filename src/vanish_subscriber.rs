use crate::repo::RepoTrait;
use async_trait::async_trait;
use nostr_sdk::prelude::PublicKey;
use redis::{
    aio::ConnectionManager,
    streams::{StreamKey, StreamReadOptions, StreamReadReply},
    AsyncCommands, RedisError,
};
use std::error::Error;
use std::sync::Arc;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{error, info};

static BLOCK_MILLIS: usize = 5000;
static VANISH_STREAM_KEY: &str = "vanish_requests";
static VANISH_LAST_ID_KEY: &str = "vanish_requests:followers_subscriber:last_id";

pub struct RedisClient {
    client: redis::Client,
}

#[async_trait]
pub trait RedisClientTrait: Send + Sync + 'static {
    type Connection: RedisClientConnectionTrait;
    async fn get_connection(&self) -> Result<Self::Connection, RedisError>;
}

impl RedisClient {
    pub fn new(url: &str) -> Self {
        let client = redis::Client::open(url).expect("Failed to create Redis client");
        RedisClient { client }
    }
}

#[async_trait]
impl RedisClientTrait for RedisClient {
    type Connection = RedisClientConnection;
    async fn get_connection(&self) -> Result<Self::Connection, RedisError> {
        let con = self.client.get_connection_manager().await?;
        Ok(RedisClientConnection { con })
    }
}

pub struct RedisClientConnection {
    con: ConnectionManager,
}

#[async_trait]
pub trait RedisClientConnectionTrait: Send + Sync + 'static {
    async fn get(&mut self, key: &str) -> Result<String, RedisError>;
    async fn set(&mut self, key: &str, value: String) -> Result<(), RedisError>;
    async fn xread_options(
        &mut self,
        keys: &[&str],
        ids: &[String],
        opts: &StreamReadOptions,
    ) -> Result<StreamReadReply, RedisError>;
}

#[async_trait]
impl RedisClientConnectionTrait for RedisClientConnection {
    async fn get(&mut self, key: &str) -> Result<String, RedisError> {
        match self.con.get(key).await {
            Ok(value) => Ok(value),
            Err(_) => self.con.get(key).await,
        }
    }

    async fn set(&mut self, key: &str, value: String) -> Result<(), RedisError> {
        match self.con.set(key, value.clone()).await {
            Ok(()) => Ok(()),
            Err(_) => self.con.set(key, value).await,
        }
    }

    async fn xread_options(
        &mut self,
        keys: &[&str],
        ids: &[String],
        opts: &StreamReadOptions,
    ) -> Result<StreamReadReply, RedisError> {
        match self.con.xread_options(keys, ids, opts).await {
            Ok(reply) => Ok(reply),
            Err(_) => self.con.xread_options(keys, ids, opts).await,
        }
    }
}

pub fn start_vanish_subscriber<T: RedisClientTrait, U: RepoTrait>(
    tracker: TaskTracker,
    redis_client: T,
    repo: Arc<U>,
    cancellation_token: CancellationToken,
) {
    tracker.spawn(async move {
        let (mut con, mut last_id) = match get_connection_and_last_id(redis_client).await {
            Ok(result) => result,
            Err(e) => {
                error!("Failed to get Redis connection: {}", e);
                return;
            }
        };

        let opts = StreamReadOptions::default().block(BLOCK_MILLIS);

        info!("Starting from last id processed: {}", last_id);

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    break;
                }

                result = async {
                    let reply: StreamReadReply = con
                        .xread_options(&[VANISH_STREAM_KEY], &[last_id.clone()], &opts)
                        .await?;

                    for StreamKey { ids, .. } in reply.keys {
                        for stream_id in ids {
                            if stream_id.id == last_id {
                                continue;
                            }

                            let Some(value) = stream_id.map.get("pubkey") else {
                                error!("Vanish request doesn't have a public key");
                                continue;
                            };

                            let public_key = match value {
                                redis::Value::BulkString(bytes) => {
                                    let public_key_string = String::from_utf8(bytes.clone())?;

                                    PublicKey::from_hex(public_key_string)?
                                }
                                _ => {
                                    error!("Vanish request public key is not a bulk string");
                                    continue;
                                }
                            };

                            if let Err(e) = repo.remove_pubkey(&public_key).await {
                                error!("Failed to remove public key: {}", e);
                            }

                            info!("Removed public key {} from vanish request", public_key.to_hex());

                            last_id = stream_id.id.clone();
                        }
                    }
                    Ok::<(), Box<dyn Error>>(())
                } => {
                    if let Err(e) = result {
                        error!("Error in Redis stream reader task: {}", e);
                        continue;
                    }
                }
            }
        }
    });
}

async fn get_connection_and_last_id<T: RedisClientTrait>(
    redis_client: T,
) -> Result<(T::Connection, String), RedisError> {
    let mut con = redis_client.get_connection().await?;
    let last_id = con
        .get(&VANISH_LAST_ID_KEY)
        .await
        .unwrap_or_else(|_| "0-0".to_string());
    Ok((con, last_id))
}
