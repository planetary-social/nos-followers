use config_rs::{Config as ConfigTree, ConfigError, Environment, File};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::env;
use std::num::{NonZeroU16, NonZeroUsize};
use tracing::info;

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub relay: String,
    pub neo4j_uri: String,
    pub neo4j_user: String,
    pub neo4j_password: String,
    pub event_channel_size: NonZeroUsize,
    pub diff_workers: NonZeroUsize,
    pub follow_change_channel_size: NonZeroUsize,
    pub follow_change_workers: NonZeroUsize,
    pub worker_timeout_secs: NonZeroUsize,
    pub google_project_id: String,
    pub google_topic: String,
    pub flush_period_seconds: NonZeroUsize,
    pub min_seconds_between_messages: NonZeroUsize,
    pub tcp_importer_port: u16,
    pub http_port: u16,
    pub pagerank_cron_expression: String,
    pub http_cache_seconds: u32,
    pub burst: NonZeroU16,
}

impl Configurable for Settings {
    fn key() -> &'static str {
        // Root key
        "followers"
    }
}

pub const ENVIRONMENT_PREFIX: &str = "APP";
pub const CONFIG_SEPARATOR: &str = "__";

#[must_use]
pub fn environment() -> String {
    env::var(format!("{ENVIRONMENT_PREFIX}{CONFIG_SEPARATOR}ENVIRONMENT"))
        .unwrap_or_else(|_| "development".into())
}

pub trait Configurable {
    fn key() -> &'static str;
}

#[derive(Debug, Clone)]
pub struct Config {
    config: ConfigTree,
}

impl Config {
    pub fn new(config_dir: &str) -> Result<Self, ConfigError> {
        let environment = environment();

        let default_config_path = format!("{}/settings.yml", &config_dir);
        let env_config_path = format!("{}/settings.{}.yml", &config_dir, &environment);
        let local_config_path = format!("{}/settings.local.yml", &config_dir);

        info!("Loading configuration from: {}", default_config_path);
        info!(
            "Loading environment-specific configuration from: {}",
            env_config_path
        );
        info!("Loading local overrides from: {}", local_config_path);

        ConfigTree::builder()
            .add_source(File::with_name(&default_config_path))
            .add_source(File::with_name(&env_config_path).required(false))
            .add_source(File::with_name(&local_config_path).required(false))
            .add_source(Environment::with_prefix(ENVIRONMENT_PREFIX).separator(CONFIG_SEPARATOR))
            .build()
            .map(|c| Config { config: c })
    }

    pub fn get<T>(&self) -> Result<T, ConfigError>
    where
        T: Configurable,
        T: DeserializeOwned,
    {
        self.config.get::<T>(T::key())
    }
}
