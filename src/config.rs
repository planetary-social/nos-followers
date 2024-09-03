use config_rs::{Config as ConfigTree, ConfigError, Environment, File};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::env;
use tracing::info;

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub relay: String,
    pub neo4j_uri: String,
    pub neo4j_user: String,
    pub neo4j_password: String,
    pub event_channel_size: usize,
    pub event_workers: usize,
    pub follow_change_channel_size: usize,
    pub follow_change_workers: usize,
    pub worker_timeout_secs: u64,
    pub google_project_id: String,
    pub google_topic: String,
    pub seconds_threshold: u64,
    pub followers_per_hour_before_rate_limit: u32,
    pub max_retention_minutes: i64,
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
