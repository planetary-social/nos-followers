use crate::repo::{Recommendation, RepoTrait};
use moka::future::Cache;
use nostr_sdk::PublicKey;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{error, info};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RecommendationStatus {
    Pending,
    Processing,
    Completed(Vec<Recommendation>),
    Failed(String),
}

#[derive(Debug, Clone)]
pub struct RecommendationRequest {
    pub pubkey: PublicKey,
    pub min_pagerank: Option<f64>,
    pub limit: Option<usize>,
    pub cache_key: String,
}

pub struct RecommendationQueue<T>
where
    T: RepoTrait,
{
    repo: Arc<T>,
    status_cache: Cache<String, RecommendationStatus>,
    result_cache: Cache<String, Vec<Recommendation>>,
    request_sender: broadcast::Sender<RecommendationRequest>,
}

impl<T> RecommendationQueue<T>
where
    T: RepoTrait + 'static,
{
    pub fn new(
        repo: Arc<T>,
        result_cache: Cache<String, Vec<Recommendation>>,
        channel_size: usize,
    ) -> (Self, broadcast::Receiver<RecommendationRequest>) {
        let (request_sender, request_receiver) = broadcast::channel(channel_size);

        let status_cache = Cache::builder()
            .time_to_live(Duration::from_secs(300)) // 5 minutes for status
            .max_capacity(10000)
            .build();

        let queue = Self {
            repo,
            status_cache,
            result_cache,
            request_sender,
        };

        (queue, request_receiver)
    }

    pub async fn request_recommendations(
        &self,
        pubkey: PublicKey,
        min_pagerank: Option<f64>,
        limit: Option<usize>,
    ) -> Result<RecommendationStatus, String> {
        let cache_key = format!(
            "{}:{}:{}",
            pubkey.to_hex(),
            min_pagerank.unwrap_or(0.3),
            limit.unwrap_or(10)
        );

        // Check if we already have a result
        if let Some(recommendations) = self.result_cache.get(&cache_key).await {
            return Ok(RecommendationStatus::Completed(recommendations));
        }

        // Check if there's already a pending request
        if let Some(status) = self.status_cache.get(&cache_key).await {
            return Ok(status);
        }

        // Create new request
        let request = RecommendationRequest {
            pubkey,
            min_pagerank,
            limit,
            cache_key: cache_key.clone(),
        };

        // Mark as pending
        self.status_cache
            .insert(cache_key.clone(), RecommendationStatus::Pending)
            .await;

        // Send request to worker
        self.request_sender
            .send(request)
            .map_err(|_| "Failed to queue recommendation request")?;

        Ok(RecommendationStatus::Pending)
    }

    pub async fn get_status(&self, cache_key: &str) -> Option<RecommendationStatus> {
        // First check if we have completed results
        if let Some(recommendations) = self.result_cache.get(cache_key).await {
            return Some(RecommendationStatus::Completed(recommendations));
        }

        // Otherwise check status cache
        self.status_cache.get(cache_key).await
    }

    pub fn start_worker(
        &self,
        task_tracker: TaskTracker,
        mut receiver: broadcast::Receiver<RecommendationRequest>,
        cancellation_token: CancellationToken,
    ) {
        let repo = self.repo.clone();
        let status_cache = self.status_cache.clone();
        let result_cache = self.result_cache.clone();

        task_tracker.spawn(async move {
            info!("Recommendation queue worker started");

            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        info!("Recommendation queue worker shutting down");
                        break;
                    }
                    Ok(request) = receiver.recv() => {
                        let cache_key = request.cache_key.clone();

                        // Mark as processing
                        status_cache
                            .insert(cache_key.clone(), RecommendationStatus::Processing)
                            .await;

                        info!(
                            "Processing recommendation request for {} (cache_key: {})",
                            request.pubkey.to_hex(),
                            cache_key
                        );

                        // Calculate recommendations
                        match repo
                            .get_recommendations(&request.pubkey, request.min_pagerank, request.limit)
                            .await
                        {
                            Ok(recommendations) => {
                                info!(
                                    "Completed recommendation calculation for {}: {} results",
                                    request.pubkey.to_hex(),
                                    recommendations.len()
                                );

                                // Store in result cache
                                result_cache
                                    .insert(cache_key.clone(), recommendations.clone())
                                    .await;

                                // Update status
                                status_cache
                                    .insert(cache_key, RecommendationStatus::Completed(recommendations))
                                    .await;
                            }
                            Err(e) => {
                                error!(
                                    "Failed to calculate recommendations for {}: {}",
                                    request.pubkey.to_hex(),
                                    e
                                );

                                // Mark as failed
                                status_cache
                                    .insert(cache_key, RecommendationStatus::Failed(e.to_string()))
                                    .await;
                            }
                        }
                    }
                }
            }
        });
    }
}
