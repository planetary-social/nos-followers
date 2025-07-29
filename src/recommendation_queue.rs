use crate::repo::{Recommendation, RepoTrait};
use moka::future::Cache;
use nostr_sdk::PublicKey;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::RwLock;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{error, info};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RecommendationStatus {
    Pending { position: usize },
    Processing,
    Completed(Vec<Recommendation>),
    Failed(String),
    QueueFull,
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
    pending_requests: Arc<RwLock<Vec<String>>>, // Track cache keys in order
    queue_size: Arc<AtomicUsize>,
    max_queue_size: usize,
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
            pending_requests: Arc::new(RwLock::new(Vec::new())),
            queue_size: Arc::new(AtomicUsize::new(0)),
            max_queue_size: 50, // Maximum 50 requests in queue
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

        // Check if queue is full
        let current_size = self.queue_size.load(Ordering::Relaxed);
        if current_size >= self.max_queue_size {
            return Ok(RecommendationStatus::QueueFull);
        }

        // Create new request
        let request = RecommendationRequest {
            pubkey,
            min_pagerank,
            limit,
            cache_key: cache_key.clone(),
        };

        // Add to pending requests and get position
        let position = {
            let mut pending = self.pending_requests.write().await;
            pending.push(cache_key.clone());
            pending.len()
        };

        // Update queue size
        self.queue_size.fetch_add(1, Ordering::Relaxed);

        // Mark as pending with position
        self.status_cache
            .insert(
                cache_key.clone(),
                RecommendationStatus::Pending { position },
            )
            .await;

        // Send request to worker
        if let Err(_) = self.request_sender.send(request) {
            // If send fails, clean up
            self.queue_size.fetch_sub(1, Ordering::Relaxed);
            let mut pending = self.pending_requests.write().await;
            pending.retain(|k| k != &cache_key);
            return Err("Failed to queue recommendation request".into());
        }

        Ok(RecommendationStatus::Pending { position })
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
        let pending_requests = self.pending_requests.clone();
        let queue_size = self.queue_size.clone();

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

                        // Remove from pending requests and update positions
                        {
                            let mut pending = pending_requests.write().await;
                            pending.retain(|k| k != &cache_key);

                            // Update positions for remaining requests
                            for (index, key) in pending.iter().enumerate() {
                                status_cache
                                    .insert(key.clone(), RecommendationStatus::Pending { position: index + 1 })
                                    .await;
                            }
                        }

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

                        // Update queue size
                        queue_size.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            }
        });
    }
}
