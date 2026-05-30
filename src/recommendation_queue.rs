use crate::repo::{Recommendation, RepoTrait};
use moka::future::Cache;
use nostr_sdk::PublicKey;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
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

/// Cache key for a recommendation request. Shared by the async queue and the
/// synchronous `/recommend` path so a result computed by either serves the other.
fn cache_key_for(pubkey: &PublicKey, min_pagerank: Option<f64>, limit: Option<usize>) -> String {
    format!(
        "{}:{}:{}",
        pubkey.to_hex(),
        min_pagerank.unwrap_or(0.3),
        limit.unwrap_or(10)
    )
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

/// RAII reservation of one recommendation-queue slot.
///
/// Constructing it with `reserve` takes a slot (counter += 1) and republishes the
/// depth gauge; dropping it releases the slot (counter -= 1) and republishes the
/// gauge. Because the counter and the gauge always move together in one place,
/// the gauge can never drift from the counter, and a slot is freed on *every*
/// exit path — including a dropped/cancelled future between reserving and
/// dispatching (e.g. the route timeout firing mid-await).
///
/// On a successful dispatch the request path hands the single pending release to
/// the worker: it `disarm`s its local guard and the worker `adopt`s the
/// reservation, so the slot is released exactly once, when the compute finishes.
struct QueueSlot {
    queue_size: Arc<AtomicUsize>,
    armed: bool,
}

impl QueueSlot {
    /// Reserve a fresh slot (counter += 1) and publish the new depth.
    fn reserve(queue_size: Arc<AtomicUsize>) -> Self {
        let depth = queue_size.fetch_add(1, Ordering::Relaxed) + 1;
        crate::metrics::recommendation_queue_depth().set(depth as f64);
        Self {
            queue_size,
            armed: true,
        }
    }

    /// Adopt an already-reserved slot without changing the counter, so its single
    /// release happens when this guard drops. Pairs with `disarm` on the handoff.
    fn adopt(queue_size: Arc<AtomicUsize>) -> Self {
        Self {
            queue_size,
            armed: true,
        }
    }

    /// Hand the pending release to another owner: this guard will not release on
    /// drop. Consumes the guard so it cannot be used afterwards.
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for QueueSlot {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let depth = self.queue_size.fetch_sub(1, Ordering::Relaxed) - 1;
        crate::metrics::recommendation_queue_depth().set(depth as f64);
    }
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
            .max_capacity(500)
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
        let cache_key = cache_key_for(&pubkey, min_pagerank, limit);

        // Check if we already have a result
        if let Some(recommendations) = self.result_cache.get(&cache_key).await {
            return Ok(RecommendationStatus::Completed(recommendations));
        }

        // Join an already in-flight request, but treat a cached Failed as a miss.
        // A failure leaves no work running, so short-circuiting on it would wedge
        // every retry on a stale, terminal Failed and never recompute; fall
        // through instead so the single-flight below re-enqueues a fresh warm-up.
        if let Some(status) = self.status_cache.get(&cache_key).await {
            if !matches!(status, RecommendationStatus::Failed(_)) {
                return Ok(status);
            }
        }

        // Best-effort backpressure: shed when the queue is already saturated.
        // This is a soft signal (QueueFull is never cached); the authoritative
        // dedup is the single-flight entry below.
        let current_size = self.queue_size.load(Ordering::Relaxed);
        if current_size >= self.max_queue_size {
            crate::metrics::recommend_queue_full().increment(1);
            return Ok(RecommendationStatus::QueueFull);
        }

        // Single-flight the cold-miss enqueue. `or_insert_with_if` atomically
        // inserts a fresh `Pending` when the key is absent *or* holds a stale
        // terminal `Failed` (self-heal), and coalesces a concurrent burst for the
        // same key into one init: only the first caller gets `is_fresh() == true`
        // and dispatches the heavy compute, while the rest join the resulting
        // in-flight status. This closes the check-then-insert race where a cold
        // burst each enqueued a redundant Jaccard compute.
        let entry = self
            .status_cache
            .entry_by_ref(&cache_key)
            .or_insert_with_if(
                async { RecommendationStatus::Pending { position: 0 } },
                |current| matches!(current, RecommendationStatus::Failed(_)),
            )
            .await;

        if !entry.is_fresh() {
            // Another caller already reserved this key; join its in-flight status.
            return Ok(entry.into_value());
        }

        // We won the single-flight for this key. Reserve the queue slot *first*,
        // via an RAII guard, so the reservation is released on every exit path
        // below — including a dropped/cancelled future between here and dispatch.
        // The guard also keeps the depth gauge in lockstep with the counter.
        let slot = QueueSlot::reserve(self.queue_size.clone());

        // Record the real queue position and dispatch the background warm-up.
        let position = {
            let mut pending = self.pending_requests.write().await;
            pending.push(cache_key.clone());
            pending.len()
        };

        let pending_status = RecommendationStatus::Pending { position };
        self.status_cache
            .insert(cache_key.clone(), pending_status.clone())
            .await;

        let request = RecommendationRequest {
            pubkey,
            min_pagerank,
            limit,
            cache_key: cache_key.clone(),
        };
        if self.request_sender.send(request).is_err() {
            // Dispatch failed: clear the in-flight status so a later retry
            // re-enqueues instead of joining a phantom request. The `slot` guard
            // releases the reservation as it drops on return.
            let mut pending = self.pending_requests.write().await;
            pending.retain(|k| k != &cache_key);
            self.status_cache.invalidate(&cache_key).await;
            return Err("Failed to queue recommendation request".into());
        }

        // Dispatch succeeded: hand the slot's single release to the worker, which
        // releases it when the compute finishes. `disarm` stops this local guard
        // from also releasing it (which would double-count).
        slot.disarm();
        Ok(pending_status)
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

                        // Take over the queue-slot reservation the request path
                        // handed off on dispatch. Its Drop releases the slot (and
                        // republishes the depth gauge) when this job finishes, on
                        // every arm below, so the gauge can never diverge from the
                        // counter.
                        let _slot = QueueSlot::adopt(queue_size.clone());

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
                        let started = Instant::now();
                        match repo
                            .get_recommendations(&request.pubkey, request.min_pagerank, request.limit)
                            .await
                        {
                            Ok(recommendations) => {
                                // Record how long the warm-up compute took so
                                // operators can watch warm latency.
                                crate::metrics::recommend_warm_seconds()
                                    .record(started.elapsed().as_secs_f64());

                                info!(
                                    "Completed recommendation calculation for {}: {} results",
                                    request.pubkey.to_hex(),
                                    recommendations.len()
                                );

                                // Store in result cache (get_status checks this first)
                                result_cache
                                    .insert(cache_key.clone(), recommendations)
                                    .await;

                                // Remove transient status now that result is cached
                                status_cache.remove(&cache_key).await;
                            }
                            Err(e) => {
                                error!(
                                    "Failed to calculate recommendations for {}: {}",
                                    request.pubkey.to_hex(),
                                    e
                                );

                                // Count the failure distinctly from a warming
                                // cold-miss so operators can tell them apart.
                                crate::metrics::recommend_failed().increment(1);

                                // Mark as failed
                                status_cache
                                    .insert(cache_key, RecommendationStatus::Failed(e.to_string()))
                                    .await;
                            }
                        }
                        // `_slot` drops here, releasing the reservation and
                        // republishing the depth gauge.
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::{Recommendation, RepoError};
    use nostr_sdk::Keys;
    use tokio::sync::Barrier;

    /// A repo whose only job is to count how often the expensive recommendation
    /// compute runs, so a test can prove the queue collapses a concurrent cold
    /// burst for one key into a single compute rather than one per request.
    #[derive(Default)]
    struct CountingRepo {
        calls: Arc<AtomicUsize>,
    }

    impl RepoTrait for CountingRepo {
        async fn get_recommendations(
            &self,
            _pubkey: &PublicKey,
            _min_pagerank: Option<f64>,
            _limit: Option<usize>,
        ) -> Result<Vec<Recommendation>, RepoError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            // A short compute so that, in a racy queue, redundant jobs stay
            // queued long enough to be observed draining; a single-flight queue
            // still runs this exactly once.
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(Vec::new())
        }
    }

    fn queue_with_worker(calls: Arc<AtomicUsize>) -> RecommendationQueue<CountingRepo> {
        let repo = Arc::new(CountingRepo { calls });
        let result_cache = Cache::builder().build();
        let (queue, receiver) = RecommendationQueue::new(repo, result_cache, 64);
        queue.start_worker(TaskTracker::new(), receiver, CancellationToken::new());
        queue
    }

    /// Drain barrier: wait until the worker has finished every job that was
    /// actually enqueued (each completed job decrements `queue_size`).
    async fn wait_for_drain<T: RepoTrait + 'static>(queue: &RecommendationQueue<T>) {
        for _ in 0..1000 {
            if queue.queue_size.load(Ordering::SeqCst) == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("recommendation queue never drained");
    }

    /// A burst of concurrent cold requests for one key must enqueue a single
    /// background compute, not one per request. Regression test for the
    /// non-atomic check-then-insert in `request_recommendations`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_cold_requests_for_one_key_compute_once() {
        const BURST: usize = 24;
        let calls = Arc::new(AtomicUsize::new(0));
        let queue = Arc::new(queue_with_worker(calls.clone()));
        let pov = Keys::generate().public_key();

        let barrier = Arc::new(Barrier::new(BURST));
        let mut tasks = Vec::with_capacity(BURST);
        for _ in 0..BURST {
            let queue = queue.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                queue
                    .request_recommendations(pov, None, Some(10))
                    .await
                    .expect("request is accepted");
            }));
        }
        for task in tasks {
            task.await.expect("request task completes");
        }

        wait_for_drain(&queue).await;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "a concurrent cold burst for one key must collapse to a single compute"
        );
    }

    /// A request future that is dropped (cancelled) after it has reserved its
    /// queue slot but before it dispatches must still release that slot —
    /// otherwise a route timeout firing mid-await would leak a slot and, over
    /// time, wedge the queue at `QueueFull`. The reservation rides an RAII guard
    /// acquired before the first await, so the cancellation releases it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_request_cancelled_before_dispatch_releases_its_slot() {
        let calls = Arc::new(AtomicUsize::new(0));
        let queue = Arc::new(queue_with_worker(calls));
        let pov = Keys::generate().public_key();

        // Hold the pending-requests lock so the winning request parks *after* it
        // reserves its slot but *before* it can record its position and dispatch.
        let lock = queue.pending_requests.write().await;

        let request = {
            let queue = queue.clone();
            tokio::spawn(async move { queue.request_recommendations(pov, None, Some(10)).await })
        };

        // Wait until the slot is reserved (the request is now parked on the lock).
        let mut reserved = false;
        for _ in 0..500 {
            if queue.queue_size.load(Ordering::SeqCst) == 1 {
                reserved = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(
            reserved,
            "the winning request reserves its slot before the first await"
        );

        // Cancel the in-flight request while it is parked before dispatch.
        request.abort();
        let _ = request.await;
        drop(lock);

        // The guard must have released the slot despite the cancellation.
        let mut released = false;
        for _ in 0..500 {
            if queue.queue_size.load(Ordering::SeqCst) == 0 {
                released = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(
            released,
            "a cancelled request must release its reserved slot"
        );
    }

    #[test]
    fn reserve_then_drop_conserves_the_slot_counter() {
        let counter = Arc::new(AtomicUsize::new(0));
        {
            let _slot = QueueSlot::reserve(counter.clone());
            assert_eq!(counter.load(Ordering::SeqCst), 1, "reserve takes a slot");
        }
        assert_eq!(counter.load(Ordering::SeqCst), 0, "drop releases the slot");
    }

    #[test]
    fn a_disarmed_slot_does_not_release_on_drop() {
        let counter = Arc::new(AtomicUsize::new(0));
        let slot = QueueSlot::reserve(counter.clone());
        slot.disarm();
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "disarm hands the release to another owner; the slot stays reserved"
        );
    }

    #[test]
    fn an_adopted_slot_releases_the_handed_off_reservation_on_drop() {
        // Models the worker side: the request path reserved (counter == 1) and
        // disarmed its guard, handing the single release to the worker.
        let counter = Arc::new(AtomicUsize::new(1));
        {
            let _slot = QueueSlot::adopt(counter.clone());
            assert_eq!(
                counter.load(Ordering::SeqCst),
                1,
                "adopt does not re-reserve"
            );
        }
        assert_eq!(
            counter.load(Ordering::SeqCst),
            0,
            "the adopting owner releases the slot on drop"
        );
    }
}
