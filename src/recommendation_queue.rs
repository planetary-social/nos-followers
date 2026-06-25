use crate::repo::{Recommendation, RepoTrait};
use moka::future::Cache;
use nostr_sdk::PublicKey;
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{error, info, warn};

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
///
/// The request path's guard also carries [`SlotCleanup`]: if its future is
/// dropped/cancelled *before* a successful dispatch (e.g. the client disconnects
/// or the route timeout fires mid-await), the in-flight `Pending` status and the
/// `pending_requests` entry have already been recorded but no request will ever
/// run, so the guard's Drop clears them — otherwise every retry for that key
/// would join a phantom `Pending` and wedge until the status TTL expires. The
/// worker's adopted guard carries no cleanup: its Drop is the normal completion
/// release and must not touch the status the worker is legitimately managing.
struct QueueSlot {
    queue_size: Arc<AtomicUsize>,
    /// Phantom-state cleanup to run if this guard releases the slot itself (the
    /// request-path reservation was dropped before a successful dispatch). `None`
    /// for the worker's adopted slot.
    cleanup: Option<SlotCleanup>,
    armed: bool,
}

/// The state a cancelled request-path reservation must roll back: the `Pending`
/// status entry and the `pending_requests` bookkeeping entry for its key.
struct SlotCleanup {
    status_cache: Cache<String, RecommendationStatus>,
    pending_requests: Arc<RwLock<Vec<String>>>,
    cache_key: String,
}

impl SlotCleanup {
    /// Remove the phantom `Pending` status and the `pending_requests` entry,
    /// mirroring the send-failure cleanup, so a later retry re-enqueues instead of
    /// joining a request that will never run.
    async fn run(self) {
        {
            let mut pending = self.pending_requests.write().await;
            pending.retain(|k| k != &self.cache_key);
        }
        self.status_cache.invalidate(&self.cache_key).await;
    }
}

impl QueueSlot {
    /// Reserve a fresh slot (counter += 1) and publish the new depth. `cleanup`
    /// rolls back the phantom `Pending`/`pending_requests` state if this guard is
    /// dropped before its reservation is disarmed on a successful dispatch.
    fn reserve(queue_size: Arc<AtomicUsize>, cleanup: SlotCleanup) -> Self {
        let depth = queue_size.fetch_add(1, Ordering::Relaxed) + 1;
        crate::metrics::recommendation_queue_depth().set(depth as f64);
        Self {
            queue_size,
            cleanup: Some(cleanup),
            armed: true,
        }
    }

    /// Adopt an already-reserved slot without changing the counter, so its single
    /// release happens when this guard drops. Pairs with `disarm` on the handoff.
    /// Carries no cleanup: the worker owns the status from here on.
    fn adopt(queue_size: Arc<AtomicUsize>) -> Self {
        Self {
            queue_size,
            cleanup: None,
            armed: true,
        }
    }

    /// Hand the pending release to another owner: this guard will not release the
    /// slot or run its cleanup on drop. Consumes the guard so it cannot be used
    /// afterwards.
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

        // Released by this guard before a successful dispatch (cancellation or
        // send failure): roll back the phantom status/pending state. The rollback
        // is async (moka + the pending lock) but Drop is sync, so spawn it.
        if let Some(cleanup) = self.cleanup.take() {
            match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    handle.spawn(cleanup.run());
                }
                Err(_) => {
                    warn!(
                        "queue slot dropped outside a runtime; phantom recommendation status \
                         for {} not cleared",
                        cleanup.cache_key
                    );
                }
            }
        }
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
        // The guard also keeps the depth gauge in lockstep with the counter and,
        // until it is disarmed on a successful dispatch, rolls back the phantom
        // `Pending`/`pending_requests` state we are about to record if this future
        // is dropped before the request is sent.
        let slot = QueueSlot::reserve(
            self.queue_size.clone(),
            SlotCleanup {
                status_cache: self.status_cache.clone(),
                pending_requests: self.pending_requests.clone(),
                cache_key: cache_key.clone(),
            },
        );

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
            // Dispatch failed: returning here drops the still-armed `slot` guard,
            // whose Drop releases the reservation *and* runs its cleanup — clearing
            // the in-flight status and the pending_requests entry so a later retry
            // re-enqueues instead of joining a phantom request. This is the same
            // rollback a dropped/cancelled future gets, via one code path.
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

    /// Start the recommendation warm-up worker pool.
    ///
    /// A single worker would drain the queue serially, so distinct cold warm-ups
    /// for different keys run one at a time. The queue's `request_sender` is a
    /// `broadcast` channel, and a broadcast fan-out where every worker `recv()`s
    /// the shared channel would run *each* job once per worker. So, mirroring
    /// `WorkerPool`, a dispatcher round-robins the broadcast stream across
    /// `num_workers` per-worker `mpsc` channels, letting `num_workers` distinct
    /// warm-ups run in parallel while each job is still handled exactly once.
    pub fn start_workers(
        &self,
        task_tracker: TaskTracker,
        receiver: broadcast::Receiver<RecommendationRequest>,
        cancellation_token: CancellationToken,
        num_workers: NonZeroUsize,
    ) {
        let mut worker_txs = Vec::with_capacity(num_workers.get());
        for index in 0..num_workers.get() {
            // Depth 1: just enough to let the dispatcher stage the next job for a
            // worker that is still computing, preserving round-robin spread.
            let (worker_tx, worker_rx) = mpsc::channel::<RecommendationRequest>(1);
            worker_txs.push(worker_tx);
            self.spawn_worker(&task_tracker, worker_rx, cancellation_token.clone(), index);
        }

        spawn_dispatcher(&task_tracker, receiver, worker_txs, cancellation_token);
    }

    /// Spawn one pool worker: it pulls jobs handed to it by the dispatcher and
    /// runs the warm-up compute for each.
    fn spawn_worker(
        &self,
        task_tracker: &TaskTracker,
        mut worker_rx: mpsc::Receiver<RecommendationRequest>,
        cancellation_token: CancellationToken,
        index: usize,
    ) {
        let repo = self.repo.clone();
        let status_cache = self.status_cache.clone();
        let result_cache = self.result_cache.clone();
        let pending_requests = self.pending_requests.clone();
        let queue_size = self.queue_size.clone();

        task_tracker.spawn(async move {
            info!("Recommendation queue worker {index} started");

            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        info!("Recommendation queue worker {index} shutting down");
                        break;
                    }
                    maybe_request = worker_rx.recv() => {
                        match maybe_request {
                            Some(request) => {
                                Self::process_request(
                                    request,
                                    &repo,
                                    &status_cache,
                                    &result_cache,
                                    &pending_requests,
                                    &queue_size,
                                )
                                .await;
                            }
                            None => {
                                info!("Recommendation queue worker {index} channel closed");
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    /// Run one warm-up: mark it processing, compute, and cache the result (or the
    /// failure). Shared by every pool worker so the per-job semantics — slot
    /// release, status bookkeeping, metrics — stay identical regardless of pool
    /// size.
    async fn process_request(
        request: RecommendationRequest,
        repo: &Arc<T>,
        status_cache: &Cache<String, RecommendationStatus>,
        result_cache: &Cache<String, Vec<Recommendation>>,
        pending_requests: &Arc<RwLock<Vec<String>>>,
        queue_size: &Arc<AtomicUsize>,
    ) {
        let cache_key = request.cache_key.clone();

        // Take over the queue-slot reservation the request path handed off on
        // dispatch. Its Drop releases the slot (and republishes the depth gauge)
        // when this job finishes, on every arm below, so the gauge can never
        // diverge from the counter.
        let _slot = QueueSlot::adopt(queue_size.clone());

        // Remove from pending requests and update positions
        {
            let mut pending = pending_requests.write().await;
            pending.retain(|k| k != &cache_key);

            // Update positions for remaining requests
            for (index, key) in pending.iter().enumerate() {
                status_cache
                    .insert(
                        key.clone(),
                        RecommendationStatus::Pending {
                            position: index + 1,
                        },
                    )
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
                // Record how long the warm-up compute took so operators can watch
                // warm latency.
                crate::metrics::recommend_warm_seconds().record(started.elapsed().as_secs_f64());

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

                // Count the failure distinctly from a warming cold-miss so
                // operators can tell them apart.
                crate::metrics::recommend_failed().increment(1);

                // Mark as failed
                status_cache
                    .insert(cache_key, RecommendationStatus::Failed(e.to_string()))
                    .await;
            }
        }
        // `_slot` drops here, releasing the reservation and republishing the
        // depth gauge.
    }
}

/// Round-robin the broadcast request stream across the pool's per-worker `mpsc`
/// channels — mirroring `WorkerPool`'s dispatcher — so each job is handed to
/// exactly one worker (a broadcast fan-out would run every job once per worker).
fn spawn_dispatcher(
    task_tracker: &TaskTracker,
    mut receiver: broadcast::Receiver<RecommendationRequest>,
    worker_txs: Vec<mpsc::Sender<RecommendationRequest>>,
    cancellation_token: CancellationToken,
) {
    task_tracker.spawn(async move {
        let mut worker_txs_cycle = worker_txs.iter().cycle();

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("Recommendation queue dispatcher shutting down");
                    break;
                }
                result = receiver.recv() => {
                    match result {
                        Ok(request) => {
                            let Some(worker_tx) = worker_txs_cycle.next() else {
                                error!("Recommendation queue dispatcher has no workers");
                                break;
                            };
                            if let Err(e) = worker_tx.send(request).await {
                                error!("Recommendation queue dispatcher failed to hand off to a worker: {e}");
                                break;
                            }
                        }
                        Err(RecvError::Lagged(n)) => {
                            warn!("Recommendation queue dispatcher lagged and missed {n} requests");
                        }
                        Err(RecvError::Closed) => {
                            info!("Recommendation queue request channel closed");
                            break;
                        }
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::{Recommendation, RepoError};
    use nostr_sdk::Keys;
    use std::num::NonZeroUsize;
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

    /// A repo that records the peak number of `get_recommendations` calls running
    /// at once, so a test can prove distinct warm-ups run in parallel across a
    /// worker pool (peak == pool size) rather than serially (peak == 1).
    struct ConcurrencyRepo {
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
        hold: Duration,
    }

    impl RepoTrait for ConcurrencyRepo {
        async fn get_recommendations(
            &self,
            _pubkey: &PublicKey,
            _min_pagerank: Option<f64>,
            _limit: Option<usize>,
        ) -> Result<Vec<Recommendation>, RepoError> {
            let now = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(now, Ordering::SeqCst);
            // Hold the "compute" long enough that genuinely parallel workers
            // overlap inside this call before any of them releases.
            tokio::time::sleep(self.hold).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(Vec::new())
        }
    }

    fn queue_with_worker(calls: Arc<AtomicUsize>) -> RecommendationQueue<CountingRepo> {
        let repo = Arc::new(CountingRepo { calls });
        let result_cache = Cache::builder().build();
        let (queue, receiver) = RecommendationQueue::new(repo, result_cache, 64);
        queue.start_workers(
            TaskTracker::new(),
            receiver,
            CancellationToken::new(),
            NonZeroUsize::new(1).unwrap(),
        );
        queue
    }

    /// Build a queue backed by a `ConcurrencyRepo` with a `num_workers`-wide pool,
    /// returning the queue plus the peak-concurrency counter the test asserts on.
    fn queue_with_pool(
        num_workers: usize,
        hold: Duration,
    ) -> (RecommendationQueue<ConcurrencyRepo>, Arc<AtomicUsize>) {
        let max_active = Arc::new(AtomicUsize::new(0));
        let repo = Arc::new(ConcurrencyRepo {
            active: Arc::new(AtomicUsize::new(0)),
            max_active: max_active.clone(),
            hold,
        });
        let result_cache = Cache::builder().build();
        let (queue, receiver) = RecommendationQueue::new(repo, result_cache, 64);
        queue.start_workers(
            TaskTracker::new(),
            receiver,
            CancellationToken::new(),
            NonZeroUsize::new(num_workers).unwrap(),
        );
        (queue, max_active)
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

    /// Beyond releasing the slot, a request future dropped after it recorded its
    /// in-flight `Pending` status but before it dispatched must also clear that
    /// phantom status (and its pending_requests entry). Otherwise every later
    /// retry for the key would join a `Pending` that no worker will ever complete
    /// and wedge until the 5-minute status TTL expires.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_request_cancelled_before_dispatch_clears_the_phantom_pending_status() {
        let calls = Arc::new(AtomicUsize::new(0));
        let queue = Arc::new(queue_with_worker(calls));
        let pov = Keys::generate().public_key();
        let cache_key = cache_key_for(&pov, None, Some(10));

        // Park the winning request after it reserves its slot and records
        // `Pending` but before it can dispatch, by holding the pending lock.
        let lock = queue.pending_requests.write().await;

        let request = {
            let queue = queue.clone();
            tokio::spawn(async move { queue.request_recommendations(pov, None, Some(10)).await })
        };

        // Wait until it has parked (slot reserved == Pending already recorded).
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
            "the winning request reserves its slot and records Pending before the await"
        );
        assert!(
            matches!(
                queue.status_cache.get(&cache_key).await,
                Some(RecommendationStatus::Pending { .. })
            ),
            "a Pending status is recorded before dispatch"
        );

        // Cancel mid-await, then release the lock so the spawned cleanup can run.
        request.abort();
        let _ = request.await;
        drop(lock);

        // The guard's cleanup must clear the phantom Pending, the pending entry,
        // and the slot.
        let mut cleared = false;
        for _ in 0..500 {
            if queue.status_cache.get(&cache_key).await.is_none()
                && queue.pending_requests.read().await.is_empty()
                && queue.queue_size.load(Ordering::SeqCst) == 0
            {
                cleared = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(
            cleared,
            "a cancelled request must clear its phantom Pending status, pending entry, and slot"
        );

        // A fresh request for the same key must re-enqueue a new warm-up rather
        // than join the (now cleared) phantom — proving the wedge is gone.
        let status = queue
            .request_recommendations(pov, None, Some(10))
            .await
            .expect("the retry re-enqueues");
        assert!(
            matches!(status, RecommendationStatus::Pending { .. }),
            "the retry starts a new warm-up instead of joining a phantom"
        );
    }

    /// A disarmed slot is the successful-dispatch handoff: the worker now owns the
    /// status, so the request-path guard must NOT clear it (nor release the slot —
    /// the worker's adopted guard does that on completion).
    #[tokio::test]
    async fn a_disarmed_queue_slot_keeps_the_pending_status() {
        let status_cache: Cache<String, RecommendationStatus> = Cache::builder().build();
        let pending: Arc<RwLock<Vec<String>>> = Arc::new(RwLock::new(Vec::new()));
        let queue_size = Arc::new(AtomicUsize::new(0));
        let key = "k".to_string();

        status_cache
            .insert(key.clone(), RecommendationStatus::Pending { position: 1 })
            .await;
        pending.write().await.push(key.clone());

        let slot = QueueSlot::reserve(
            queue_size.clone(),
            SlotCleanup {
                status_cache: status_cache.clone(),
                pending_requests: pending.clone(),
                cache_key: key.clone(),
            },
        );
        assert_eq!(queue_size.load(Ordering::Relaxed), 1);

        // Successful dispatch hands the release to the worker.
        slot.disarm();

        // Give any (erroneously) spawned cleanup a chance to run before asserting.
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }

        assert!(
            matches!(
                status_cache.get(&key).await,
                Some(RecommendationStatus::Pending { .. })
            ),
            "a disarmed slot must not clear the status the worker now owns"
        );
        assert!(
            !pending.read().await.is_empty(),
            "a disarmed slot must not touch pending_requests"
        );
        assert_eq!(
            queue_size.load(Ordering::Relaxed),
            1,
            "a disarmed slot leaves the counter for the worker's adopted release"
        );
    }

    /// Two cold warm-ups for *distinct* keys must run at the same time when the
    /// pool has more than one worker, instead of serializing behind a single
    /// worker. Regression test for the single-worker drain.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn distinct_cold_warmups_run_in_parallel_across_the_pool() {
        let (queue, max_active) = queue_with_pool(2, Duration::from_millis(80));
        let queue = Arc::new(queue);
        let pk1 = Keys::generate().public_key();
        let pk2 = Keys::generate().public_key();

        queue
            .request_recommendations(pk1, None, Some(10))
            .await
            .expect("first request is accepted");
        queue
            .request_recommendations(pk2, None, Some(10))
            .await
            .expect("second request is accepted");

        wait_for_drain(&queue).await;

        assert_eq!(
            max_active.load(Ordering::SeqCst),
            2,
            "distinct warm-ups must overlap with a 2-worker pool"
        );
    }

    /// A pool sized to one worker still serializes distinct warm-ups, proving the
    /// configured pool size is honored (and that the parallelism above is not an
    /// artifact of the test always overlapping).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_single_worker_pool_serializes_distinct_warmups() {
        let (queue, max_active) = queue_with_pool(1, Duration::from_millis(40));
        let queue = Arc::new(queue);
        let pk1 = Keys::generate().public_key();
        let pk2 = Keys::generate().public_key();

        queue
            .request_recommendations(pk1, None, Some(10))
            .await
            .expect("first request is accepted");
        queue
            .request_recommendations(pk2, None, Some(10))
            .await
            .expect("second request is accepted");

        wait_for_drain(&queue).await;

        assert_eq!(
            max_active.load(Ordering::SeqCst),
            1,
            "a single-worker pool must run distinct warm-ups one at a time"
        );
    }

    /// A throwaway cleanup for the counter-focused slot tests, which do not
    /// exercise the phantom-state rollback.
    fn unused_cleanup() -> SlotCleanup {
        SlotCleanup {
            status_cache: Cache::builder().build(),
            pending_requests: Arc::new(RwLock::new(Vec::new())),
            cache_key: "unused".to_string(),
        }
    }

    #[test]
    fn reserve_then_drop_conserves_the_slot_counter() {
        let counter = Arc::new(AtomicUsize::new(0));
        {
            let _slot = QueueSlot::reserve(counter.clone(), unused_cleanup());
            assert_eq!(counter.load(Ordering::SeqCst), 1, "reserve takes a slot");
        }
        assert_eq!(counter.load(Ordering::SeqCst), 0, "drop releases the slot");
    }

    #[test]
    fn a_disarmed_slot_does_not_release_on_drop() {
        let counter = Arc::new(AtomicUsize::new(0));
        let slot = QueueSlot::reserve(counter.clone(), unused_cleanup());
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
