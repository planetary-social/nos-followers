use anyhow::Result;
use futures::Future;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
pub struct WorkerPool {}

impl WorkerPool {
    pub fn start<T, W>(
        num_workers: usize,
        mut event_rx: mpsc::Receiver<T>,
        cancellation_token: CancellationToken,
        worker: W,
    ) -> Result<TaskTracker>
    where
        T: Send + 'static,
        W: WorkerTask<T> + Send + Sync + 'static,
    {
        let tracker = TaskTracker::new();

        // Spawn a pool of worker tasks to process each event. Call the worker_fn for each event.
        let mut worker_txs = Vec::new();

        let worker_clone = Arc::new(worker);
        for _ in 0..num_workers {
            let (worker_tx, mut worker_rx) = mpsc::channel::<T>(100);
            worker_txs.push(worker_tx);

            let tracker = tracker.clone();

            let w = worker_clone.clone();
            tracker.spawn(async move {
                while let Some(event) = worker_rx.recv().await {
                    w.call(event).await
                }
            });
        }

        let token_clone = cancellation_token.clone();

        // Dispatcher task to worker pool
        tracker.clone().spawn({
            let worker_txs = worker_txs.clone();

            async move {
                while let Some(event) = event_rx.recv().await {
                    if token_clone.is_cancelled() {
                        break;
                    }

                    // Load balance events to workers based on simple current millis
                    let milliseconds_timestamp: u128 = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis();

                    let index = (milliseconds_timestamp % num_workers as u128) as usize;

                    if let Err(e) = worker_txs[index].send(event).await {
                        eprintln!("Failed to send to worker: {:?}", e);
                    }
                }
            }
        });

        Ok(tracker)
    }
}

pub trait WorkerTask<T> {
    fn call(&self, args: T) -> impl Future<Output = ()> + std::marker::Send;
}
