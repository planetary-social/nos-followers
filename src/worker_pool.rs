use crate::send_with_checks::SendWithChecks;
use anyhow::Result;
use futures::Future;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::error;
pub struct WorkerPool {}

// A channel based worker pool that distributes work to a pool of workers.
// Items come through the item_rx channel and are distributed to workers.
// Workers implement the WorkerTask trait that receives the item to process.
impl WorkerPool {
    pub fn start<Item, Worker>(
        num_workers: usize,
        mut item_rx: mpsc::Receiver<Item>,
        cancellation_token: CancellationToken,
        worker: Worker,
    ) -> Result<TaskTracker>
    where
        Item: Send + 'static,
        Worker: WorkerTask<Item> + Send + Sync + 'static,
    {
        let tracker = TaskTracker::new();

        // Spawn a pool of worker tasks to process each item. Call the worker_fn for each item.
        let mut worker_txs = Vec::new();

        let worker_clone = Arc::new(worker);
        for _ in 0..num_workers {
            let (worker_tx, mut worker_rx) = mpsc::channel::<Item>(10);
            worker_txs.push(worker_tx);

            let tracker = tracker.clone();

            let w = worker_clone.clone();
            tracker.spawn(async move {
                while let Some(item) = worker_rx.recv().await {
                    if let Err(e) = w.call(item).await {
                        error!("Worker failed: {:?}", e);
                    }
                }
            });
        }

        let token_clone = cancellation_token.clone();

        // Dispatcher task to worker pool
        tracker.clone().spawn({
            async move {
                // Simple cycle iterator to distribute work to workers in a round-robin fashion.
                let mut worker_txs_cycle = worker_txs.iter().cycle();

                while let Some(item) = item_rx.recv().await {
                    if token_clone.is_cancelled() {
                        break;
                    }

                    let Some(worker_tx) = worker_txs_cycle.next() else {
                        error!("Failed to get worker");
                        break;
                    };

                    if let Err(e) = worker_tx.send_with_checks(item) {
                        error!("Failed to send to worker: {:?}", e);
                        break;
                    }
                }
            }
        });

        Ok(tracker)
    }
}

pub trait WorkerTask<T> {
    fn call(&self, args: T) -> impl Future<Output = Result<()>> + std::marker::Send;
}
