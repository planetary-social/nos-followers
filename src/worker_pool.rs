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
        mut item_receiver: mpsc::Receiver<Item>,
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
            let (worker_tx, mut worker_rx) = mpsc::channel::<WorkerTaskItem<Item>>(10);
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
                let max_capacity = item_receiver.max_capacity();

                while let Some(item) = item_receiver.recv().await {
                    if token_clone.is_cancelled() {
                        break;
                    }

                    let Some(worker_tx) = worker_txs_cycle.next() else {
                        error!("Failed to get worker");
                        break;
                    };

                    let channel_load =
                        ((max_capacity - item_receiver.capacity()) * 100 / max_capacity) as u8;
                    let worker_item = WorkerTaskItem {
                        item,
                        channel_load,
                        max_capacity,
                    };
                    if let Err(e) = worker_tx.send_with_checks(worker_item) {
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
    fn call(&self, args: WorkerTaskItem<T>)
        -> impl Future<Output = Result<()>> + std::marker::Send;
}

pub struct WorkerTaskItem<T> {
    pub item: T,
    // The percentage of the channel's capacity that was used when this item was sent.
    pub channel_load: u8,
    pub max_capacity: usize,
}
