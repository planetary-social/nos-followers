use futures::Future;
use metrics::counter;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::{
    broadcast::{self, error::RecvError},
    mpsc,
};
use tokio::time::{timeout, Duration};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};
pub struct WorkerPool {}

// A channel based worker pool that distributes work to a pool of workers.
// Items come through the item_rx channel and are distributed to workers.
// Workers implement the WorkerTask trait that receives the item to process.
impl WorkerPool {
    pub fn start<Item, Worker>(
        num_workers: usize,
        worker_timeout_secs: u64,
        mut item_receiver: broadcast::Receiver<Item>,
        cancellation_token: CancellationToken,
        worker: Worker,
    ) -> Result<TaskTracker, Box<dyn Error>>
    where
        Item: Send + Clone + 'static,
        Worker: WorkerTask<Item> + Send + Sync + 'static,
    {
        let tracker = TaskTracker::new();

        // Spawn a pool of worker tasks to process each item. Call the worker_fn for each item.
        let mut worker_txs = Vec::new();

        let worker_clone = Arc::new(worker);
        let token_clone = cancellation_token.clone();

        for _ in 0..num_workers {
            let (worker_tx, mut worker_rx) = mpsc::channel::<WorkerTaskItem<Item>>(1);
            worker_txs.push(worker_tx);

            //let tracker = tracker.clone();

            let worker = worker_clone.clone();
            let token_clone = token_clone.clone();
            tracker.spawn(async move {
                loop {
                    tokio::select! {
                        _ = token_clone.cancelled() => {
                            info!("Cancellation token is cancelled, stopping worker");
                            break;
                        }

                        Some(item) = worker_rx.recv() => {
                              let result = timeout(Duration::from_secs(worker_timeout_secs), worker.call(item)).await;

                              match result {
                                  Ok(Ok(())) => {},
                                  Ok(Err(e)) => {
                                      counter!("worker_failures").increment(1);
                                      error!("Worker failed: {}", e);
                                  },
                                  Err(_) => {
                                      counter!("worker_timeouts").increment(1);
                                      error!("Worker task timed out after {} seconds", worker_timeout_secs);
                                  }
                              }
                        }
                    }
                }

                token_clone.cancel();
                info!("Worker task finished");
            });
        }

        let token_clone = token_clone.clone();

        // Dispatcher task to worker pool
        tracker.spawn(async move {
            // Simple cycle iterator to distribute work to workers in a round-robin fashion.
            let mut worker_txs_cycle = worker_txs.iter().cycle();

            loop {
                tokio::select! {
                    _ = token_clone.cancelled() => {
                        info!("Cancellation token is cancelled, stopping worker pool");
                        break;
                    }

                    result = item_receiver.recv() => {
                        match result {
                            Ok(item) => {
                                let Some(worker_tx) = worker_txs_cycle.next() else {
                                    error!("Failed to get worker");
                                    break;
                                };

                                let worker_item = WorkerTaskItem { item };

                                if let Err(e) = worker_tx.send(worker_item).await {
                                    error!("Failed to send to worker: {}", e);
                                    break;
                                }
                            }
                            Err(RecvError::Lagged(n)) => {
                                counter!("worker_lagged").increment(1);
                                warn!("Receiver lagged and missed {} messages", n);
                            }
                            Err(RecvError::Closed) => {
                                counter!("worker_closed").increment(1);
                                error!("Item receiver channel closed");
                                break;
                            }
                        }
                    }
                }
            }

            token_clone.cancel();
            info!("Worker pool finished");
        });

        Ok(tracker)
    }
}

pub trait WorkerTask<T> {
    fn call(
        &self,
        args: WorkerTaskItem<T>,
    ) -> impl Future<Output = Result<(), Box<dyn Error>>> + std::marker::Send;
}

pub struct WorkerTaskItem<T> {
    pub item: T,
}

impl<T> WorkerTaskItem<T> {
    pub fn new(item: T) -> Self {
        Self { item }
    }
}
