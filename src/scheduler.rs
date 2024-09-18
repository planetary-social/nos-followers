use crate::metrics;
use crate::{config::Settings, repo::RepoTrait};
use anyhow::Result;
use std::sync::Arc;
use tokio::time::{Duration, Instant};
use tokio_cron_scheduler::{Job, JobScheduler};
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{error, info};

pub async fn start_scheduler<T>(
    task_tracker: TaskTracker,
    repo: Arc<T>,
    cancellation_token: CancellationToken,
    settings: &Settings,
) -> Result<()>
where
    T: RepoTrait + 'static,
{
    let mut sched = JobScheduler::new().await?;
    let cron_expression = settings.pagerank_cron_expression.as_str();
    let repo_clone = Arc::clone(&repo);

    let job = Job::new_async(cron_expression, move |_uuid, _l| {
        let repo_inner = Arc::clone(&repo_clone);

        Box::pin(async move {
            info!("Starting scheduled PageRank update...");

            let start_time = Instant::now();

            let retry_strategy = ExponentialBackoff::from_millis(100)
                .max_delay(Duration::from_secs(10))
                .map(jitter);

            let result = Retry::spawn(retry_strategy, || async {
                if let Err(e) = repo_inner.update_memory_graph().await {
                    error!("Memory graph update failed: {:?}", e);
                    return Err(e);
                }

                if let Err(e) = repo_inner.update_pagerank().await {
                    error!("Failed to update PageRank: {:?}", e);
                    return Err(e);
                }

                Ok(())
            })
            .await;

            let elapsed = start_time.elapsed();

            match result {
                Ok(_) => info!("PageRank updated successfully in {:?}", elapsed),
                Err(e) => error!(
                    "Failed to update PageRank after retries in {:?}: {:?}",
                    elapsed, e
                ),
            }

            metrics::pagerank_seconds().set(elapsed.as_secs_f64());
        })
    })?;

    sched.add(job).await?;
    sched.start().await?;

    task_tracker.spawn(async move {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                info!("Scheduler task cancelled, shutting down scheduler.");
                if let Err(e) = sched.shutdown().await {
                    error!("Failed to shut down scheduler: {:?}", e);
                }
            }
        }
    });

    Ok(())
}
