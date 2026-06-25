use anyhow::{bail, Result};
use async_trait::async_trait;
use nostr_sdk::prelude::*;
use std::marker::Send;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::Sender;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, error, info};

#[async_trait]
pub trait GetEventsOf: Send + Sync {
    async fn get_events_of(
        &self,
        filters: Vec<Filter>,
        timeout: Option<Duration>,
    ) -> Result<Vec<Event>, Error>;
}

#[async_trait]
impl GetEventsOf for Client {
    async fn get_events_of(
        &self,
        filters: Vec<Filter>,
        timeout: Option<Duration>,
    ) -> Result<Vec<Event>, Error> {
        self.get_events_of(filters, timeout).await
    }
}

pub fn create_client() -> Client {
    let opts = Options::default()
        .skip_disconnected_relays(true)
        .wait_for_send(false)
        .connection_timeout(Some(Duration::from_secs(5)))
        .send_timeout(Some(Duration::from_secs(5)))
        .wait_for_subscription(true);

    ClientBuilder::default().opts(opts).build()
}

/// Mark the relay subscription up: set the shared readiness flag (read by
/// `GET /health`) and the `relay_subscription_up` metric together, so the probe
/// and the gauge can never disagree about whether we are subscribed.
fn mark_subscription_up(relay_subscribed: &AtomicBool) {
    relay_subscribed.store(true, Ordering::Relaxed);
    crate::metrics::relay_subscription_up().set(1.0);
}

/// Mark the relay subscription down: the readiness flag and the metric move
/// together (see [`mark_subscription_up`]).
fn mark_subscription_down(relay_subscribed: &AtomicBool) {
    relay_subscribed.store(false, Ordering::Relaxed);
    crate::metrics::relay_subscription_up().set(0.0);
}

#[allow(clippy::too_many_arguments)]
pub fn start_nostr_subscription(
    task_tracker: TaskTracker,
    nostr_client: Arc<Client>,
    relays: Vec<String>,
    filters: Vec<Filter>,
    event_tx: Sender<Box<Event>>,
    cancellation_token: CancellationToken,
    relay_subscribed: Arc<AtomicBool>,
) {
    task_tracker.spawn(async move {
        // Publish the liveness series up front (not yet subscribed) so the gauge
        // is present on /metrics from startup — a never-coming-up subscription is
        // then alertable as `relay_subscription_up == 0`, not merely absent. The
        // readiness flag starts false too, so `/health` reports not-ready until we
        // actually subscribe.
        mark_subscription_down(&relay_subscribed);

        for relay in relays {
            info!("Connecting to relay: {}", relay);
            if let Err(e) = nostr_client.add_relay(relay.clone()).await {
                // A bad relay must not take the process down; log and continue so
                // the remaining relays — and the HTTP server — keep working.
                error!("Failed to add relay {}: {}", relay, e);
            }
        }

        nostr_client.connect().await;

        // Tear the client down once, when the shared token is cancelled.
        spawn_subscription_shutdown(nostr_client.clone(), cancellation_token.clone());

        // Keep the subscription alive across transient relay failures. This loop
        // owns the relay lifecycle and never cancels the shared token, so a relay
        // outage can no longer take the HTTP server down with it.
        retry_until_cancelled(
            cancellation_token.clone(),
            relay_subscribed.clone(),
            move || {
                let client = nostr_client.clone();
                let filters = filters.clone();
                let event_tx = event_tx.clone();
                let cancellation_token = cancellation_token.clone();
                let relay_subscribed = relay_subscribed.clone();
                async move {
                    run_subscription(
                        &client,
                        &filters,
                        event_tx,
                        &cancellation_token,
                        &relay_subscribed,
                    )
                    .await
                }
            },
        )
        .await;

        info!("Relay subscription loop ended");
    });
}

/// On shutdown (the shared token is cancelled), unsubscribe and shut the client
/// down. Spawned once per subscription, independent of the retry loop.
fn spawn_subscription_shutdown(client: Arc<Client>, cancellation_token: CancellationToken) {
    // Own a `Client` (cheap — it is Arc-internal) so `shutdown`, which consumes
    // self, can be called after the `&self` `unsubscribe_all`.
    let client = client.as_ref().clone();
    tokio::spawn(async move {
        cancellation_token.cancelled().await;
        debug!("Cancelling relay subscription worker");
        client.unsubscribe_all().await;
        if let Err(e) = client.shutdown().await {
            error!("Failed to shutdown client: {}", e);
        }
    });
}

/// One subscription attempt: (re)subscribe and pump notifications until the relay
/// stream ends or the shared token is cancelled. Returns `Err` on a subscribe
/// failure so the retry loop can back off — but never cancels the shared token,
/// so a relay problem cannot tear down the HTTP server or the worker pools.
async fn run_subscription(
    client: &Client,
    filters: &[Filter],
    event_tx: Sender<Box<Event>>,
    cancellation_token: &CancellationToken,
    relay_subscribed: &AtomicBool,
) -> Result<()> {
    info!("Subscribing to {:?}", filters);

    if let Err(e) = client.subscribe(filters.to_vec(), None).await {
        // Do NOT cancel the shared token; surface the error so the retry loop
        // backs off and tries again.
        bail!("Failed to subscribe: {}", e);
    }

    // Subscribed and about to pump notifications: the subscription is up. The
    // retry loop drives it back down when this attempt ends.
    mark_subscription_up(relay_subscribed);

    client
        .handle_notifications(|notification| async {
            if cancellation_token.is_cancelled() {
                return Ok(true);
            }

            if let RelayPoolNotification::Event { event, .. } = notification {
                debug!("Received event: {}", event.id);
                if let Err(e) = event_tx.send(event) {
                    // No receivers left; leave the notification loop so the retry
                    // loop can re-establish later. Do NOT cancel the shared token.
                    error!("Failed to send nostr event: {}", e);
                    return Ok(true);
                }
            }

            // True would exit from the loop
            Ok(false)
        })
        .await?;

    Ok(())
}

/// Backoff bounds for re-establishing a dropped relay subscription.
const INITIAL_RELAY_BACKOFF: Duration = Duration::from_secs(1);
const MAX_RELAY_BACKOFF: Duration = Duration::from_secs(60);

/// Run `attempt` repeatedly with capped exponential backoff until the shared
/// `cancellation_token` is cancelled (graceful shutdown). A failed attempt is
/// logged and retried — crucially it must NOT cancel the token, because the same
/// token drives the HTTP server and the worker pools: a relay outage must never
/// tear those down. Returns only when the token is cancelled.
async fn retry_until_cancelled<F, Fut>(
    cancellation_token: CancellationToken,
    relay_subscribed: Arc<AtomicBool>,
    mut attempt: F,
) where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let mut backoff = INITIAL_RELAY_BACKOFF;
    loop {
        if cancellation_token.is_cancelled() {
            return;
        }

        // Run the attempt, but abandon it immediately on shutdown.
        let outcome = tokio::select! {
            biased;
            _ = cancellation_token.cancelled() => return,
            outcome = attempt() => outcome,
        };

        match outcome {
            // A clean end (e.g. the relay closed the stream): reconnect after the
            // base delay.
            Ok(()) => backoff = INITIAL_RELAY_BACKOFF,
            // A failure must not cancel the shared token — log it and back off.
            Err(e) => error!("Relay subscription failed; retrying in {backoff:?}: {e:#}"),
        }

        // The attempt has ended — the subscription is no longer up. Publish that
        // before backing off (readiness flag + gauge together) so a persistent
        // outage (or a dispatcher-death that keeps re-subscribing) stays
        // observable as `relay_subscription_up == 0` on /metrics and as a 503 on
        // /health. This never cancels the shared token: the decoupling holds.
        mark_subscription_down(&relay_subscribed);

        // Wait out the backoff, but wake immediately on shutdown.
        tokio::select! {
            biased;
            _ = cancellation_token.cancelled() => return,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(MAX_RELAY_BACKOFF);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::mpsc;

    #[tokio::test(start_paused = true)]
    async fn relay_failures_are_retried_and_never_cancel_the_shared_token() {
        let cancellation_token = CancellationToken::new();
        let attempts = Arc::new(AtomicUsize::new(0));
        let (tx, mut rx) = mpsc::unbounded_channel::<()>();

        let handle = tokio::spawn({
            let cancellation_token = cancellation_token.clone();
            let attempts = attempts.clone();
            async move {
                retry_until_cancelled(
                    cancellation_token,
                    Arc::new(AtomicBool::new(false)),
                    move || {
                        let attempts = attempts.clone();
                        let tx = tx.clone();
                        async move {
                            attempts.fetch_add(1, Ordering::SeqCst);
                            let _ = tx.send(());
                            bail!("relay unavailable")
                        }
                    },
                )
                .await
            }
        });

        // Observe two failed attempts, proving the loop retries through the
        // backoff. Awaiting the channel parks this task so the paused clock
        // auto-advances through the sleep.
        rx.recv().await.expect("first attempt fires");
        rx.recv().await.expect("a failed attempt is retried");

        // The crux of the fix: repeated relay failures must NOT cancel the shared
        // lifecycle token (which also drives the HTTP server and workers).
        assert!(
            !cancellation_token.is_cancelled(),
            "relay failures must not cancel the shared lifecycle token"
        );

        // Graceful shutdown still stops the loop.
        cancellation_token.cancel();
        handle
            .await
            .expect("the retry loop exits when the token is cancelled");
    }

    #[tokio::test(start_paused = true)]
    async fn an_already_cancelled_token_runs_no_attempt() {
        let cancellation_token = CancellationToken::new();
        cancellation_token.cancel();
        let attempts = Arc::new(AtomicUsize::new(0));

        let attempts_clone = attempts.clone();
        retry_until_cancelled(
            cancellation_token,
            Arc::new(AtomicBool::new(false)),
            move || {
                let attempts = attempts_clone.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
        )
        .await;

        assert_eq!(
            attempts.load(Ordering::SeqCst),
            0,
            "an already-cancelled token must skip the attempt entirely"
        );
    }

    /// Render the metrics a closure emits against a test-local Prometheus
    /// recorder, so a test can assert the relay-liveness gauge without touching
    /// global metric state or racing other tests.
    fn render_relay_metrics<F, Fut>(body: F) -> String
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        ::metrics::with_local_recorder(&recorder, || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("current-thread runtime")
                .block_on(body());
        });
        handle.render()
    }

    /// A failed attempt must drive `relay_subscription_up` to 0 before backing
    /// off, so a persistent relay outage stays observable on /metrics even though
    /// it no longer cancels the shared token (the decoupling must stay silent in
    /// every other respect).
    #[test]
    fn a_failed_attempt_marks_the_subscription_down() {
        let relay_subscribed = Arc::new(AtomicBool::new(true));
        let flag = relay_subscribed.clone();
        let rendered = render_relay_metrics(|| async move {
            // Pretend the subscription had come up, then let one attempt fail.
            crate::metrics::relay_subscription_up().set(1.0);
            let token = CancellationToken::new();
            let attempt_token = token.clone();
            retry_until_cancelled(token.clone(), flag, move || {
                let attempt_token = attempt_token.clone();
                async move {
                    // Stop the loop after this single failed attempt.
                    attempt_token.cancel();
                    bail!("relay unavailable")
                }
            })
            .await;
        });

        assert!(
            rendered.contains("relay_subscription_up 0"),
            "a failed attempt must mark the subscription down; got:\n{rendered}"
        );
        assert!(
            !relay_subscribed.load(Ordering::Relaxed),
            "a failed attempt must also flip the /health readiness flag to not-ready"
        );
    }

    /// A clean return (the relay closed the stream, or no receivers remain) must
    /// also drive the gauge to 0 before re-looping, so a dispatcher-death that
    /// keeps re-subscribing is held at 0 and stays alertable.
    #[test]
    fn a_clean_return_marks_the_subscription_down() {
        let relay_subscribed = Arc::new(AtomicBool::new(true));
        let flag = relay_subscribed.clone();
        let rendered = render_relay_metrics(|| async move {
            crate::metrics::relay_subscription_up().set(1.0);
            let token = CancellationToken::new();
            let attempt_token = token.clone();
            retry_until_cancelled(token.clone(), flag, move || {
                let attempt_token = attempt_token.clone();
                async move {
                    attempt_token.cancel();
                    Ok(())
                }
            })
            .await;
        });

        assert!(
            rendered.contains("relay_subscription_up 0"),
            "a clean return must mark the subscription down; got:\n{rendered}"
        );
        assert!(
            !relay_subscribed.load(Ordering::Relaxed),
            "a clean return must also flip the /health readiness flag to not-ready"
        );
    }
}
