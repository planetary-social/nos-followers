use metrics::{describe_counter, describe_gauge, describe_histogram, Counter, Gauge, Histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::OnceLock;

pub struct Metrics {
    pub pubsub_messages: Counter,
    pub contact_lists_processed: Counter,
    pub follows: Counter,
    pub unfollows: Counter,
    pub worker_lagged: Counter,
    pub worker_closed: Counter,
    pub verified_nip05: Counter,
    pub individual_follow_messages: Counter,
    pub aggregated_follow_messages: Counter,
    pub followers_per_message: Histogram,
    pub unfollowers_per_message: Histogram,
    pub retained_follow_changes: Gauge,
}

// Global OnceLock to hold the Metrics instance
static METRICS: OnceLock<Metrics> = OnceLock::new();

impl Metrics {
    fn new() -> Self {
        Self {
            pubsub_messages: metrics::counter!("pubsub_messages"),
            contact_lists_processed: metrics::counter!("contact_lists_processed"),
            follows: metrics::counter!("follows"),
            unfollows: metrics::counter!("unfollows"),
            worker_lagged: metrics::counter!("worker_lagged"),
            worker_closed: metrics::counter!("worker_closed"),
            verified_nip05: metrics::counter!("verified_nip05"),
            individual_follow_messages: metrics::counter!("individual_follow_messages"),
            aggregated_follow_messages: metrics::counter!("aggregated_follow_messages"),
            followers_per_message: metrics::histogram!("followers_per_message"),
            unfollowers_per_message: metrics::histogram!("unfollowers_per_message"),
            retained_follow_changes: metrics::gauge!("retained_follow_changes"),
        }
    }

    pub fn worker_failures(&self, name: String, id: usize) -> Counter {
        metrics::counter!("worker_failures", "name" => name, "id" => id.to_string())
    }

    pub fn worker_timeouts(&self, name: String, id: usize) -> Counter {
        metrics::counter!("worker_timeouts", "name" => name, "id" => id.to_string())
    }
}

// Function to access the initialized metrics
pub fn get_metrics() -> &'static Metrics {
    METRICS.get_or_init(Metrics::new)
}

// Setup metrics with descriptions
pub fn setup_metrics() -> Result<PrometheusHandle, anyhow::Error> {
    describe_counter!(
        "pubsub_messages",
        "Number of messages published to Google Pub/Sub"
    );
    describe_counter!(
        "contact_lists_processed",
        "Number of contact lists processed"
    );
    describe_counter!("follows", "Number of follows");
    describe_counter!("unfollows", "Number of unfollows");
    describe_counter!(
        "worker_lagged",
        "Number of times a worker lagged behind and missed messages, consider increasing worker pool size or channel buffer size"
    );
    describe_counter!("worker_closed", "Number of times a worker channel closed");
    describe_counter!(
        "worker_failures",
        "Number of times a worker failed to process an item"
    );
    describe_counter!("worker_timeouts", "Number of times a worker timed out");
    describe_counter!("verified_nip05", "Number of verified NIP05 ids fetched");

    describe_counter!(
        "individual_follow_messages",
        "Total number of individual follow messages sent"
    );
    describe_counter!(
        "aggregated_follow_messages",
        "Total number of aggregated follow messages sent"
    );

    describe_histogram!(
        "followers_per_message",
        "Number of followers per aggregated message"
    );
    describe_histogram!(
        "unfollowers_per_message",
        "Number of unfollowers per aggregated message"
    );
    describe_gauge!(
        "retained_follow_changes",
        "Number of retained follow changes"
    );

    // Prometheus setup
    let prometheus_builder = PrometheusBuilder::new();
    let prometheus_handle = prometheus_builder.install_recorder()?;
    Ok(prometheus_handle)
}
