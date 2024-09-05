use metrics::{describe_counter, describe_gauge, describe_histogram, Counter, Gauge, Histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

// PubSub messages counter
pub fn pubsub_messages() -> Counter {
    metrics::counter!("pubsub_messages")
}

// Contact lists processed counter
pub fn contact_lists_processed() -> Counter {
    metrics::counter!("contact_lists_processed")
}

// Follows counter
pub fn follows() -> Counter {
    metrics::counter!("follows")
}

// Unfollows counter
pub fn unfollows() -> Counter {
    metrics::counter!("unfollows")
}

// Worker lagged counter
pub fn worker_lagged() -> Counter {
    metrics::counter!("worker_lagged")
}

// Worker closed counter
pub fn worker_closed() -> Counter {
    metrics::counter!("worker_closed")
}

// Verified NIP05 counter
pub fn verified_nip05() -> Counter {
    metrics::counter!("verified_nip05")
}

// Individual follow messages counter
pub fn individual_follow_messages() -> Counter {
    metrics::counter!("individual_follow_messages")
}

// Aggregated follow messages counter
pub fn aggregated_follow_messages() -> Counter {
    metrics::counter!("aggregated_follow_messages")
}

// Worker failures counter (with labels)
pub fn worker_failures(name: String, id: usize) -> Counter {
    metrics::counter!("worker_failures", "name" => name, "id" => id.to_string())
}

// Worker timeouts counter (with labels)
pub fn worker_timeouts(name: String, id: usize) -> Counter {
    metrics::counter!("worker_timeouts", "name" => name, "id" => id.to_string())
}

// Followers per message histogram
pub fn followers_per_message() -> Histogram {
    metrics::histogram!("followers_per_message")
}

// Unfollowers per message histogram
pub fn unfollowers_per_message() -> Histogram {
    metrics::histogram!("unfollowers_per_message")
}

// Retained follow changes gauge
pub fn retained_follow_changes() -> Gauge {
    metrics::gauge!("retained_follow_changes")
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
    describe_counter!("worker_lagged", "Number of times a worker lagged behind");
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