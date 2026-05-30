use metrics::{describe_counter, describe_gauge, describe_histogram, Counter, Gauge, Histogram};
use metrics_exporter_prometheus::{BuildError, PrometheusBuilder, PrometheusHandle};

pub fn pubsub_messages() -> Counter {
    metrics::counter!("pubsub_messages")
}

pub fn contact_lists_processed() -> Counter {
    metrics::counter!("contact_lists_processed")
}

pub fn already_seen_contact_lists() -> Counter {
    metrics::counter!("already_seen_contact_lists")
}

pub fn sudden_follow_drops() -> Counter {
    metrics::counter!("sudden_follow_drops")
}

pub fn nos_sudden_follow_drops() -> Counter {
    metrics::counter!("nos_sudden_follow_drops")
}

pub fn follows() -> Counter {
    metrics::counter!("follows")
}

pub fn unfollows() -> Counter {
    metrics::counter!("unfollows")
}

pub fn worker_lagged(name: String) -> Counter {
    metrics::counter!("worker_lagged", "name" => name)
}

pub fn worker_closed(name: String) -> Counter {
    metrics::counter!("worker_closed", "name" => name)
}

pub fn verified_nip05() -> Counter {
    metrics::counter!("verified_nip05")
}

pub fn individual_follow_messages() -> Counter {
    metrics::counter!("individual_follow_messages")
}

pub fn aggregated_follow_messages() -> Counter {
    metrics::counter!("aggregated_follow_messages")
}

pub fn worker_failures(name: String, id: usize) -> Counter {
    metrics::counter!("worker_failures", "name" => name, "id" => id.to_string())
}

pub fn worker_timeouts(name: String, id: usize) -> Counter {
    metrics::counter!("worker_timeouts", "name" => name, "id" => id.to_string())
}

pub fn followers_per_message() -> Histogram {
    metrics::histogram!("followers_per_message")
}

pub fn retained_follow_changes() -> Gauge {
    metrics::gauge!("retained_follow_changes")
}

pub fn pagerank_seconds() -> Gauge {
    metrics::gauge!("pagerank_seconds")
}

pub fn recommend_cold_miss() -> Counter {
    metrics::counter!("recommend_cold_miss")
}

pub fn recommend_warm_seconds() -> Histogram {
    metrics::histogram!("recommend_warm_seconds")
}

pub fn recommendation_queue_depth() -> Gauge {
    metrics::gauge!("recommendation_queue_depth")
}

pub fn recommend_queue_full() -> Counter {
    metrics::counter!("recommend_queue_full")
}

pub fn recommend_failed() -> Counter {
    metrics::counter!("recommend_failed")
}

pub fn openranking_request_seconds(endpoint: &'static str) -> Histogram {
    metrics::histogram!("openranking_request_seconds", "endpoint" => endpoint)
}

pub fn setup_metrics() -> Result<PrometheusHandle, BuildError> {
    describe_counter!(
        "pubsub_messages",
        "Number of messages published to Google Pub/Sub"
    );
    describe_counter!(
        "contact_lists_processed",
        "Number of contact lists processed"
    );
    describe_counter!(
        "already_seen_contact_lists",
        "Number of contact lists we have already processed"
    );
    describe_counter!(
        "sudden_follow_drops",
        "Number of contact lists that got a sudden drop on followees"
    );
    describe_counter!(
        "nos_sudden_follow_drops",
        "Number of contact lists that got a sudden drop on followees using the Nos agent"
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
    describe_gauge!(
        "retained_follow_changes",
        "Number of retained follow changes"
    );
    describe_gauge!("pagerank_seconds", "Seconds it takes to calculate pagerank");

    describe_counter!(
        "recommend_cold_miss",
        "Number of /recommend responses served from the cold-miss warming path (empty result, short TTL)"
    );
    describe_histogram!(
        "recommend_warm_seconds",
        "Seconds a background recommendation warm-up compute takes"
    );
    describe_gauge!(
        "recommendation_queue_depth",
        "Number of recommendation warm-ups currently queued"
    );
    describe_counter!(
        "recommend_queue_full",
        "Number of /recommend requests shed because the warm-up queue was full"
    );
    describe_counter!(
        "recommend_failed",
        "Number of background recommendation warm-up computes that failed"
    );
    describe_histogram!(
        "openranking_request_seconds",
        "Latency of OpenRanking endpoint handlers, labelled by endpoint"
    );

    let prometheus_builder = PrometheusBuilder::new();
    let prometheus_handle = prometheus_builder.install_recorder()?;
    Ok(prometheus_handle)
}
