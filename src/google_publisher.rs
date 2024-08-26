use crate::domain::follow_change::FollowChange;
use crate::google_pubsub_client::{GooglePublisherError, PublishEvents};
use tokio::select;
use tokio::sync::mpsc::{self, error::SendError};
use tokio::time::{self, Duration};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

/// Google publisher for follow changes. It batches follow changes and publishes
/// them to Google PubSub after certain time is elapsed or a size threshold is
/// hit.
pub struct GooglePublisher {
    sender: mpsc::Sender<FollowChange>,
}

impl GooglePublisher {
    pub async fn create(
        cancellation_token: CancellationToken,
        mut client: impl PublishEvents + Send + Sync + 'static,
        seconds_threshold: u64,
        size_threshold: usize,
    ) -> Result<Self, GooglePublisherError> {
        let (publication_sender, mut publication_receiver) = mpsc::channel::<FollowChange>(1);

        tokio::spawn(async move {
            let mut buffer = Vec::new();

            let mut interval = time::interval(Duration::from_secs(seconds_threshold));

            loop {
                select! {
                    _ = cancellation_token.cancelled() => {
                        debug!("Cancellation token is cancelled, stopping Google publisher");
                        break;
                    }

                    _ = interval.tick() => {
                        if !buffer.is_empty() {
                            info!("Publishing batch of {} follow changes", buffer.len());
                            if let Err(e) = client.publish_events(buffer.split_off(0)).await {
                                match &e {
                                    // We've seen this happen sporadically, we don't neet to kill the look in this situation
                                    GooglePublisherError::PublishError(_) => {
                                        error!("{}", e);
                                    }
                                    _ => {
                                        error!("Failed to publish events: {}", e);
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    recv_result = publication_receiver.recv() => {
                        match recv_result {
                            Some(follow_change) => {
                                buffer.push(follow_change);
                            }
                            None => {
                                debug!("Publication receiver closed, stopping Google publisher");
                                break;
                            }
                        }
                    }
                }

                if buffer.len() >= size_threshold {
                    info!(
                        "Publishing batch of {} follow changes after reaching threshold of {} items",
                        buffer.len(), size_threshold
                    );
                    if let Err(e) = client.publish_events(buffer.split_off(0)).await {
                        error!("Failed to publish events: {}", e);
                        break;
                    }
                }
            }
        });

        Ok(Self {
            sender: publication_sender,
        })
    }

    pub async fn queue_publication(
        &self,
        follow_change: FollowChange,
    ) -> Result<(), SendError<FollowChange>> {
        self.sender.send(follow_change).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::follow_change::FollowChange;
    use crate::google_pubsub_client::GooglePublisherError;
    use futures::Future;
    use gcloud_sdk::tonic::Status;
    use nostr_sdk::prelude::Keys;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tokio_util::sync::CancellationToken;

    struct MockPublishEvents {
        published_events: Arc<Mutex<Vec<FollowChange>>>,
        fail_publish: bool,
    }

    impl PublishEvents for MockPublishEvents {
        fn publish_events(
            &mut self,
            follow_changes: Vec<FollowChange>,
        ) -> impl Future<Output = Result<(), GooglePublisherError>> + std::marker::Send {
            let published_events = self.published_events.clone();
            let fail_publish = self.fail_publish;

            async move {
                if fail_publish {
                    Err(GooglePublisherError::PublishError(Status::cancelled(
                        "Mock error",
                    )))
                } else {
                    let mut published_events = published_events.lock().await;
                    published_events.extend(follow_changes);
                    Ok(())
                }
            }
        }
    }

    #[tokio::test]
    async fn test_google_publisher() {
        let published_events = Arc::new(Mutex::new(Vec::new()));
        let mock_client = MockPublishEvents {
            published_events: published_events.clone(),
            fail_publish: false,
        };

        let cancellation_token = CancellationToken::new();
        let seconds_threshold = 1;
        let size_threshold = 5;

        let publisher = GooglePublisher::create(
            cancellation_token.clone(),
            mock_client,
            seconds_threshold,
            size_threshold,
        )
        .await
        .unwrap();

        let follower_pubkey = Keys::generate().public_key();
        let followee_pubkey = Keys::generate().public_key();

        // Queue up follow changes
        publisher
            .queue_publication(FollowChange::new_followed(
                1.into(),
                follower_pubkey,
                followee_pubkey,
            ))
            .await
            .unwrap();

        publisher
            .queue_publication(FollowChange::new_unfollowed(
                2.into(),
                follower_pubkey,
                followee_pubkey,
            ))
            .await
            .unwrap();

        // Wait enough time for the interval to trigger
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Check that events were published
        let events = published_events.lock().await;
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].at, 1.into());
        assert_eq!(events[1].at, 2.into());
    }

    #[tokio::test]
    async fn test_google_publisher_error() {
        let published_events = Arc::new(Mutex::new(Vec::new()));
        let mock_client = MockPublishEvents {
            published_events: published_events.clone(),
            fail_publish: true, // Simulate a failure in publishing
        };

        let cancellation_token = CancellationToken::new();
        let seconds_threshold = 1;
        let size_threshold = 5;

        let publisher = GooglePublisher::create(
            cancellation_token.clone(),
            mock_client,
            seconds_threshold,
            size_threshold,
        )
        .await
        .unwrap();

        let follower_pubkey = Keys::generate().public_key();
        let followee_pubkey = Keys::generate().public_key();

        // Queue up follow changes
        publisher
            .queue_publication(FollowChange::new_followed(
                1.into(),
                follower_pubkey,
                followee_pubkey,
            ))
            .await
            .unwrap();

        // Wait enough time for the interval to trigger
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Check that no events were published due to the error
        let events = published_events.lock().await;
        assert_eq!(events.len(), 0);
    }
}
