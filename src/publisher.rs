use crate::domain::FollowChange;
use crate::domain::NotificationFactory;
use crate::google_pubsub_client::{PublishEvents, PublisherError};
use std::num::NonZeroUsize;
use tokio::select;
use tokio::sync::mpsc::{self, error::SendError};
use tokio::time::{self, Duration};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

/// Publisher for follow changes. It batches follow changes and publishes
/// them to Google PubSub after certain time is elapsed or a size threshold is
/// hit.
pub struct Publisher {
    sender: mpsc::Sender<FollowChange>,
}

impl Publisher {
    pub async fn create(
        cancellation_token: CancellationToken,
        mut client: impl PublishEvents + Send + Sync + 'static,
        flush_period_seconds: NonZeroUsize,
        min_seconds_between_messages: NonZeroUsize,
    ) -> Result<Self, PublisherError> {
        let (publication_sender, mut publication_receiver) = mpsc::channel::<FollowChange>(1);

        let mut buffer = NotificationFactory::new(min_seconds_between_messages);
        tokio::spawn(async move {
            info!("Publishing messages every {} seconds", flush_period_seconds);

            let mut interval =
                time::interval(Duration::from_secs(flush_period_seconds.get() as u64));

            loop {
                select! {
                    _ = cancellation_token.cancelled() => {
                        debug!("Cancellation token is cancelled, stopping Google publisher");
                        break;
                    }

                    // The first condition to send the current buffer is the
                    // time interval. We wait a max of `seconds_threshold`
                    // seconds, after that the buffer is cleared and sent
                    _ = interval.tick() => {
                        if !buffer.is_empty() {
                            debug!("Time based threshold of {} seconds reached, publishing buffer", flush_period_seconds);

                            if let Err(e) = client.publish_events(buffer.drain_into_messages()).await {
                                match &e {
                                    // We've seen this happen sporadically, we don't neet to kill the look in this situation
                                    PublisherError::PublishError(_) => {
                                        error!("{}", e);
                                    }
                                    _ => {
                                        error!("Failed to publish events: {}", e);
                                        cancellation_token.cancel();
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    recv_result = publication_receiver.recv() => {
                        match recv_result {
                            Some(follow_change) => {
                                buffer.insert(follow_change);
                            }
                            None => {
                                debug!("Publication receiver closed, stopping Google publisher");
                                break;
                            }
                        }
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
    use crate::domain::{FollowChange, NotificationMessage};
    use crate::google_pubsub_client::PublisherError;
    use assertables::*;
    use chrono::{DateTime, Duration, Utc};
    use futures::Future;
    use gcloud_sdk::tonic::Status;
    use nonzero_ext::nonzero;
    use nostr_sdk::prelude::Keys;
    use pretty_assertions::assert_eq;
    use std::sync::Arc;
    use std::time::UNIX_EPOCH;
    use tokio::sync::Mutex;
    use tokio_util::sync::CancellationToken;

    struct MockPublishEvents {
        published_events: Arc<Mutex<Vec<NotificationMessage>>>,
        fail_publish: bool,
    }

    impl PublishEvents for MockPublishEvents {
        fn publish_events(
            &mut self,
            messages: Vec<NotificationMessage>,
        ) -> impl Future<Output = Result<(), PublisherError>> + std::marker::Send {
            let published_events = self.published_events.clone();
            let fail_publish = self.fail_publish;

            async move {
                if fail_publish {
                    Err(PublisherError::PublishError(Status::cancelled(
                        "Mock error",
                    )))
                } else {
                    let mut published_events = published_events.lock().await;
                    published_events.extend(messages);
                    Ok(())
                }
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_publisher_collapses_messages() {
        let published_events = Arc::new(Mutex::new(Vec::new()));
        let mock_client = MockPublishEvents {
            published_events: published_events.clone(),
            fail_publish: false,
        };

        let cancellation_token = CancellationToken::new();
        let flush_period_seconds = nonzero!(1usize);
        let min_seconds_between_messages = nonzero!(60usize);

        let publisher = Publisher::create(
            cancellation_token.clone(),
            mock_client,
            flush_period_seconds,
            min_seconds_between_messages,
        )
        .await
        .unwrap();

        let follower_pubkey = Keys::generate().public_key();
        let followee1_pubkey = Keys::generate().public_key();
        let followee2_pubkey = Keys::generate().public_key();

        publisher
            .queue_publication(FollowChange::new_followed(
                seconds_to_datetime(1),
                follower_pubkey,
                followee1_pubkey,
            ))
            .await
            .unwrap();

        publisher
            .queue_publication(FollowChange::new_unfollowed(
                seconds_to_datetime(1),
                follower_pubkey,
                followee2_pubkey,
            ))
            .await
            .unwrap();

        publisher
            .queue_publication(FollowChange::new_followed(
                seconds_to_datetime(2),
                follower_pubkey,
                followee2_pubkey,
            ))
            .await
            .unwrap();

        publisher
            .queue_publication(FollowChange::new_unfollowed(
                seconds_to_datetime(3),
                follower_pubkey,
                followee2_pubkey,
            ))
            .await
            .unwrap();

        tokio::time::sleep(Duration::seconds(2).to_std().unwrap()).await;

        let events = published_events.lock().await;
        assert_bag_eq!(
            events.clone(),
            [
                NotificationMessage::from(FollowChange::new_followed(
                    seconds_to_datetime(1),
                    follower_pubkey,
                    followee1_pubkey
                )),
                // The second follow change for the same followee should have collapsed
                // to a single change. We only keep the last one
                NotificationMessage::from(FollowChange::new_unfollowed(
                    seconds_to_datetime(2),
                    follower_pubkey,
                    followee2_pubkey
                ))
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_publisher() {
        let published_events = Arc::new(Mutex::new(Vec::new()));
        let mock_client = MockPublishEvents {
            published_events: published_events.clone(),
            fail_publish: false,
        };

        let cancellation_token = CancellationToken::new();
        let flush_period_seconds = nonzero!(1usize);
        let min_seconds_between_messages = nonzero!(60usize);

        let publisher = Publisher::create(
            cancellation_token.clone(),
            mock_client,
            flush_period_seconds,
            min_seconds_between_messages,
        )
        .await
        .unwrap();

        let follower_pubkey = Keys::generate().public_key();
        let followee1_pubkey = Keys::generate().public_key();
        let followee2_pubkey = Keys::generate().public_key();

        publisher
            .queue_publication(FollowChange::new_followed(
                seconds_to_datetime(2),
                follower_pubkey,
                followee1_pubkey,
            ))
            .await
            .unwrap();

        publisher
            .queue_publication(FollowChange::new_unfollowed(
                seconds_to_datetime(1),
                follower_pubkey,
                followee2_pubkey,
            ))
            .await
            .unwrap();

        tokio::time::sleep(Duration::seconds(2).to_std().unwrap()).await;

        let events = published_events.lock().await;
        assert_bag_eq!(
            events.clone(),
            [
                NotificationMessage::from(FollowChange::new_followed(
                    seconds_to_datetime(2),
                    follower_pubkey,
                    followee1_pubkey,
                )),
                NotificationMessage::from(FollowChange::new_unfollowed(
                    seconds_to_datetime(1),
                    follower_pubkey,
                    followee2_pubkey,
                ))
            ],
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_publisher_error() {
        let published_events = Arc::new(Mutex::new(Vec::new()));
        let mock_client = MockPublishEvents {
            published_events: published_events.clone(),
            fail_publish: true, // Simulate a failure in publishing
        };

        let cancellation_token = CancellationToken::new();
        let flush_period_seconds = nonzero!(1usize);
        let min_seconds_between_messages = nonzero!(60usize);

        let publisher = Publisher::create(
            cancellation_token.clone(),
            mock_client,
            flush_period_seconds,
            min_seconds_between_messages,
        )
        .await
        .unwrap();

        let follower_pubkey = Keys::generate().public_key();
        let followee_pubkey = Keys::generate().public_key();

        // Queue up follow changes
        publisher
            .queue_publication(FollowChange::new_followed(
                seconds_to_datetime(1),
                follower_pubkey,
                followee_pubkey,
            ))
            .await
            .unwrap();

        tokio::time::sleep(Duration::seconds(2).to_std().unwrap()).await;

        let events = published_events.lock().await;
        assert_eq!(events.len(), 0);
    }

    fn seconds_to_datetime(seconds: i64) -> DateTime<Utc> {
        DateTime::<Utc>::from(UNIX_EPOCH + Duration::seconds(seconds).to_std().unwrap())
    }
}
