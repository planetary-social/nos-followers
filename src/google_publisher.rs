use crate::domain::follow_change::FollowChange;
use gcloud_sdk::{
    google::pubsub::v1::{publisher_client::PublisherClient, PublishRequest, PubsubMessage},
    *,
};
use thiserror::Error;
use tokio::select;
use tokio::sync::mpsc::{self, error::SendError};
use tokio::time::{self, Duration};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

struct GooglePublisherClient {
    pubsub_client: GoogleApi<PublisherClient<GoogleAuthMiddleware>>,
    google_full_topic: String,
}

impl GooglePublisherClient {
    async fn publish_events(
        &mut self,
        follow_changes: Vec<FollowChange>,
    ) -> Result<(), GooglePublisherError> {
        let pubsub_messages: Result<Vec<PubsubMessage>, GooglePublisherError> = follow_changes
            .iter()
            .map(|follow_change| {
                let data = serde_json::to_vec(follow_change)
                    .map_err(GooglePublisherError::SerializationError)?;

                Ok(PubsubMessage {
                    data,
                    ..Default::default()
                })
            })
            .collect();

        let pubsub_messages = pubsub_messages?;

        let request = PublishRequest {
            topic: self.google_full_topic.clone(),
            messages: pubsub_messages,
        };

        self.pubsub_client
            .get()
            .publish(request)
            .await
            .map_err(GooglePublisherError::PublishError)?;

        Ok(())
    }
}

pub struct GooglePublisher {
    sender: mpsc::Sender<FollowChange>,
}

impl GooglePublisher {
    pub async fn create(
        cancellation_token: CancellationToken,
    ) -> Result<Self, GooglePublisherError> {
        let google_project_id = "pub-verse-app";
        let google_topic = "follow-changes";
        let google_full_topic = format!("projects/{}/topics/{}", google_project_id, google_topic);

        let pubsub_client: GoogleApi<PublisherClient<GoogleAuthMiddleware>> =
            GoogleApi::from_function(
                PublisherClient::new,
                "https://pubsub.googleapis.com",
                Some(google_full_topic.clone()),
            )
            .await
            .map_err(GooglePublisherError::Init)?;

        let (publication_sender, mut publication_receiver) = mpsc::channel::<FollowChange>(1);

        tokio::spawn(async move {
            let mut buffer = Vec::new();
            let size_threshold = 500;
            let seconds = 5;

            let mut client = GooglePublisherClient {
                pubsub_client,
                google_full_topic,
            };

            let mut interval = time::interval(Duration::from_secs(seconds));

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
                                    GooglePublisherError::PublishError(_) => {
                                        // TODO: We don't break in this error for the moment while investigating the cause:
                                        // server-1  | Caused by:
                                        // server-1  |     0: status: Unknown, message: "transport error", details: [], metadata: MetadataMap { headers: {} }
                                        // server-1  |     1: transport error
                                        // server-1  |     2: connection error
                                        // server-1  |     3: peer closed connection without sending TLS close_notify: https://docs.rs/rustls/latest/rustls/manual/_03_howto/index.html#unexpected-eof
                                        // server-1  |
                                        // server-1  | Stack backtrace:
                                        // server-1  |    0: std::backtrace::Backtrace::create
                                        // server-1  |    1: nos_followers::google_publisher::GooglePublisherClient::publish_events::{{closure}}
                                        // server-1  |    2: nos_followers::google_publisher::GooglePublisher::create::{{closure}}::{{closure}}k

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
                        error!("Failed to publish events: {:?}", e);
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

#[derive(Error, Debug)]
pub enum GooglePublisherError {
    #[error("Failed to publish events: {0}")]
    PublishError(#[from] tonic::Status),

    #[error("Failed to serialize event to JSON: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Failed to initialize Google publisher: {0}")]
    Init(#[from] gcloud_sdk::error::Error),
}
