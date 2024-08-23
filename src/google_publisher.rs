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

const ALLOWED_PUBKEYS: &[&str] = &[
    "07ecf9838136fe430fac43fa0860dbc62a0aac0729c5a33df1192ce75e330c9f", // Bryan
    "89ef92b9ebe6dc1e4ea398f6477f227e95429627b0a33dc89b640e137b256be5", // Daniel
    "e8ad7c13ba55ba0a04c23fc09edce74ad7a8dddc059dc2e274ff63bc2e047782", // Daphne
    "31d53c6dc32d0935a04b88d592156d350d355e2d817361ac082f775c2fe4df02", // Gergely
    "27cf2c68535ae1fc06510e827670053f5dcd39e6bd7e05f1ffb487ef2ac13549", // Josh
    "81f14ddb4704df919866a3ba0178c6b44a6a18ca8ebc7f1720c315e7ac10aad9", // Lexie
    "969e6a28ee5214cb0296ee69cbdce4f43229124a78b1043d85df31e5636d0f1f", // Linda
    "b29bb98ebecca7ae3a86a02ab6cf260baecf098dcd452ef8e5f9c549dfc0e0ef", // Martin
    "d0a1ffb8761b974cec4a3be8cbcb2e96a7090dcf465ffeac839aa4ca20c9a59e", // Matt
    "76c71aae3a491f1d9eec47cba17e229cda4113a0bbb6e6ae1776d7643e29cafa", // Rabble
    "e77b246867ba5172e22c08b6add1c7de1049de997ad2fe6ea0a352131f9a0e9a", // Sebastian
    "806d236c19d4771153406e150b1baf6257725cda781bf57442aeef53ed6cb727", // Shaina
];

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

/// Google publisher for follow changes. It batches follow changes and publishes
/// them to Google PubSub after certain time is elapsed or a size threshold is
/// hit.
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
        // TODO: Temporary filter while developing this service
        if !ALLOWED_PUBKEYS.contains(&follow_change.followee.to_hex().as_str())
            && !ALLOWED_PUBKEYS.contains(&follow_change.follower.to_hex().as_str())
        {
            return Ok(());
        }

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
