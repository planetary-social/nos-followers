use crate::FollowChange;
use futures::Future;
use gcloud_sdk::{
    google::pubsub::v1::{publisher_client::PublisherClient, PublishRequest, PubsubMessage},
    *,
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum GooglePublisherError {
    #[error("Failed to publish events: {0}")]
    PublishError(#[from] tonic::Status),

    #[error("Failed to serialize event to JSON: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Failed to initialize Google publisher: {0}")]
    Init(#[from] gcloud_sdk::error::Error),
}

pub trait PublishEvents {
    fn publish_events(
        &mut self,
        follow_changes: Vec<FollowChange>,
    ) -> impl Future<Output = Result<(), GooglePublisherError>> + std::marker::Send;
}

pub struct GooglePubSubClient {
    pubsub_client: GoogleApi<PublisherClient<GoogleAuthMiddleware>>,
    google_full_topic: String,
}

impl GooglePubSubClient {
    pub async fn new(
        google_project_id: &str,
        google_topic: &str,
    ) -> Result<Self, GooglePublisherError> {
        let google_full_topic = format!("projects/{}/topics/{}", google_project_id, google_topic);

        let pubsub_client: GoogleApi<PublisherClient<GoogleAuthMiddleware>> =
            GoogleApi::from_function(
                PublisherClient::new,
                "https://pubsub.googleapis.com",
                Some(google_full_topic.clone()),
            )
            .await
            .map_err(GooglePublisherError::Init)?;

        Ok(Self {
            pubsub_client,
            google_full_topic,
        })
    }
}

impl PublishEvents for GooglePubSubClient {
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
