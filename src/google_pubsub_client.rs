use crate::domain::NotificationMessage;
use crate::metrics;
use crate::publisher::{PublishEvents, PublisherError};
use gcloud_sdk::{
    google::pubsub::v1::{publisher_client::PublisherClient, PublishRequest, PubsubMessage},
    *,
};
use tracing::debug;

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

pub struct GooglePubSubClient {
    pubsub_client: GoogleApi<PublisherClient<GoogleAuthMiddleware>>,
    google_full_topic: String,
}

impl GooglePubSubClient {
    pub async fn new(google_project_id: &str, google_topic: &str) -> Result<Self, PublisherError> {
        let google_full_topic = format!("projects/{}/topics/{}", google_project_id, google_topic);

        let pubsub_client: GoogleApi<PublisherClient<GoogleAuthMiddleware>> =
            GoogleApi::from_function(
                PublisherClient::new,
                "https://pubsub.googleapis.com",
                Some(google_full_topic.clone()),
            )
            .await
            .map_err(|_| PublisherError::Init)?;

        Ok(Self {
            pubsub_client,
            google_full_topic,
        })
    }
}

impl PublishEvents for GooglePubSubClient {
    async fn publish_events(
        &mut self,
        follow_changes: Vec<NotificationMessage>,
    ) -> Result<(), PublisherError> {
        let pubsub_messages: Result<Vec<PubsubMessage>, PublisherError> = follow_changes
            .iter()
            .filter(|message| {
                // TODO: Temporary filter while developing this service
                ALLOWED_PUBKEYS.contains(&message.followee().to_hex().as_str())
            })
            .map(|message| {
                let data =
                    serde_json::to_vec(message).map_err(|_| PublisherError::SerializationError)?;

                Ok(PubsubMessage {
                    data,
                    ..Default::default()
                })
            })
            .collect();

        let pubsub_messages = pubsub_messages?;

        if pubsub_messages.is_empty() {
            debug!("No messages to publish");
            return Ok(());
        }

        let len = pubsub_messages.len();
        let request = PublishRequest {
            topic: self.google_full_topic.clone(),
            messages: pubsub_messages,
        };

        self.pubsub_client
            .get()
            .publish(request)
            .await
            .map_err(|_| PublisherError::PublishError)?;

        debug!(
            "Published {} messages to Google PubSub {}",
            len, self.google_full_topic
        );

        metrics::pubsub_messages().increment(len as u64);

        Ok(())
    }
}
