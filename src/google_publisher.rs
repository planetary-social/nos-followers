use crate::domain::follow_change::FollowChange;
use anyhow::{Context, Ok, Result};
use gcloud_sdk::{
    google::pubsub::v1::{publisher_client::PublisherClient, PublishRequest, PubsubMessage},
    *,
};
use tokio::select;
use tokio::sync::mpsc;
use tokio::time::{self, Duration};
use tracing::{error, info};

struct GooglePublisherClient {
    pubsub_client: GoogleApi<PublisherClient<GoogleAuthMiddleware>>,
    google_full_topic: String,
}

impl GooglePublisherClient {
    async fn publish_events(&mut self, follow_changes: Vec<FollowChange>) -> Result<()> {
        let pubsub_messages: Result<Vec<PubsubMessage>> = follow_changes
            .iter()
            .map(|follow_change| {
                let data = serde_json::to_vec(follow_change)
                    .context("Failed to serialize event to JSON")?;

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
            .context("Failed to publish follow change")?;

        Ok(())
    }
}

pub struct GooglePublisher {
    sender: mpsc::Sender<FollowChange>,
}

impl GooglePublisher {
    pub async fn create() -> Result<Self> {
        let google_project_id = "pub-verse-app";
        let google_topic = "follow-changes";
        let google_full_topic = format!("projects/{}/topics/{}", google_project_id, google_topic);

        let pubsub_client: GoogleApi<PublisherClient<GoogleAuthMiddleware>> =
            GoogleApi::from_function(
                PublisherClient::new,
                "https://pubsub.googleapis.com",
                Some(google_full_topic.clone()),
            )
            .await?;

        let (publication_sender, mut publication_receiver) =
            tokio::sync::mpsc::channel::<FollowChange>(1);

        tokio::spawn(async move {
            let mut buffer = Vec::new();
            let size_threshold = 1000;
            let seconds = 5;

            let mut client = GooglePublisherClient {
                pubsub_client,
                google_full_topic,
            };

            let mut interval = time::interval(Duration::from_secs(5));

            loop {
                select! {
                    _ = interval.tick() => {
                        if !buffer.is_empty() {
                            info!("Publishing {} follow changes after {} seconds of inactivity", buffer.len(), seconds);
                            if let Err(e) = client.publish_events(buffer.split_off(0)).await {
                                error!("Failed to publish events: {:?}", e);
                                break;
                            }
                        }
                    }

                    Some(follow_change) = publication_receiver.recv() => {
                        buffer.push(follow_change);
                    }

                    else => {
                        break;
                    }
                }

                if buffer.len() >= size_threshold {
                    info!(
                        "Publishing {} follow changes after reaching threshold",
                        buffer.len()
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

    pub async fn queue_publication(&mut self, follow_change: FollowChange) -> Result<()> {
        self.sender
            .send(follow_change)
            .await
            .context("Failed to queue follow change")
    }
}
