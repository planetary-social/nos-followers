use rand::{rngs::StdRng, Rng, SeedableRng};
use std::cmp::max;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::Sender;
use tracing::{info, warn};

pub trait SendWithChecks<T> {
    async fn send_with_checks(&self, item: T) -> Result<(), SendError<T>>;
}

impl<T> SendWithChecks<T> for Sender<T> {
    async fn send_with_checks(&self, item: T) -> Result<(), SendError<T>> {
        let mut rng = StdRng::from_entropy();
        if rng.gen_bool(0.025) {
            let max_capacity = self.max_capacity();
            let capacity = self.capacity();
            let threshold = max(1, max_capacity / 10);

            if capacity < threshold {
                warn!(
                    "Channel buffer is at 90%! ({}/{})",
                    max_capacity - capacity,
                    max_capacity
                );
            } else {
                info!(
                    "Channel buffer is at a healthy level. ({}/{})",
                    max_capacity - capacity,
                    max_capacity
                );
            }
        }

        self.send(item).await
    }
}
