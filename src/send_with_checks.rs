use rand::Rng;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::Sender;
use tracing::{info, warn};

pub trait SendWithChecks<T> {
    fn send_with_checks(&self, item: T) -> Result<(), TrySendError<T>>;
}

impl<T> SendWithChecks<T> for Sender<T> {
    fn send_with_checks(&self, item: T) -> Result<(), TrySendError<T>> {
        let max_capacity = self.max_capacity();
        let capacity = self.capacity();

        let mut rng = rand::thread_rng();
        if rng.gen_bool(0.05) {
            if capacity < max_capacity / 10 {
                warn!(
                    "Channel buffer is at 90%! used {} of {} slots",
                    max_capacity - capacity,
                    max_capacity
                );
            } else {
                info!(
                    "Channel buffer is at a healthy level, used {} of {} slots",
                    max_capacity - capacity,
                    max_capacity
                );
            }
        }

        self.try_send(item)
    }
}
