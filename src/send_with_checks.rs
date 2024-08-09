use anyhow::{bail, Result};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::Sender;
use tracing::{debug, warn};

pub trait SendWithChecks<T> {
    fn send_with_checks(&self, item: T) -> Result<()>;
}

impl<T> SendWithChecks<T> for Sender<T> {
    fn send_with_checks(&self, item: T) -> Result<()> {
        match self.try_send(item) {
            Ok(_) => {
                let current_seconds = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

                // Every 10 seconds we check if the channel buffer is almost full
                if current_seconds % 5 == 0 {
                    let max_capacity = self.max_capacity();
                    let capacity = self.capacity();
                    let used_capacity = max_capacity - capacity;

                    // Warn if we are already using 80% of the buffer
                    if capacity < max_capacity / 5 {
                        warn!(
                            "Channel buffer is almost full! used {} from {}",
                            used_capacity, max_capacity
                        );
                    } else {
                        debug!(
                            "Channel buffer is healthy, used {} from {}",
                            used_capacity, max_capacity
                        );
                    }
                }

                Ok(())
            }
            Err(e) => match e {
                TrySendError::Closed(_) => {
                    bail!("Failed to send follow change: channel closed");
                }
                TrySendError::Full(_) => {
                    bail!(
                        "Failed to send follow change: channel full, max capacity: {}",
                        self.max_capacity()
                    );
                }
            },
        }
    }
}
