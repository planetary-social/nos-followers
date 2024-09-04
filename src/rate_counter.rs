use governor::clock::Clock;
use governor::clock::Reference;
use std::time::Duration;

#[derive(Debug)]
pub struct RateCounter<T>
where
    T: Clock,
{
    items_sent: Vec<T::Instant>,
    limit: u32,
    clock: T,
    max_age: Duration,
}

impl<T: Clock> RateCounter<T> {
    pub fn new(limit: u32, max_age: Duration, clock: T) -> Self {
        Self {
            items_sent: Vec::new(),
            limit,
            clock,
            max_age,
        }
    }

    pub fn bump(&mut self) {
        let now = self.clock.now();
        self.items_sent.push(now);
    }

    pub fn is_hit(&mut self) -> bool {
        // Remove all items older than max_age
        let now = self.clock.now();
        self.items_sent
            .retain(|&instant| now.duration_since(instant) < self.max_age.into());

        self.items_sent.len() as u32 >= self.limit
    }
}
