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

#[cfg(test)]

mod tests {
    use super::*;
    use governor::clock::FakeRelativeClock;

    #[test]
    fn test_rate_counter() {
        let clock = FakeRelativeClock::default();
        // Configured for a rate of 2 items in 10 seconds before hitting the limit
        let mut rate_counter = RateCounter::new(2, Duration::from_secs(10), clock.clone());

        rate_counter.bump();
        rate_counter.bump();
        assert!(
            rate_counter.is_hit(),
            "2 items in 0 seconds should hit the limit"
        );

        clock.advance(Duration::from_secs(5));
        assert!(
            rate_counter.is_hit(),
            "2 items in 5 seconds should hit the limit"
        );

        clock.advance(Duration::from_secs(5));
        rate_counter.bump();
        rate_counter.bump();
        assert!(
            rate_counter.is_hit(),
            "4 items in 10 seconds should hit the limit"
        );

        clock.advance(Duration::from_secs(5));
        assert!(
            rate_counter.is_hit(),
            "4 items in 15 seconds should hit the limit"
        );

        clock.advance(Duration::from_secs(5));
        assert!(
            !rate_counter.is_hit(),
            "4 items in 20 seconds should not hit the limit"
        );
    }
}
