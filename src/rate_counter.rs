use governor::clock::Clock;
use governor::clock::Reference;
use std::time::Duration;

#[derive(Debug)]
pub struct RateCounter<T>
where
    T: Clock,
{
    items_sent: Vec<T::Instant>,
    limit: usize,
    clock: T,
    max_age: Duration,
}

impl<T: Clock> RateCounter<T> {
    pub fn new(limit: usize, max_age: Duration, clock: T) -> Self {
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

    pub fn is_full(&mut self) -> bool {
        // Remove all items that reach max_age
        let now = self.clock.now();
        self.items_sent
            .retain(|&instant| now.duration_since(instant) <= self.max_age.into());

        self.items_sent.len() >= self.limit
    }

    pub fn limit(&self) -> usize {
        self.limit
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use governor::clock::FakeRelativeClock;

    struct TestCase {
        name: &'static str,
        bumps: usize,
        advance: Duration,
        expected_is_full: bool,
    }

    #[test]
    fn test_rate_counter_table() {
        let test_cases = vec![
            TestCase {
                name: "Not full at zero time with one bump",
                bumps: 1,
                advance: Duration::from_secs(0),
                expected_is_full: false,
            },
            TestCase {
                name: "Full at limit",
                bumps: 2,
                advance: Duration::from_secs(0),
                expected_is_full: true,
            },
            TestCase {
                name: "Full at limit before max age",
                bumps: 2,
                advance: Duration::from_secs(9),
                expected_is_full: true,
            },
            TestCase {
                name: "Full with one more bump than limit",
                bumps: 3,
                advance: Duration::from_secs(0),
                expected_is_full: true,
            },
            TestCase {
                name: "Full at max age and limit",
                bumps: 2,
                advance: Duration::from_secs(10),
                expected_is_full: true,
            },
            TestCase {
                name: "Not full below limit at max age",
                bumps: 1,
                advance: Duration::from_secs(10),
                expected_is_full: false,
            },
            TestCase {
                name: "Full above limit at max age",
                bumps: 3,
                advance: Duration::from_secs(10),
                expected_is_full: true,
            },
            TestCase {
                name: "Not full above limit after max age",
                bumps: 3,
                advance: Duration::from_secs(11),
                expected_is_full: false,
            },
        ];

        for case in test_cases {
            let clock = FakeRelativeClock::default();
            let mut rate_counter = RateCounter::new(2, Duration::from_secs(10), clock.clone());

            for _ in 0..case.bumps {
                rate_counter.bump();
            }

            clock.advance(case.advance);

            let is_full = rate_counter.is_full();

            assert_eq!(
                is_full, case.expected_is_full,
                "Test case '{}' failed: expected is_full={} but got {}",
                case.name, case.expected_is_full, is_full
            );
        }
    }
}
