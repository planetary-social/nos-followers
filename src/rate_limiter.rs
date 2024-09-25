use tokio::time::{Duration, Instant};
use tracing::debug;

/// Token bucket rate limiter.
pub struct RateLimiter {
    capacity: f64,            // Initial capacity, credit in the bucket
    tokens: f64,              // Current number of tokens (can be negative)
    refill_rate_per_sec: f64, // Tokens added per second
    last_refill: Instant,     // Last time tokens were refilled
    max_negative_tokens: f64, // Maximum allowed negative tokens (deficit)
}

impl RateLimiter {
    /// Creates a new RateLimiter.
    pub fn new(initial_capacity: f64, refill_duration: Duration) -> Self {
        let refill_rate_per_sec = refill_duration.as_secs_f64();
        let tokens = initial_capacity;
        let max_negative_tokens = initial_capacity * 1000.0;

        Self {
            capacity: initial_capacity,
            tokens,
            refill_rate_per_sec,
            last_refill: Instant::now(),
            max_negative_tokens,
        }
    }

    /// Refills tokens based on the elapsed time since the last refill.
    fn refill_tokens(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;

        let tokens_to_add = elapsed / self.refill_rate_per_sec;
        debug!("Tokens to add: {}", tokens_to_add);
        self.tokens = (self.tokens + tokens_to_add).min(self.capacity);
    }

    /// Checks if the specified number of tokens are available without consuming them.
    pub fn can_consume(&mut self, tokens_needed: f64) -> bool {
        self.refill_tokens();
        self.tokens >= tokens_needed
    }

    /// Attempts to consume the specified number of tokens.
    pub fn consume(&mut self, tokens_needed: f64) -> bool {
        self.refill_tokens();
        if self.tokens >= tokens_needed {
            self.tokens -= tokens_needed;
            true
        } else {
            false
        }
    }

    /// Consumes tokens regardless of availability (can result in negative token count).
    pub fn overcharge(&mut self, tokens_needed: f64) {
        self.refill_tokens();
        self.tokens -= tokens_needed;

        if self.tokens < -self.max_negative_tokens {
            self.tokens = -self.max_negative_tokens;
        }
    }

    pub fn get_available_tokens(&mut self) -> f64 {
        self.refill_tokens();
        self.tokens
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Duration;
    use tokio::time::{self};

    #[tokio::test]
    async fn test_initial_tokens() {
        let capacity = 10.0;
        let refill_duration = Duration::from_secs(86400); // 1 day
        let rate_limiter = RateLimiter::new(capacity, refill_duration);

        assert_eq!(rate_limiter.tokens, capacity);
    }

    #[tokio::test]
    async fn test_consume_tokens_success() {
        let capacity = 10.0;
        let refill_duration = Duration::from_secs(86400); // 1 day
        let mut rate_limiter = RateLimiter::new(capacity, refill_duration);

        // Consume 5 tokens
        let result = rate_limiter.consume(5.0);
        assert!(result);
        assert_eq!(rate_limiter.tokens, 5.0);
    }

    #[tokio::test]
    async fn test_consume_tokens_failure() {
        let capacity = 10.0;
        let refill_duration = Duration::from_secs(86400); // 1 day
        let mut rate_limiter = RateLimiter::new(capacity, refill_duration);

        // Attempt to consume more tokens than available
        let result = rate_limiter.consume(15.0);
        assert!(!result);
        // Tokens should remain unchanged since consume failed
        assert_eq!(rate_limiter.tokens, capacity);
    }

    #[tokio::test]
    async fn test_overcharge() {
        let capacity = 10.0;
        let refill_duration = Duration::from_secs(86400); // 1 day
        let max_negative_tokens = capacity * 1000.0;
        let mut rate_limiter = RateLimiter::new(capacity, refill_duration);

        // Overcharge by 15 tokens
        rate_limiter.overcharge(15.0);
        assert_eq!(rate_limiter.tokens, -5.0);

        // Overcharge beyond max_negative_tokens
        rate_limiter.overcharge(max_negative_tokens * 2.0);
        assert_eq!(rate_limiter.tokens, -max_negative_tokens);
    }

    #[tokio::test(start_paused = true)]
    async fn test_refill_tokens() {
        let capacity = 10.0;
        let refill_duration = Duration::from_secs(10);
        let mut rate_limiter = RateLimiter::new(capacity, refill_duration);

        // Consume all tokens
        let result = rate_limiter.consume(capacity);
        assert!(result);
        assert_eq!(rate_limiter.tokens, 0.0);

        // Advance time by half of the refill duration
        time::advance(Duration::from_secs(5)).await;
        rate_limiter.refill_tokens();
        assert_eq!(rate_limiter.tokens, 0.5);

        // Advance time to complete the refill duration
        time::advance(Duration::from_secs(5)).await;
        rate_limiter.refill_tokens();
        assert_eq!(rate_limiter.tokens, 1.0);
    }

    #[tokio::test(start_paused = true)]
    async fn test_overcharge_and_refill() {
        let capacity = 10.0;
        let refill_duration = Duration::from_secs(10);
        let mut rate_limiter = RateLimiter::new(capacity, refill_duration);

        // Overcharge by 15 tokens
        rate_limiter.overcharge(15.0);
        assert_eq!(rate_limiter.tokens, -5.0);

        // Advance time to refill tokens
        time::advance(Duration::from_secs(6 * 10)).await; // Wait enough to refill capacity
        rate_limiter.refill_tokens();
        assert_eq!(rate_limiter.tokens, 1.0);
    }

    #[tokio::test]
    async fn test_max_negative_tokens() {
        let capacity = 10.0;
        let refill_duration = Duration::from_secs(86400); // 1 day
        let max_negative_tokens = capacity * 1000.0;
        let mut rate_limiter = RateLimiter::new(capacity, refill_duration);

        // Overcharge repeatedly to exceed max_negative_tokens
        rate_limiter.overcharge(max_negative_tokens + 50.0);
        assert_eq!(rate_limiter.tokens, -max_negative_tokens);
    }
}
