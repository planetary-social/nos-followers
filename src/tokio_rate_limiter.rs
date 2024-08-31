use anyhow::Result;
use governor::{
    clock::Clock, middleware::NoOpMiddleware, nanos::Nanos, state::keyed::DefaultKeyedStateStore,
    Quota, RateLimiter,
};
use nostr_sdk::PublicKey;
use std::num::NonZero;
use std::ops::Add;
use tokio::time::Instant;

/// We use tokio as a clock for the rate limiter so we can fake time in the tests
pub type TokioRateLimiter<K, MW = NoOpMiddleware<TokioInstant>> =
    RateLimiter<K, DefaultKeyedStateStore<K>, TokioClock, MW>;

pub fn create_tokio_rate_limiter(max_rate_per_hour: u32) -> Result<TokioRateLimiter<PublicKey>> {
    Ok(RateLimiter::dashmap_with_clock(
        Quota::per_hour(NonZero::new(max_rate_per_hour).ok_or(anyhow::anyhow!(
            "max_follows_per_hour must be greater than zero"
        ))?),
        &TokioClock {},
    ))
}

#[derive(Clone)]
pub struct TokioClock;
impl Clock for TokioClock {
    type Instant = TokioInstant;

    fn now(&self) -> Self::Instant {
        TokioInstant(Instant::now())
    }
}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct TokioInstant(Instant);

impl Add<Nanos> for TokioInstant {
    type Output = Self;

    fn add(self, other: Nanos) -> Self {
        TokioInstant(self.0 + tokio::time::Duration::from_nanos(other.as_u64()))
    }
}

impl governor::clock::Reference for TokioInstant {
    fn duration_since(&self, earlier: Self) -> Nanos {
        Nanos::from(earlier.0.duration_since(self.0).as_nanos() as u64)
    }

    fn saturating_sub(&self, duration: Nanos) -> Self {
        TokioInstant(self.0 - tokio::time::Duration::from_nanos(duration.as_u64()))
    }
}
