use serde::Deserialize;
use std::time::{Duration, Instant};

/// Configuration for reconnection backoff strategy.
#[derive(Debug, Clone, Deserialize)]
pub struct ReconnectCfg {
    /// Number of fast retry attempts before exponential backoff starts.
    pub fast_attempts: u32,
    /// Delay (ms) between fast retry attempts.
    pub fast_delay_ms: u64,

    /// Base delay (ms) for exponential backoff calculation.
    pub base_delay_ms: u64,
    /// Maximum delay (ms) cap for exponential backoff.
    pub max_delay_ms: u64,
    /// Exponential growth factor for delays.
    pub factor: f64,

    /// Time (ms) after a successful connection to reset attempts counter.
    pub reset_after_ms: u64,

    /// Maximum number of retries (None = unlimited).
    pub max_retries: Option<u32>,
}

impl Default for ReconnectCfg {
    fn default() -> Self {
        Self {
            fast_attempts: 3,
            fast_delay_ms: 100,
            base_delay_ms: 500,
            max_delay_ms: 30_000,
            factor: 2.0,
            reset_after_ms: 60_000,
            max_retries: None,
        }
    }
}

/// Backoff state machine for reconnection attempts.
pub struct Backoff {
    cfg: ReconnectCfg,
    attempt: u32,
    last_ok: Instant,
}

impl Backoff {
    /// Creates a new `Backoff` with the given configuration.
    pub fn new(cfg: ReconnectCfg) -> Self {
        Self {
            last_ok: Instant::now(),
            attempt: 0,
            cfg,
        }
    }

    /// Call this after a successful connection to reset counters.
    #[inline]
    pub fn on_success(&mut self) {
        self.last_ok = Instant::now();
        self.attempt = 0;
    }

    /// Returns the next delay duration before retrying.
    #[inline]
    pub fn next_delay(&mut self) -> Duration {
        // Fast retry attempts
        if self.attempt < self.cfg.fast_attempts {
            self.attempt += 1;
            return Duration::from_millis(self.cfg.fast_delay_ms);
        }

        // Exponential backoff with cap
        let exp = (self.cfg.base_delay_ms as f64)
            * self
                .cfg
                .factor
                .powi((self.attempt - self.cfg.fast_attempts + 1) as i32);
        let capped = exp.min(self.cfg.max_delay_ms as f64) as u64;
        self.attempt = self.attempt.saturating_add(1);

        Duration::from_millis(capped)
    }

    /// Checks if attempts should be reset due to elapsed time.
    #[inline]
    pub fn should_reset(&self) -> bool {
        self.last_ok.elapsed() >= Duration::from_millis(self.cfg.reset_after_ms)
    }

    /// Returns how many attempts are left, or None if unlimited.
    #[inline]
    pub fn attempts_left(&self) -> Option<u32> {
        self.cfg.max_retries.map(|m| m.saturating_sub(self.attempt))
    }
}
