use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::connector::features::{
    http::client::ReqwestClientSpec, shared::clients_map::ClientConfig,
};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HttpConnectorConfig {
    pub default_max_cores: Option<usize>,
    pub specific_core_ids: Vec<usize>,
    pub use_core_stats: bool,
    pub client: ClientConfig<ReqwestClientSpec>,
}

/// Retry strategy applied to each HTTP request.
#[derive(Debug, Clone, Deserialize)]
pub struct HttpRetryConfig {
    #[serde(default = "HttpRetryConfig::default_max_attempts")]
    pub max_attempts: usize,
    #[serde(default = "HttpRetryConfig::default_initial_delay_ms")]
    pub initial_delay_ms: u64,
    #[serde(default = "HttpRetryConfig::default_backoff_multiplier")]
    pub backoff_multiplier: f64,
    #[serde(default = "HttpRetryConfig::default_max_delay_ms")]
    pub max_delay_ms: u64,
}

impl Default for HttpRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: Self::default_max_attempts(),
            initial_delay_ms: Self::default_initial_delay_ms(),
            backoff_multiplier: Self::default_backoff_multiplier(),
            max_delay_ms: Self::default_max_delay_ms(),
        }
    }
}

impl HttpRetryConfig {
    #[inline]
    const fn default_max_attempts() -> usize {
        3
    }

    #[inline]
    const fn default_initial_delay_ms() -> u64 {
        200
    }

    #[inline]
    const fn default_backoff_multiplier() -> f64 {
        2.0
    }

    #[inline]
    const fn default_max_delay_ms() -> u64 {
        2_000
    }

    /// Returns the total number of attempts the runner should make (including the first request).
    #[inline]
    pub fn attempts(&self) -> usize {
        self.max_attempts.max(1)
    }

    /// Calculates the delay before the given retry attempt (attempt >= 1).
    #[inline]
    pub fn delay_for_attempt(&self, attempt: usize) -> Duration {
        if attempt == 0 {
            return Duration::from_millis(0);
        }

        let base = self.initial_delay_ms as f64;
        let factor = self.backoff_multiplier.clamp(1.0, 10.0);
        let exp = factor.powi((attempt - 1) as i32);
        let millis = (base * exp).min(self.max_delay_ms as f64) as u64;
        Duration::from_millis(millis)
    }
}
