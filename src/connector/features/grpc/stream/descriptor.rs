use crate::connector::{Kind, StreamDescriptor, Venue};
use crate::utils::CorePickPolicy;
use serde::Deserialize;
use crate::connector::features::reqwest::rate_limiter::RateLimitConfig;

/// Конфиг потока: лимиты, задержки, политика выбора core, настройки rate-limit.
/// Есть удобные фабрики: low_latency(), high_throughput().
#[derive(Debug, Clone, Deserialize)]
pub struct GrpcStreamDescriptor {
    pub max_hook_calls_at_once: usize, // сколько событий можно обработать за цикл
    pub wait_async_tasks_us: u64,      // задержка между задачами
    pub max_pending_actions: Option<usize>,
    pub max_pending_events: Option<usize>,
    pub core_pick_policy: Option<CorePickPolicy>,
    pub rate_limits: Vec<RateLimitConfig>,
}

impl GrpcStreamDescriptor {
    pub fn new(
        max_hook_calls_at_once: Option<usize>,
        wait_async_tasks_us: Option<u64>,
        max_pending_actions: Option<usize>,
        max_pending_events: Option<usize>,
        core_pick_policy: Option<CorePickPolicy>,
        rate_limits: Option<Vec<RateLimitConfig>>,
    ) -> Self {
        let max_hook_calls_at_once = max_hook_calls_at_once.filter(|&x| x > 0).unwrap_or(10);
        let wait_async_tasks_us = wait_async_tasks_us.unwrap_or(100);
        Self {
            max_hook_calls_at_once,
            wait_async_tasks_us,
            max_pending_actions,
            max_pending_events,
            core_pick_policy,
            rate_limits: rate_limits.unwrap_or_default(),
        }
    }

    pub fn low_latency() -> Self { Self { max_hook_calls_at_once: 4, wait_async_tasks_us: 0, ..Default::default() } }
    pub fn high_throughput() -> Self { Self { max_hook_calls_at_once: 64, wait_async_tasks_us: 200, ..Default::default() } }
    pub fn add_rate_limit(&mut self, rl: RateLimitConfig) { self.rate_limits.push(rl); }
}

impl Default for GrpcStreamDescriptor {
    fn default() -> Self {
        Self {
            max_hook_calls_at_once: 10,
            wait_async_tasks_us: 100,
            max_pending_actions: None,
            max_pending_events: None,
            core_pick_policy: None,
            rate_limits: vec![],
        }
    }
}

impl StreamDescriptor for GrpcStreamDescriptor {
    fn venue(&self) -> impl Venue { "any" }
    fn kind(&self) -> impl Kind { "grpc" }
    fn max_pending_actions(&self) -> Option<usize> { self.max_pending_actions }
    fn max_pending_events(&self) -> Option<usize> { self.max_pending_events }
    fn core_pick_policy(&self) -> Option<CorePickPolicy> { self.core_pick_policy }
    fn health_at_start(&self) -> bool { false }
}