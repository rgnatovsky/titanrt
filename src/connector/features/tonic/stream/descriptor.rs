use std::fmt::Debug;

use crate::connector::features::shared::rate_limiter::RateLimitConfig;
use crate::connector::{Kind, StreamDescriptor, Venue};
use crate::utils::CorePickPolicy;
use serde::Deserialize;

const fn default_outbound_buffer() -> usize {
    64
}

#[derive(Debug, Clone, Deserialize)]
pub struct TonicDescriptor<T> {
    /// Maximum number of hook calls at once.
    pub max_hook_calls_at_once: usize,
    /// Delay between async tasks.
    pub wait_async_tasks_us: u64,
    /// Maximum number of pending actions.
    pub max_pending_actions: Option<usize>,
    /// Maximum number of pending events.
    pub max_pending_events: Option<usize>,
    /// Processor Core selection policy.
    pub core_pick_policy: Option<CorePickPolicy>,
    /// Rate limits.
    #[serde(default)]
    pub rate_limits: Vec<RateLimitConfig>,
    /// Limits the maximum size of a decoded message.
    pub max_decoding_message_size: Option<usize>,
    /// Limits the maximum size of an encoded message.
    pub max_encoding_message_size: Option<usize>,
    /// Buffer for outbound streaming messages (client/bidi).
    #[serde(default = "default_outbound_buffer")]
    pub outbound_buffer: usize,
    pub custom_data: Option<T>,
}

impl<T> TonicDescriptor<T> {
    pub fn new(
        max_hook_calls_at_once: Option<usize>,
        wait_async_tasks_us: Option<u64>,
        max_pending_actions: Option<usize>,
        max_pending_events: Option<usize>,
        core_pick_policy: Option<CorePickPolicy>,
        rate_limits: Option<Vec<RateLimitConfig>>,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
        outbound_buffer: Option<usize>,
        custom_data: Option<T>,
    ) -> Self {
        let mut descriptor = Self::default();
        descriptor.max_hook_calls_at_once = max_hook_calls_at_once.filter(|&x| x > 0).unwrap_or(10);
        descriptor.wait_async_tasks_us = wait_async_tasks_us.unwrap_or(100);
        descriptor.max_pending_actions = max_pending_actions;
        descriptor.max_pending_events = max_pending_events;
        descriptor.core_pick_policy = core_pick_policy;
        descriptor.rate_limits = rate_limits.unwrap_or_default();
        descriptor.max_decoding_message_size = max_decoding_message_size;
        descriptor.max_encoding_message_size = max_encoding_message_size;
        descriptor.outbound_buffer = outbound_buffer.unwrap_or(default_outbound_buffer());
        descriptor.custom_data = custom_data;
        descriptor
    }

    pub fn low_latency() -> Self {
        let mut descriptor = Self::default();
        descriptor.max_hook_calls_at_once = 4;
        descriptor.wait_async_tasks_us = 0;
        descriptor
    }

    pub fn high_throughput() -> Self {
        let mut descriptor = Self::default();
        descriptor.max_hook_calls_at_once = 64;
        descriptor.wait_async_tasks_us = 200;
        descriptor
    }

    pub fn add_rate_limit(&mut self, rl: RateLimitConfig) {
        self.rate_limits.push(rl);
    }
}

impl<T> Default for TonicDescriptor<T> {
    fn default() -> Self {
        Self {
            max_hook_calls_at_once: 10,
            wait_async_tasks_us: 100,
            max_pending_actions: None,
            max_pending_events: None,
            core_pick_policy: None,
            rate_limits: Vec::new(),
            max_decoding_message_size: None,
            max_encoding_message_size: None,
            outbound_buffer: default_outbound_buffer(),
            custom_data: None,
        }
    }
}

impl<T: Debug + Clone + Send + 'static> StreamDescriptor<T> for TonicDescriptor<T> {
    fn venue(&self) -> impl Venue {
        "any"
    }

    fn kind(&self) -> impl Kind {
        "tonic"
    }

    fn max_pending_actions(&self) -> Option<usize> {
        self.max_pending_actions
    }

    fn max_pending_events(&self) -> Option<usize> {
        self.max_pending_events
    }

    fn core_pick_policy(&self) -> Option<CorePickPolicy> {
        self.core_pick_policy
    }

    fn health_at_start(&self) -> bool {
        false
    }

    fn custom_data(&self) -> Option<&T> {
        self.custom_data.as_ref()
    }
}
