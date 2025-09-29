use serde::Deserialize;

use crate::connector::{Kind, StreamDescriptor, Venue};
use crate::utils::CorePickPolicy;

#[derive(Debug, Clone, Deserialize)]
pub struct WebSocketStreamDescriptor {
    pub max_hook_calls_at_once: usize,
    pub wait_async_tasks_us: u64,
    pub max_pending_actions: Option<usize>,
    pub max_pending_events: Option<usize>,
    pub core_pick_policy: Option<CorePickPolicy>,
}

impl WebSocketStreamDescriptor {
    pub fn new(
        max_hook_calls_at_once: Option<usize>,
        wait_async_tasks_us: Option<u64>,
        max_pending_actions: Option<usize>,
        max_pending_events: Option<usize>,
        core_pick_policy: Option<CorePickPolicy>,
    ) -> Self {
        Self {
            max_hook_calls_at_once: max_hook_calls_at_once.filter(|v| *v > 0).unwrap_or(32),
            wait_async_tasks_us: wait_async_tasks_us.unwrap_or(50),
            max_pending_actions,
            max_pending_events,
            core_pick_policy,
        }
    }

    pub fn low_latency() -> Self {
        Self {
            max_hook_calls_at_once: 8,
            wait_async_tasks_us: 0,
            max_pending_actions: Some(512),
            max_pending_events: Some(512),
            core_pick_policy: None,
        }
    }

    pub fn high_throughput() -> Self {
        Self {
            max_hook_calls_at_once: 128,
            wait_async_tasks_us: 200,
            max_pending_actions: Some(2048),
            max_pending_events: Some(2048),
            core_pick_policy: None,
        }
    }
}

impl Default for WebSocketStreamDescriptor {
    fn default() -> Self {
        Self {
            max_hook_calls_at_once: 32,
            wait_async_tasks_us: 50,
            max_pending_actions: None,
            max_pending_events: None,
            core_pick_policy: None,
        }
    }
}

impl StreamDescriptor for WebSocketStreamDescriptor {
    fn venue(&self) -> impl Venue {
        "any"
    }

    fn kind(&self) -> impl Kind {
        "websocket"
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
}
