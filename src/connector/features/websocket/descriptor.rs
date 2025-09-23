use crate::connector::{Kind, StreamDescriptor, Venue};
use crate::utils::CorePickPolicy;
use serde::Deserialize;

const fn default_idle_sleep_us() -> u64 { 200 } 

// описывает поведение раннера 
#[derive(Debug, Clone, Deserialize)]
pub struct WsStreamingDescriptor {
    pub max_hook_calls_at_once: usize,
    pub max_pending_actions: Option<usize>,
    pub max_pending_events: Option<usize>,
    pub core_pick_policy: Option<CorePickPolicy>,
    #[serde(default = "default_idle_sleep_us")]
    pub idle_sleep_us: u64,                 
}

impl Default for WsStreamingDescriptor {
    fn default() -> Self {
        Self {
            max_hook_calls_at_once: 10,
            max_pending_actions: None,
            max_pending_events: None,
            core_pick_policy: None,
            idle_sleep_us: default_idle_sleep_us(),
        }
    }
}

impl StreamDescriptor for WsStreamingDescriptor {
    fn venue(&self) -> impl Venue { "any" }
    fn kind(&self)  -> impl Kind  { "ws_streaming" }
    fn max_pending_actions(&self) -> Option<usize> { self.max_pending_actions }
    fn max_pending_events(&self)  -> Option<usize> { self.max_pending_events }
    fn core_pick_policy(&self)    -> Option<CorePickPolicy> { self.core_pick_policy }
    fn health_at_start(&self) -> bool { false }
}