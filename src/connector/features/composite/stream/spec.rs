use std::collections::HashMap;

use crate::connector::features::composite::stream::event::{StreamEventContext, StreamEventParsed};
use crate::connector::features::grpc::stream::GrpcDescriptor;
use crate::connector::features::http::stream::descriptor::HttpDescriptor;
use crate::connector::features::shared::rate_limiter::RateLimitConfig;
use crate::connector::features::websocket::stream::WebSocketStreamDescriptor;
use serde::Deserialize;
use serde_json::Value;

use crate::utils::{CorePickPolicy, SharedStr};

#[derive(Debug, Clone, Deserialize)]
pub struct StreamSpec {
    pub name: SharedStr,
    pub kind: StreamKind,
    #[serde(default)]
    pub ignore_init: Option<bool>,
    #[serde(default = "default_hook_calls_at_once")]
    pub max_hook_calls_at_once: usize,
    #[serde(default = "default_wait_async_task")]
    pub wait_async_tasks_us: u64,
    #[serde(default)]
    pub max_pending_actions: Option<usize>,
    #[serde(default)]
    pub max_pending_events: Option<usize>,
    #[serde(default)]
    pub core_pick_policy: Option<CorePickPolicy>,
    #[serde(default)]
    pub rate_limits: Vec<RateLimitConfig>,
    #[serde(default)]
    pub max_decoding_message_size: Option<usize>,
    #[serde(default)]
    pub max_encoding_message_size: Option<usize>,
    #[serde(default)]
    pub metadata: HashMap<String, Value>,
}

fn default_wait_async_task() -> u64 {
    1000
}

fn default_hook_calls_at_once() -> usize {
    128
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamKind {
    Http,
    Grpc,
    Ws,
}

impl AsRef<str> for StreamKind {
    fn as_ref(&self) -> &str {
        match self {
            StreamKind::Http => "http",
            StreamKind::Grpc => "grpc",
            StreamKind::Ws => "ws",
        }
    }
}

impl StreamSpec {
    pub fn get_context<E: StreamEventParsed>(&self) -> StreamEventContext<E> {
        let mut ctx = StreamEventContext::new(self.name.as_str());
        ctx.set_metadata(self.metadata.clone());
        ctx
    }

    pub fn maybe_http<E: StreamEventParsed>(
        &self,
        payload: &StreamEventContext<E>,
    ) -> Option<HttpDescriptor<StreamEventContext<E>>> {
        match self.kind {
            StreamKind::Http => Some(HttpDescriptor::new(
                Some(self.max_hook_calls_at_once),
                Some(self.wait_async_tasks_us),
                self.max_pending_actions,
                self.max_pending_events,
                self.core_pick_policy,
                Some(self.rate_limits.clone()),
                Some(payload.clone()),
            )),
            _ => None,
        }
    }

    pub fn maybe_grpc<E: StreamEventParsed>(
        &self,
        payload: &StreamEventContext<E>,
    ) -> Option<GrpcDescriptor<StreamEventContext<E>>> {
        match self.kind {
            StreamKind::Grpc => Some(GrpcDescriptor::new(
                Some(self.max_hook_calls_at_once),
                Some(self.wait_async_tasks_us),
                self.max_pending_actions,
                self.max_pending_events,
                self.core_pick_policy,
                Some(self.rate_limits.clone()),
                self.max_decoding_message_size,
                self.max_encoding_message_size,
                None,
                Some(payload.clone()),
            )),
            _ => None,
        }
    }

    pub fn maybe_ws<E: StreamEventParsed>(
        &self,
        payload: &StreamEventContext<E>,
    ) -> Option<WebSocketStreamDescriptor<StreamEventContext<E>>> {
        match self.kind {
            StreamKind::Ws => Some(WebSocketStreamDescriptor::new(
                Some(self.max_hook_calls_at_once),
                Some(self.wait_async_tasks_us),
                self.max_pending_actions,
                self.max_pending_events,
                self.core_pick_policy,
                Some(self.rate_limits.clone()),
                Some(payload.clone()),
            )),
            _ => None,
        }
    }
}
