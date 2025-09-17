use bytes::Bytes;
use serde_json::Value;
use std::time::Duration;
use tonic::metadata::{AsciiMetadataKey, MetadataMap, MetadataValue};
use uuid::Uuid;

use crate::connector::features::reqwest::rate_limiter::RateLimitContext;
/// Универсальное описание RPC-вызова (без конкретных protobuf-типов).
#[derive(Debug, Clone)]
pub enum GrpcCall {
    /// Unary: один запрос → один ответ.
    Unary { method: String, message: Bytes },
    /// Server streaming: один запрос → поток ответов.
    ServerStreaming { method: String, message: Bytes },
    /// Client streaming: поток запросов → один ответ.
    ClientStreaming {
        method: String,
        messages: Vec<Bytes>,
    },
    /// BiDi streaming: двунаправленный поток.
    BidiStreaming {
        method: String,
        messages: Vec<Bytes>,
    },
}

#[derive(Debug, Clone)]
pub struct GrpcAction {
    pub call: GrpcCall,          // какой тип вызова
    pub metadata: MetadataMap,   // заголовки gRPC
    pub conn_id: Option<u16>,    // к какому коннекту пойти
    pub req_id: Option<Uuid>,    // идентификатор запроса
    pub label: Option<&'static str>, // произвольный ярлык
    pub timeout: Option<Duration>,   // таймаут
    pub rl_ctx: Option<Bytes>,       // контекст для rate-limiter
    pub rl_weight: Option<usize>,    // вес запроса для rate-limiter
    pub payload: Option<Value>,      // "сырой" JSON payload (для логов/отладки)
}

impl GrpcAction {
    pub fn builder(call: GrpcCall) -> ActionBuilder {
        ActionBuilder::new(call)
    }
}

#[derive(Debug, Clone)]
pub struct ActionBuilder {
    call: GrpcCall,
    metadata: MetadataMap,
    conn_id: Option<u16>,
    req_id: Option<Uuid>,
    label: Option<&'static str>,
    timeout: Option<Duration>,
    rl_ctx: Option<Bytes>,
    rl_weight: Option<usize>,
    payload: Option<Value>,
}

impl ActionBuilder {
    pub fn new(call: GrpcCall) -> Self {
        Self {
            call,
            metadata: MetadataMap::new(),
            conn_id: None,
            req_id: None,
            label: None,
            timeout: None,
            rl_ctx: None,
            rl_weight: None,
            payload: None,
        }
    }

    pub fn req_id(mut self, id: Uuid) -> Self {
        self.req_id = Some(id);
        self
    }
    pub fn conn_id(mut self, id: u16) -> Self {
        self.conn_id = Some(id);
        self
    }
    pub fn timeout(mut self, t: Duration) -> Self {
        self.timeout = Some(t);
        self
    }
    pub fn payload(mut self, payload: Value) -> Self {
        self.payload = Some(payload);
        self
    }
    pub fn label(mut self, label: &'static str) -> Self {
        self.label = Some(label);
        self
    }

    pub fn header_kv(mut self, k: &str, v: &str) -> Self {
        if let (Ok(key), Ok(val)) = (k.parse::<AsciiMetadataKey>(), MetadataValue::try_from(v)) {
            self.metadata.insert(key, val);
        }
        self
    }
    pub fn bearer(self, token: &str) -> Self {
        self.header_kv("authorization", &format!("Bearer {}", token))
    }
    pub fn api_key_header(self, name: &str, value: &str) -> Self {
        self.header_kv(name, value)
    }

    pub fn rl_ctx(mut self, ctx: RateLimitContext<'_>) -> Self {
        self.rl_ctx = Some(ctx.to_bytes());
        self
    }
    pub fn rl_ctx_bytes(mut self, ctx: Bytes) -> Self {
        self.rl_ctx = Some(ctx);
        self
    }
    pub fn rl_weight(mut self, w: usize) -> Self {
        self.rl_weight = Some(w);
        self
    }

    pub fn build(self) -> GrpcAction {
        GrpcAction {
            call: self.call,
            metadata: self.metadata,
            conn_id: self.conn_id,
            req_id: self.req_id,
            label: self.label,
            timeout: self.timeout,
            rl_ctx: self.rl_ctx,
            rl_weight: self.rl_weight,
            payload: self.payload,
        }
    }
}

/// Хелперы-конструкторы
pub fn unary(method: impl Into<String>, msg: Bytes) -> ActionBuilder {
    GrpcAction::builder(GrpcCall::Unary {
        method: method.into(),
        message: msg,
    })
}
pub fn server_streaming(method: impl Into<String>, msg: Bytes) -> ActionBuilder {
    GrpcAction::builder(GrpcCall::ServerStreaming {
        method: method.into(),
        message: msg,
    })
}
pub fn client_streaming(method: impl Into<String>, messages: Vec<Bytes>) -> ActionBuilder {
    GrpcAction::builder(GrpcCall::ClientStreaming {
        method: method.into(),
        messages,
    })
}
pub fn bidi_streaming(method: impl Into<String>, messages: Vec<Bytes>) -> ActionBuilder {
    GrpcAction::builder(GrpcCall::BidiStreaming {
        method: method.into(),
        messages,
    })
}
