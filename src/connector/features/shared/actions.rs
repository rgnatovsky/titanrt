use std::borrow::Cow;
use std::time::Duration;

use bytes::Bytes;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use crate::connector::features::shared::rate_limiter::RateLimitContext;

/// Fluent builder for StreamAction
#[derive(Debug)]
pub struct StreamActionBuilder<Inner> {
    inner: Option<Inner>,
    timeout: Option<Duration>,
    req_id: Option<Uuid>,
    label: Option<Cow<'static, str>>,
    conn_id: Option<usize>,
    rl_ctx: Option<Bytes>,
    rl_weight: Option<usize>,
    json: Option<Value>,
}

impl<Inner> StreamActionBuilder<Inner> {
    /// Create a builder from a spec
    pub fn new(spec: Option<Inner>) -> Self {
        Self {
            inner: spec,
            timeout: None,
            req_id: None,
            label: None,
            conn_id: None,
            rl_ctx: None,
            rl_weight: None,
            json: None,
        }
    }

    /// consume builder and set req_id
    pub fn req_id(mut self, id: Uuid) -> Self {
        self.req_id = Some(id);
        self
    }

    /// set conn id
    pub fn conn_id(mut self, id: usize) -> Self {
        self.conn_id = Some(id);
        self
    }

    /// set timeout
    pub fn timeout(mut self, t: Duration) -> Self {
        self.timeout = Some(t);
        self
    }

    /// set JSON payload directly
    pub fn payload_value(mut self, payload: Value) -> Self {
        self.json = Some(payload);
        self
    }

    /// set payload from any `Serialize` type (may fail)
    pub fn payload_from<T: Serialize>(mut self, payload: T) -> Result<Self, serde_json::Error> {
        self.json = Some(serde_json::to_value(payload)?);
        Ok(self)
    }

    /// flexible label: accepts &'static str or String
    pub fn label<S>(mut self, label: S) -> Self
    where
        S: Into<Cow<'static, str>>,
    {
        self.label = Some(label.into());
        self
    }

    /// accepts RateLimitContext and converts to Bytes
    pub fn rl_ctx(mut self, ctx: RateLimitContext) -> Self {
        self.rl_ctx = Some(ctx.to_bytes());
        self
    }

    /// accepts any Into<Bytes>
    pub fn rl_ctx_bytes<B>(mut self, bytes: B) -> Self
    where
        B: Into<Bytes>,
    {
        self.rl_ctx = Some(bytes.into());
        self
    }

    /// set rate-limit weight
    pub fn rl_weight(mut self, weight: usize) -> Self {
        self.rl_weight = Some(weight);
        self
    }

    /// set inner action
    pub fn inner(mut self, inner: Inner) -> Self {
        self.inner = Some(inner);
        self
    }

    /// finalize build
    pub fn build(self) -> StreamAction<Inner> {
        StreamAction {
            inner: self.inner,
            conn_id: self.conn_id,
            req_id: self.req_id,
            label: self.label,
            timeout: self.timeout,
            rl_ctx: self.rl_ctx,
            rl_weight: self.rl_weight,
            json: self.json,
        }
    }
}

/// StreamAction holds a spec + optional metadata.
/// Поля сделаны приватными — используйте геттеры.
#[derive(Debug)]
pub struct StreamAction<Inner> {
    inner: Option<Inner>,
    conn_id: Option<usize>,
    req_id: Option<Uuid>,
    label: Option<Cow<'static, str>>,
    timeout: Option<Duration>,
    rl_ctx: Option<Bytes>,
    rl_weight: Option<usize>,
    json: Option<Value>,
}

impl<Inner> StreamAction<Inner> {
    /// Create a builder
    pub fn builder(inner: Option<Inner>) -> StreamActionBuilder<Inner> {
        StreamActionBuilder::new(inner)
    }
    /// Get a reference to the inner spec
    pub fn inner(&self) -> Option<&Inner> {
        self.inner.as_ref()
    }
    /// Take the inner spec
    pub fn inner_take(&mut self) -> Option<Inner> {
        self.inner.take()
    }

    /// Consume and return the inner spec
    pub fn into_inner(self) -> Option<Inner> {
        self.inner
    }

    pub fn conn_id(&self) -> Option<usize> {
        self.conn_id
    }

    pub fn req_id(&self) -> Option<Uuid> {
        self.req_id
    }

    pub fn label(&self) -> Option<&str> {
        self.label.as_deref()
    }
    pub fn label_take(&mut self) -> Option<Cow<'static, str>> {
        self.label.take()
    }

    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    pub fn rl_ctx(&self) -> Option<&Bytes> {
        self.rl_ctx.as_ref()
    }

    pub fn rl_weight(&self) -> Option<usize> {
        self.rl_weight
    }

    pub fn json(&self) -> Option<&Value> {
        self.json.as_ref()
    }
    pub fn json_take(&mut self) -> Option<Value> {
        self.json.take()
    }

    /// Return tuple with owned parts, useful when you need to move everything
    pub fn into_parts(
        self,
    ) -> (
        Option<Inner>,
        Option<usize>,
        Option<Uuid>,
        Option<Cow<'static, str>>,
        Option<Duration>,
        Option<Bytes>,
        Option<usize>,
        Option<Value>,
    ) {
        (
            self.inner,
            self.conn_id,
            self.req_id,
            self.label,
            self.timeout,
            self.rl_ctx,
            self.rl_weight,
            self.json,
        )
    }
}

// conditional Clone: только если Inner: Clone
impl<Inner: Clone> Clone for StreamAction<Inner> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            conn_id: self.conn_id,
            req_id: self.req_id,
            label: self.label.clone(),
            timeout: self.timeout,
            rl_ctx: self.rl_ctx.clone(),
            rl_weight: self.rl_weight,
            json: self.json.clone(),
        }
    }
}
