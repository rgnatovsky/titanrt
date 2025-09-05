use crate::connector::features::reqwest::rate_limiter::RateLimitContext;
use bytes::Bytes;
use reqwest::Client;
pub use reqwest::{header::HeaderMap, Method, Url};
use serde::Serialize;
use serde_json::Value;
use std::time::Duration;
use uuid::Uuid;

/// Represents the body of an HTTP request.
#[derive(Debug, Clone)]
pub enum ActionBody {
    /// No body (e.g. for GET or DELETE).
    Empty,
    /// Raw bytes with an optional Content-Type header.
    Bytes {
        content_type: Option<&'static str>,
        bytes: Bytes,
    },
    /// JSON value to be serialized into the request body.
    Json(Value),
    /// Form-encoded key-value pairs.
    Form(Vec<(String, String)>),
}

impl ActionBody {
    /// Applies the body variant to a reqwest RequestBuilder.
    pub(crate) fn apply_to(self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match self {
            ActionBody::Empty => rb,

            ActionBody::Bytes {
                content_type,
                bytes,
            } => {
                let rb = if let Some(ct) = content_type {
                    rb.header("Content-Type", ct)
                } else {
                    rb
                };
                rb.body(bytes)
            }

            ActionBody::Json(v) => {
                let buf = serde_json::to_vec(&v).expect("json serialize");
                rb.header("Content-Type", "application/json")
                    .body(Bytes::from(buf))
            }

            ActionBody::Form(pairs) => rb.form(&pairs),
        }
    }
}

/// Builder for constructing Action objects with a fluent API.
#[derive(Debug)]
pub struct ActionBuilder {
    method: Method,
    url: Url,
    body: ActionBody,
    query: Vec<(String, String)>,
    headers: HeaderMap,
    timeout: Option<Duration>,
    req_id: Option<Uuid>,
    label: Option<&'static str>,
    ip_id: Option<u16>,
    rl_ctx: Option<Bytes>,
}

impl ActionBuilder {
    /// Creates a new builder with the given HTTP method and URL.
    pub fn new(method: Method, url: Url) -> Self {
        Self {
            method,
            url,
            body: ActionBody::Empty,
            query: Vec::new(),
            headers: HeaderMap::new(),
            req_id: None,
            label: None,
            ip_id: None,
            rl_ctx: None,
            timeout: None,
        }
    }

    /// Sets the request identifier (trace ID).
    pub fn req_id(mut self, id: Uuid) -> Self {
        self.req_id = Some(id);
        self
    }
    /// Sets the local IP identifier used for client selection.
    pub fn ip_id(mut self, id: u16) -> Self {
        self.ip_id = Some(id);
        self
    }
    /// Sets a request timeout.
    pub fn timeout(mut self, t: Duration) -> Self {
        self.timeout = Some(t);
        self
    }

    /// Adds a custom header key-value pair.
    pub fn header_kv(mut self, k: &str, v: &str) -> Self {
        use reqwest::header::{HeaderName, HeaderValue};
        if let (Ok(k), Ok(v)) = (k.parse::<HeaderName>(), HeaderValue::from_str(v)) {
            self.headers.insert(k, v);
        }
        self
    }
    /// Sets the Authorization header with a bearer token.
    pub fn bearer(mut self, token: &str) -> Self {
        self = self.header_kv("Authorization", &format!("Bearer {}", token));
        self
    }
    /// Sets a custom API key header.
    pub fn api_key_header(self, name: &str, value: &str) -> Self {
        self.header_kv(name, value)
    }

    /// Adds a query parameter.
    pub fn query(mut self, k: &str, v: &str) -> Self {
        self.query.push((k.to_string(), v.to_string()));
        self
    }
    /// Adds a query parameter if value is Some.
    pub fn query_opt(mut self, k: &str, v: Option<impl ToString>) -> Self {
        if let Some(v) = v {
            self.query.push((k.to_string(), v.to_string()));
        }
        self
    }
    /// Serializes a struct into query parameters using serde.
    pub fn query_serde<T: Serialize>(mut self, v: &T) -> Self {
        let s = serde_urlencoded::to_string(v).unwrap_or_default();
        if !s.is_empty() {
            for pair in s.split('&') {
                if let Some((k, v)) = pair.split_once('=') {
                    self.query.push((k.to_string(), v.to_string()));
                }
            }
        }
        self
    }

    /// Sets a label for the request.
    pub fn label(mut self, label: &'static str) -> Self {
        self.label = Some(label);
        self
    }

    /// Sets an empty body.
    pub fn empty(mut self) -> Self {
        self.body = ActionBody::Empty;
        self
    }
    /// Sets a JSON body from a serde_json::Value.
    pub fn json_val(mut self, v: Value) -> Self {
        self.body = ActionBody::Json(v);
        self
    }
    /// Sets a JSON body from a serializable type.
    pub fn json<T: Serialize>(mut self, v: &T) -> Self {
        self.body = ActionBody::Json(serde_json::to_value(v).expect("serialize json"));
        self
    }
    /// Sets a form-encoded body.
    pub fn form(mut self, pairs: Vec<(impl Into<String>, impl Into<String>)>) -> Self {
        self.body = ActionBody::Form(
            pairs
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        );
        self
    }
    /// Sets a raw bytes body without Content-Type.
    pub fn bytes(mut self, bytes: Bytes) -> Self {
        self.body = ActionBody::Bytes {
            content_type: None,
            bytes,
        };
        self
    }
    /// Sets a raw bytes body with explicit Content-Type.
    pub fn bytes_with_ct(mut self, bytes: Bytes, ct: &'static str) -> Self {
        self.body = ActionBody::Bytes {
            content_type: Some(ct),
            bytes,
        };
        self
    }

    pub fn rl_ctx(mut self, ctx: RateLimitContext) -> Self {
        self.rl_ctx = Some(ctx.to_bytes());
        self
    }

    pub fn rl_ctx_bytes(mut self, ctx: Bytes) -> Self {
        self.rl_ctx = Some(ctx);
        self
    }

    /// Builds and returns the final Action.
    pub fn build(self) -> ReqwestAction {
        ReqwestAction {
            req_id: self.req_id,
            ip_id: self.ip_id,
            method: self.method,
            rl_ctx: self.rl_ctx,
            label: self.label,
            url: self.url,
            headers: self.headers,
            timeout: self.timeout,
            body: self.body,
            query: self.query,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReqwestAction {
    pub method: Method,
    pub url: Url,
    pub body: ActionBody,
    pub query: Vec<(String, String)>,
    pub headers: HeaderMap,
    pub req_id: Option<Uuid>,
    pub label: Option<&'static str>,
    pub ip_id: Option<u16>,
    pub rl_ctx: Option<Bytes>,
    pub timeout: Option<Duration>,
}

impl ReqwestAction {
    pub(crate) fn to_request_builder(self, client: &Client) -> reqwest::RequestBuilder {
        let mut rb = client.request(self.method, self.url);
        if !self.headers.is_empty() {
            rb = rb.headers(self.headers);
        }
        if !self.query.is_empty() {
            rb = rb.query(&self.query);
        }

        if let Some(timeout) = self.timeout {
            rb = rb.timeout(timeout);
        }

        self.body.apply_to(rb)
    }

    pub fn builder(method: Method, url: Url) -> ActionBuilder {
        ActionBuilder::new(method, url)
    }
    pub fn get(url: Url) -> ActionBuilder {
        Self::builder(Method::GET, url)
    }
    pub fn post(url: Url) -> ActionBuilder {
        Self::builder(Method::POST, url)
    }
    pub fn put(url: Url) -> ActionBuilder {
        Self::builder(Method::PUT, url)
    }
    pub fn delete(url: Url) -> ActionBuilder {
        Self::builder(Method::DELETE, url)
    }
}
