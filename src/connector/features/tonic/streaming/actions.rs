use std::fmt;

use bytes::Bytes;
use serde::Deserialize;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::metadata::{AsciiMetadataKey, MetadataMap, MetadataValue};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamingMode {
    Server,
    Client,
    Bidi,
}


impl fmt::Display for StreamingMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let repr = match self {
            StreamingMode::Server => "server_streaming",
            StreamingMode::Client => "client_streaming",
            StreamingMode::Bidi => "bidi_streaming",
        };
        f.write_str(repr)
    }
}

/// Control plane instructions for a persistent tonic streaming session.
#[derive(Debug, Clone)]
pub enum StreamingActionInner {
    /// Establish (or re-establish) the gRPC stream.
    Connect(ConnectConfig),
    /// Push a payload into an active client/bidirectional stream.
    Send(Bytes),
    /// Close the outbound side of the stream (client or bidi).
    Finish,
    /// Abort the stream locally.
    Disconnect,
}

#[derive(Debug, Clone)]
pub struct ConnectConfig {
    pub mode: StreamingMode,
    pub method: PathAndQuery,
    pub initial_message: Option<Bytes>,
    pub metadata: MetadataMap,
}

impl StreamingActionInner {
    pub fn connect(method: &str, mode: StreamingMode) -> StreamingConnectBuilder {
        StreamingConnectBuilder::new(method, mode)
    }

    pub fn send(message: Bytes) -> Self {
        StreamingActionInner::Send(message)
    }

    pub fn finish() -> Self {
        StreamingActionInner::Finish
    }

    pub fn disconnect() -> Self {
        StreamingActionInner::Disconnect
    }
}

#[derive(Debug, Clone)]
pub struct StreamingConnectBuilder {
    mode: StreamingMode,
    method: PathAndQuery,
    initial_message: Option<Bytes>,
    metadata: MetadataMap,
}

impl StreamingConnectBuilder {
    fn new(method: &str, mode: StreamingMode) -> Self {
        Self {
            mode,
            method: method
                .parse()
                .expect("invalid gRPC method path for streaming connect"),
            initial_message: None,
            metadata: MetadataMap::new(),
        }
    }

    /// Set the initial subscription payload sent when the stream is opened.
    pub fn subscription<B>(mut self, payload: B) -> Self
    where
        B: Into<Bytes>,
    {
        self.initial_message = Some(payload.into());
        self
    }

    pub fn header_kv(mut self, key: &str, value: &str) -> Self {
        if let (Ok(k), Ok(v)) = (
            key.parse::<AsciiMetadataKey>(),
            MetadataValue::try_from(value),
        ) {
            self.metadata.insert(k, v);
        }
        self
    }

    pub fn bearer(self, token: &str) -> Self {
        self.header_kv("authorization", &format!("Bearer {}", token))
    }

    pub fn api_key_header(self, name: &str, value: &str) -> Self {
        self.header_kv(name, value)
    }

    pub fn build(self) -> StreamingActionInner {
        StreamingActionInner::Connect(ConnectConfig {
            mode: self.mode,
            method: self.method,
            initial_message: self.initial_message,
            metadata: self.metadata,
        })
    }
}
