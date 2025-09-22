use std::fmt;

use bytes::{Bytes, BytesMut};
use prost::Message;
use serde::Deserialize;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::metadata::{AsciiMetadataKey, MetadataMap, MetadataValue};

use crate::connector::features::shared::actions::StreamActionBuilder;
use crate::connector::features::tonic::unary::GrpcMethod;

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
pub enum StreamingAction {
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

impl StreamingAction {
    pub fn connect(method: GrpcMethod, mode: StreamingMode) -> StreamingConnectBuilder {
        StreamingConnectBuilder::new(method, mode)
    }

    pub fn send_encoded(message: Bytes) -> Self {
        StreamingAction::Send(message)
    }

    pub fn send<T: Message>(message: T) -> Self {
        let mut buf = BytesMut::with_capacity(message.encoded_len());
        message.encode(&mut buf).unwrap();
        StreamingAction::Send(buf.freeze())
    }

    pub fn finish() -> Self {
        StreamingAction::Finish
    }

    pub fn disconnect() -> Self {
        StreamingAction::Disconnect
    }

    pub fn to_builder(self) -> StreamActionBuilder<StreamingAction> {
        StreamActionBuilder::new(Some(self))
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
    fn new(method: GrpcMethod, mode: StreamingMode) -> Self {
        Self {
            mode,
            method: method.parse(),
            initial_message: None,
            metadata: MetadataMap::new(),
        }
    }

    /// Set the initial subscription payload sent when the stream is opened.
    pub fn subscription<T>(mut self, payload: T) -> Self
    where
        T: Message,
    {
        let mut buf = BytesMut::with_capacity(payload.encoded_len());
        payload.encode(&mut buf).unwrap();
        self.initial_message = Some(buf.freeze());
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

    pub fn build(self) -> StreamingAction {
        StreamingAction::Connect(ConnectConfig {
            mode: self.mode,
            method: self.method,
            initial_message: self.initial_message,
            metadata: self.metadata,
        })
    }
}
