use std::fmt;

use bytes::{Bytes, BytesMut};
use prost::Message;
use serde::Deserialize;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::metadata::{AsciiMetadataKey, MetadataMap, MetadataValue};

use crate::connector::features::shared::actions::StreamActionBuilder;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GrpcStreamMode {
    Server,
    Client,
    Bidi,
}

impl fmt::Display for GrpcStreamMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let repr = match self {
            GrpcStreamMode::Server => "server_streaming",
            GrpcStreamMode::Client => "client_streaming",
            GrpcStreamMode::Bidi => "bidi_streaming",
        };
        f.write_str(repr)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum GrpcMethod<'a> {
    Plain(&'a str),
    Full {
        pkg: &'a str,
        service: &'a str,
        method: &'a str,
    },
}

impl GrpcMethod<'_> {
    pub fn parse(self) -> PathAndQuery {
        match self {
            GrpcMethod::Plain(s) => {
                if s.starts_with('/') {
                    s.try_into().unwrap()
                } else {
                    format!("/{}", s).try_into().unwrap()
                }
            }
            GrpcMethod::Full {
                pkg,
                service,
                method,
            } => {
                let path = format!("/{}.{}/{}", pkg, service, method);
                path.parse().unwrap()
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum GrpcCommand {
    Stream(GrpcStreamCommand),
    Unary(GrpcUnaryCall),
}

impl From<GrpcStreamCommand> for GrpcCommand {
    fn from(cmd: GrpcStreamCommand) -> Self {
        GrpcCommand::Stream(cmd)
    }
}

impl From<GrpcUnaryCall> for GrpcCommand {
    fn from(call: GrpcUnaryCall) -> Self {
        GrpcCommand::Unary(call)
    }
}

/// Control plane instructions for a persistent tonic streaming session.
#[derive(Debug, Clone)]
pub enum GrpcStreamCommand {
    /// Establish (or re-establish) the gRPC stream.
    Connect(GrpcStreamConnect),
    /// Push a payload into an active client/bidirectional stream.
    Send(Bytes),
    /// Close the outbound side of the stream (client or bidi).
    Finish,
    /// Abort the stream locally.
    Disconnect,
}

#[derive(Debug, Clone)]
pub struct GrpcStreamConnect {
    pub mode: GrpcStreamMode,
    pub method: PathAndQuery,
    pub initial_message: Option<Bytes>,
    pub metadata: MetadataMap,
}

impl GrpcStreamCommand {
    pub fn connect(method: GrpcMethod, mode: GrpcStreamMode) -> GrpcStreamConnectBuilder {
        GrpcStreamConnectBuilder::new(method, mode)
    }

    pub fn send_encoded(message: Bytes) -> Self {
        GrpcStreamCommand::Send(message)
    }

    pub fn send<T: Message>(message: T) -> Self {
        let mut buf = BytesMut::with_capacity(message.encoded_len());
        message.encode(&mut buf).unwrap();
        GrpcStreamCommand::Send(buf.freeze())
    }

    pub fn finish() -> Self {
        GrpcStreamCommand::Finish
    }

    pub fn disconnect() -> Self {
        GrpcStreamCommand::Disconnect
    }

    pub fn to_builder(self) -> StreamActionBuilder<GrpcCommand> {
        StreamActionBuilder::new(Some(self.into()))
    }
}

#[derive(Debug, Clone)]
pub struct GrpcStreamConnectBuilder {
    mode: GrpcStreamMode,
    method: PathAndQuery,
    initial_message: Option<Bytes>,
    metadata: MetadataMap,
}

impl GrpcStreamConnectBuilder {
    fn new(method: GrpcMethod, mode: GrpcStreamMode) -> Self {
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

    pub fn build(self) -> GrpcStreamCommand {
        GrpcStreamCommand::Connect(GrpcStreamConnect {
            mode: self.mode,
            method: self.method,
            initial_message: self.initial_message,
            metadata: self.metadata,
        })
    }

    pub fn to_builder(self) -> StreamActionBuilder<GrpcCommand> {
        self.build().to_builder()
    }
}

#[derive(Debug, Clone)]
pub struct GrpcUnaryCall {
    pub method: PathAndQuery,
    pub msg: BytesMut,
    pub meta: MetadataMap,
}

impl GrpcUnaryCall {
    pub fn with_bytes(method: GrpcMethod, msg: BytesMut) -> GrpcUnaryCall {
        Self {
            method: method.parse(),
            msg,
            meta: MetadataMap::new(),
        }
    }

    pub fn new<T: Message>(method: GrpcMethod, msg: T) -> GrpcUnaryCall {
        let mut bytes_mut = BytesMut::with_capacity(msg.encoded_len());
        msg.encode(&mut bytes_mut).unwrap();

        Self {
            method: method.parse(),
            msg: bytes_mut,
            meta: MetadataMap::new(),
        }
    }

    pub fn header_kv(&mut self, k: &str, v: &str) {
        if let (Ok(key), Ok(val)) = (k.parse::<AsciiMetadataKey>(), MetadataValue::try_from(v)) {
            self.meta.insert(key, val);
        }
    }

    pub fn to_builder(self) -> StreamActionBuilder<GrpcCommand> {
        StreamActionBuilder::new(Some(self.into()))
    }
}
