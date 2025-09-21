use crate::connector::features::tonic::codec::RawCodec;
use crate::connector::features::tonic::unary::UnaryEvent;
use bytes::Bytes;
use std::time::Duration;
use tokio::time::{self};
use tonic::codegen::http::uri::PathAndQuery;
use tonic::{
    Request, Status,
    client::Grpc,
    metadata::{AsciiMetadataKey, MetadataMap, MetadataValue},
    transport::Channel,
};

/// Универсальное описание RPC-вызова (без конкретных protobuf-типов).
#[derive(Debug, Clone)]
pub struct UnaryMessage {
    method: PathAndQuery,
    message: Bytes,
}

#[derive(Debug, Clone)]
pub struct UnaryAction {
    pub msg: UnaryMessage, // какой тип вызова
    pub meta: MetadataMap, // заголовки gRPC
}

impl UnaryAction {
    pub(crate) async fn execute(
        self,
        grpc: &mut Grpc<Channel>,
        timeout: Option<Duration>,
    ) -> Result<UnaryEvent, Status> {
        let mut request = Request::new(self.msg.message);

        *request.metadata_mut() = self.meta;

        let resp = if let Some(t) = timeout {
            let resp = match time::timeout(t, grpc.unary(request, self.msg.method, RawCodec)).await
            {
                Ok(r) => r,
                Err(_) => return Err(Status::deadline_exceeded("client timeout")),
            };
            resp
        } else {
            grpc.unary(request, self.msg.method, RawCodec).await
        };

        let resp = match resp {
            Ok(r) => r,
            Err(e) => return Err(e),
        };

        let ev = UnaryEvent::from_ok_unary(resp);

        Ok(ev)
    }
    pub fn builder(call: UnaryMessage) -> UnaryActionBuilder {
        UnaryActionBuilder::new(call)
    }
}

#[derive(Debug, Clone)]
pub struct UnaryActionBuilder {
    grpc: UnaryMessage,
    metadata: MetadataMap,
}

impl UnaryActionBuilder {
    pub fn new(call: UnaryMessage) -> Self {
        Self {
            grpc: call,
            metadata: MetadataMap::new(),
        }
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

    pub fn build(self) -> UnaryAction {
        UnaryAction {
            msg: self.grpc,
            meta: self.metadata,
        }
    }
}
