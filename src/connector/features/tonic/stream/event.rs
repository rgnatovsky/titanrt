use anyhow::anyhow;
use bytes::Bytes;
use tonic::{metadata::MetadataMap, Code, Status};

use crate::connector::features::shared::events::StreamEventInner;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrpcEventKind {
    StreamConnected,
    StreamDisconnected,
    StreamItem,
    UnaryResponse,
    BadCommand
}

#[derive(Debug, Clone)]
pub struct GrpcEvent {
    kind: GrpcEventKind,
    code: Code,
    err_msg: Option<String>,
    metadata: MetadataMap,
    body: Option<Bytes>,
}

impl GrpcEvent {
    pub fn from_ok() -> Self {
        Self {
            kind: GrpcEventKind::StreamConnected,
            code: Code::Ok,
            err_msg: None,
            metadata: MetadataMap::new(),
            body: None,
        }
    }

    pub fn from_ok_unary(resp: tonic::Response<Bytes>) -> Self {
        let (metadata, body, _trailing) = resp.into_parts();
        Self {
            kind: GrpcEventKind::UnaryResponse,
            code: Code::Ok,
            err_msg: None,
            metadata,
            body: Some(body),
        }
    }

    pub fn from_ok_stream_item(body: Bytes) -> Self {
        Self {
            kind: GrpcEventKind::StreamItem,
            code: Code::Ok,
            err_msg: None,
            metadata: MetadataMap::new(),
            body: Some(body),
        }
    }

    pub fn from_ok_stream_close(metadata: MetadataMap) -> Self {
        let status = Status::aborted("grpc streaming aborted".to_string());
        Self {
            kind: GrpcEventKind::StreamDisconnected,
            code: status.code(),
            err_msg: Some(status.message().to_string()),
            metadata,
            body: None,
        }
    }

    pub fn from_status(kind: GrpcEventKind, st: Status) -> Self {
        Self {
            kind,
            code: st.code(),
            err_msg: Some(st.message().to_string()),
            metadata: st.metadata().clone(),
            body: None,
        }
    }

    pub fn kind(&self) -> GrpcEventKind {
        self.kind
    }

    pub fn metadata(&self) -> &MetadataMap {
        &self.metadata
    }

    pub fn decode_as<T: prost::Message + Default>(&self) -> anyhow::Result<T> {
        if let Some(ref b) = self.body {
            T::decode(b.as_ref()).map_err(|e| anyhow!("Prost decode error: {e}"))
        } else {
            Err(anyhow!("No body to decode"))
        }
    }
}

impl StreamEventInner for GrpcEvent {
    type Body = Bytes;
    type Err = String;
    type Code = Code;

    fn status(&self) -> Option<&Self::Code> {
        Some(&self.code)
    }

    fn is_ok(&self) -> bool {
        self.code == Code::Ok
    }

    fn error(&self) -> Option<&Self::Err> {
        self.err_msg.as_ref()
    }

    fn body(&self) -> Option<&Self::Body> {
        self.body.as_ref()
    }

    fn into_body(self) -> Option<Self::Body> {
        self.body
    }
}
