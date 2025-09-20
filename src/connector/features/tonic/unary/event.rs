use bytes::Bytes;
use tonic::{Code, Status, metadata::MetadataMap};

use crate::connector::features::shared::events::StreamEventInner;

/// Внутреннее представление gRPC-ивента, теперь совместимо с StreamEventInner.
#[derive(Debug, Clone)]
pub struct UnaryEvent {
    code: Option<Code>,
    err_msg: Option<String>,
    metadata: MetadataMap,
    body: Option<Bytes>,
    is_stream_item: bool,
}

impl UnaryEvent {
    pub fn from_ok_unary(resp: tonic::Response<Bytes>) -> Self {
        let (metadata, body, _trailing) = resp.into_parts();
        Self {
            code: None,
            err_msg: None,
            metadata,
            body: Some(body),
            is_stream_item: false,
        }
    }

    pub fn from_ok_stream_item(body: Bytes) -> Self {
        Self {
            code: None,
            err_msg: None,
            metadata: MetadataMap::new(),
            body: Some(body),
            is_stream_item: true,
        }
    }

    pub fn from_status(st: Status) -> Self {
        Self {
            code: Some(st.code()),
            err_msg: Some(st.message().to_string()),
            metadata: MetadataMap::new(),
            body: None,
            is_stream_item: false,
        }
    }

    pub fn metadata(&self) -> &MetadataMap {
        &self.metadata
    }

    pub fn is_stream_item(&self) -> bool {
        self.is_stream_item
    }

    pub fn body_bytes(&self) -> Option<&Bytes> {
        self.body.as_ref()
    }

    pub fn is_ok(&self) -> bool {
        self.code.is_none()
    }

    pub fn maybe_error_msg(&self) -> Option<String> {
        self.code.map(|c| {
            format!(
                "gRPC status: {:?}, {}",
                c,
                self.err_msg.clone().unwrap_or_default()
            )
        })
    }
}

impl StreamEventInner for UnaryEvent {
    type Body = Bytes;
    type Err = String;
    type Code = Code;

    fn status(&self) -> Option<&Self::Code> {
        self.code.as_ref()
    }

    fn error(&self) -> Option<&Self::Err> {
        self.err_msg.as_ref()
    }

    fn body(&self) -> Option<&Self::Body> {
        self.body.as_ref()
    }
}
