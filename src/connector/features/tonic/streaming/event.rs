use bytes::Bytes;
use tonic::{Code, Status, metadata::MetadataMap};

use crate::connector::features::shared::events::StreamEventInner;

#[derive(Debug, Clone)]
pub struct StreamingEvent {
    code: Option<Code>,
    err_msg: Option<String>,
    metadata: MetadataMap,
    body: Option<Bytes>,
    is_stream_item: bool,
}

impl StreamingEvent {
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

    pub fn from_ok_stream_close(metadata: MetadataMap) -> Self {
        Self {
            code: None,
            err_msg: None,
            metadata,
            body: None,
            is_stream_item: false,
        }
    }

    pub fn from_status(st: Status) -> Self {
        Self {
            code: Some(st.code()),
            err_msg: Some(st.message().to_string()),
            metadata: st.metadata().clone(),
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
}

impl StreamEventInner for StreamingEvent {
    type Body = Bytes;
    type Err = String;
    type Code = Code;

    fn status(&self) -> Option<&Self::Code> {
        self.code.as_ref()
    }

    fn is_ok(&self) -> bool {
        self.code.is_none()
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
