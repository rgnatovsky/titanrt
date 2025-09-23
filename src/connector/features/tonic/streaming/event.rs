use anyhow::anyhow;
use bytes::Bytes;
use tonic::{Code, Status, metadata::MetadataMap};

use crate::connector::features::shared::events::StreamEventInner;

#[derive(Debug, Clone)]
pub struct StreamingEvent {
    code: Code,
    err_msg: Option<String>,
    metadata: MetadataMap,
    body: Option<Bytes>,
    is_stream_item: bool,
}

impl StreamingEvent {
    pub fn from_ok() -> Self {
        Self {
            code: Code::Ok,
            err_msg: None,
            metadata: MetadataMap::new(),
            body: None,
            is_stream_item: false,
        }
    }

    pub fn from_ok_unary(resp: tonic::Response<Bytes>) -> Self {
        let (metadata, body, _trailing) = resp.into_parts();
        Self {
            code: Code::Ok,
            err_msg: None,
            metadata,
            body: Some(body),
            is_stream_item: false,
        }
    }

    pub fn from_ok_stream_item(body: Bytes) -> Self {
        Self {
            code: Code::Ok,
            err_msg: None,
            metadata: MetadataMap::new(),
            body: Some(body),
            is_stream_item: true,
        }
    }

    pub fn from_ok_stream_close(metadata: MetadataMap) -> Self {
        let status = Status::aborted("grpc streaming aborted".to_string());
        Self {
            code: status.code(),
            err_msg: Some(status.message().to_string()),
            metadata,
            body: None,
            is_stream_item: false,
        }
    }

    pub fn from_status(st: Status) -> Self {
        Self {
            code: st.code(),
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

    pub fn decode_as<T: prost::Message + Default>(&self) -> anyhow::Result<T> {
        if let Some(ref b) = self.body {
            T::decode(b.as_ref()).map_err(|e| anyhow!("Prost decode error: {e}"))
        } else {
            Err(anyhow!("No body to decode"))
        }
    }
}

impl StreamEventInner for StreamingEvent {
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
