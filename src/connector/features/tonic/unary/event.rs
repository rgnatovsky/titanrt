use anyhow::anyhow;
use bytes::Bytes;
use tonic::{Code, Status, metadata::MetadataMap};

use crate::connector::features::shared::events::StreamEventInner;

/// Внутреннее представление gRPC-ивента, теперь совместимо с StreamEventInner.
#[derive(Debug, Clone)]
pub struct UnaryEvent {
    code: Code,
    err_msg: Option<String>,
    pub metadata: MetadataMap,
    body: Option<Bytes>,
}

impl UnaryEvent {
    pub(crate) fn from_ok_unary(resp: tonic::Response<Bytes>) -> Self {
        let (metadata, body, _trailing) = resp.into_parts();
        Self {
            code: Code::Ok,
            err_msg: None,
            metadata,
            body: Some(body),
        }
    }

    pub(crate) fn from_status(st: Status) -> Self {
        Self {
            code: st.code(),
            err_msg: Some(st.message().to_string()),
            metadata: MetadataMap::new(),
            body: None,
        }
    }

    pub fn decode_as<T: prost::Message + Default>(&self) -> anyhow::Result<T> {
        if let Some(ref b) = self.body {
            T::decode(b.as_ref()).map_err(|e| anyhow!("Prost decode error: {e}"))
        } else {
            Err(anyhow!("No body to decode"))
        }
    }
}

impl StreamEventInner for UnaryEvent {
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
