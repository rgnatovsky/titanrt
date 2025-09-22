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

#[derive(Debug, Clone)]
pub struct UnaryAction {
    pub method: PathAndQuery,
    pub msg: Bytes,
    pub meta: MetadataMap,
}

impl UnaryAction {
    pub(crate) async fn execute(
        self,
        grpc: &mut Grpc<Channel>,
        timeout: Option<Duration>,
    ) -> Result<UnaryEvent, Status> {
        let mut request = Request::new(self.msg);

        *request.metadata_mut() = self.meta;

        let resp = if let Some(t) = timeout {
            let resp = match time::timeout(t, grpc.unary(request, self.method, RawCodec)).await {
                Ok(r) => r,
                Err(_) => return Err(Status::deadline_exceeded("client timeout")),
            };
            resp
        } else {
            grpc.unary(request, self.method, RawCodec).await
        };

        let resp = match resp {
            Ok(r) => r,
            Err(e) => return Err(e),
        };

        let ev = UnaryEvent::from_ok_unary(resp);

        Ok(ev)
    }
    
    pub fn new(method: &str, msg: Bytes) -> anyhow::Result<UnaryAction> {
        Ok(Self {
            method: method.try_into()?,
            msg,
            meta: MetadataMap::new(),
        })
    }

    pub fn header_kv(&mut self, k: &str, v: &str) {
        if let (Ok(key), Ok(val)) = (k.parse::<AsciiMetadataKey>(), MetadataValue::try_from(v)) {
            self.meta.insert(key, val);
        }
    }
}
