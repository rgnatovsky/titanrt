use crate::connector::features::shared::actions::StreamActionBuilder;
use crate::connector::features::tonic::codec::RawCodec;
use crate::connector::features::tonic::unary::UnaryEvent;
use bytes::BytesMut;
use prost::Message;
use std::time::Duration;
use tokio::time::{self};
use tonic::codegen::http::uri::PathAndQuery;
use tonic::{
    Request, Status,
    client::Grpc,
    metadata::{AsciiMetadataKey, MetadataMap, MetadataValue},
    transport::Channel,
};

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
pub struct UnaryAction {
    pub method: PathAndQuery,
    pub msg: BytesMut,
    pub meta: MetadataMap,
}

impl UnaryAction {
    pub(crate) async fn execute(
        self,
        grpc: &mut Grpc<Channel>,
        timeout: Option<Duration>,
    ) -> Result<UnaryEvent, Status> {
        let mut request = Request::new(self.msg.freeze());

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

    pub fn with_bytes(method: GrpcMethod, msg: BytesMut) -> UnaryAction {
        Self {
            method: method.parse(),
            msg,
            meta: MetadataMap::new(),
        }
    }

    pub fn new<T: Message>(method: GrpcMethod, msg: T) -> UnaryAction {
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

    pub fn to_builder(self) -> StreamActionBuilder<UnaryAction> {
        StreamActionBuilder::new(Some(self))
    }
}
