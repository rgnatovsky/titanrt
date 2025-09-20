use std::time::Duration;

use crate::connector::features::tonic::unary::RawCodec;
use bytes::Bytes;
use crossbeam::channel::Sender;
use futures::StreamExt;
use serde_json::Value;
use tokio::time::{self, };
use tonic::codegen::http::uri::PathAndQuery;
use tonic::{
    Request, Status,
    client::Grpc,
    metadata::{AsciiMetadataKey, MetadataMap, MetadataValue},
    transport::Channel,
};

/// Универсальное описание RPC-вызова (без конкретных protobuf-типов).
#[derive(Debug, Clone)]
pub enum GrpcMessage {
    /// Unary: один запрос → один ответ.
    Unary {
        method: PathAndQuery,
        message: Bytes,
    },
    /// Server streaming: один запрос → поток ответов.
    ServerStreaming {
        method: PathAndQuery,
        message: Bytes,
    },
    /// Client streaming: поток запросов → один ответ.
    ClientStreaming {
        method: PathAndQuery,
        messages: Vec<Bytes>,
    },
    /// BiDi streaming: двунаправленный поток.
    BidiStreaming {
        method: PathAndQuery,
        messages: Vec<Bytes>,
    },
}

impl GrpcMessage {}
#[derive(Debug, Clone)]
pub struct TonicActionSpec {
    pub grpc: GrpcMessage,     // какой тип вызова
    pub metadata: MetadataMap, // заголовки gRPC
}

impl TonicActionSpec {
    pub(crate) async fn execute(
        self,
        grpc: &mut Grpc<Channel>,
        req_id: Option<uuid::Uuid>,
        label: Option<&'static str>,
        payload: Option<Value>,
        timeout: Option<Duration>,
        res_tx: &Sender<StreamEvent<GrpcEventInner>>,
    ) -> Result<GrpcEvent, Status> {
        let ev = match self.grpc {
            GrpcMessage::Unary { method, message } => {
                let mut request = Request::new(message);

                *request.metadata_mut() = self.metadata;

                let resp = if let Some(t) = timeout {
                    let resp = match time::timeout(t, grpc.unary(request, method, RawCodec)).await {
                        Ok(r) => r,
                        Err(_) => return Err(Status::deadline_exceeded("client timeout")),
                    };
                    resp
                } else {
                    grpc.unary(request, method, RawCodec).await
                };

                let resp = match resp {
                    Ok(r) => r,
                    Err(e) => return Err(e),
                };

                GrpcEvent::from_ok_unary(resp, req_id, label, payload.clone())
            }

            GrpcMessage::ServerStreaming { method, message } => {
                let mut request = Request::new(message);
                *request.metadata_mut() = self.metadata;

                let mut stream = grpc
                    .server_streaming(request, method, RawCodec)
                    .await?
                    .into_inner();

                while let Some(item) = stream.next().await {
                    match item {
                        Ok(item) => {
                            let _ = res_tx.send(GrpcEvent::from_ok_stream_item(
                                item,
                                req_id,
                                label,
                                payload.clone(),
                            ));
                        }
                        Err(e) => return Err(e),
                    }
                }

                return Err(Status::resource_exhausted("server disconnected"));
            }
            _ => unimplemented!(),
            // GrpcMessage::ClientStreaming { method, messages } => {
            //     client_stream_call(&mut grpc, &method, messages, md)
            //         .await
            //         .map(|(b, meta)| {
            //             GrpcEvent::from_ok_unary(b, meta, req_id, label, payload.clone())
            //         })
            // }
            // GrpcMessage::BidiStreaming { method, messages } => {
            //     let rtx = res_tx.clone();
            //     let payload_bidi = payload.clone();
            //     bidi_stream_call(&mut grpc, &method, messages, md, move |item| {
            //         let _ = rtx.send(GrpcEvent::from_ok_stream_item(
            //             item,
            //             req_id,
            //             label,
            //             payload_bidi.clone(),
            //         ));
            //     })
            //     .await
            //     .map(|_| {
            //         GrpcEvent::from_ok_unary(
            //             bytes::Bytes::new(),
            //             MetadataMap::new(),
            //             req_id,
            //             label,
            //             payload.clone(),
            //         )
            //     })
            // }
        };

        Ok(ev)
    }
    pub fn builder(call: GrpcMessage) -> TonicActionBuilder {
        TonicActionBuilder::new(call)
    }

    /// Хелперы-конструкторы
    pub fn unary(method: &str, msg: Bytes) -> TonicActionBuilder {
        Self::builder(GrpcMessage::Unary {
            method: method.parse().unwrap(),
            message: msg,
        })
    }
    pub fn server_streaming(method: &str, msg: Bytes) -> TonicActionBuilder {
        Self::builder(GrpcMessage::ServerStreaming {
            method: method.parse().unwrap(),
            message: msg,
        })
    }
    pub fn client_streaming(method: &str, messages: Vec<Bytes>) -> TonicActionBuilder {
        Self::builder(GrpcMessage::ClientStreaming {
            method: method.parse().unwrap(),
            messages,
        })
    }
    pub fn bidi_streaming(method: &str, messages: Vec<Bytes>) -> TonicActionBuilder {
        Self::builder(GrpcMessage::BidiStreaming {
            method: method.parse().unwrap(),
            messages,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TonicActionBuilder {
    grpc: GrpcMessage,
    metadata: MetadataMap,
}

impl TonicActionBuilder {
    pub fn new(call: GrpcMessage) -> Self {
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

    pub fn build(self) -> TonicActionSpec {
        TonicActionSpec {
            grpc: self.grpc,
            metadata: self.metadata,
        }
    }
}


// // client & bidi streaming — подаём заранее подготовленные фреймы (Vec<Bytes>)
// async fn client_stream_call(
//     grpc: &mut Grpc<Channel>,
//     method: &str,
//     msgs: Vec<bytes::Bytes>,
//     md: MetadataMap,
// ) -> Result<(bytes::Bytes, MetadataMap), Status> {
//     let path: PathAndQuery = method
//         .parse()
//         .map_err(|_| Status::invalid_argument("invalid method path"))?;
//     let stream = stream::iter(msgs.into_iter().map(Ok::<_, Status>));
//     let mut request = Request::new(stream);
//     *request.metadata_mut() = md;
//     let resp = grpc
//         .client_streaming(request, path, RawStreamingCodec)
//         .await?;
//     let meta = resp.metadata().clone();
//     Ok((resp.into_inner(), meta))
// }

// async fn bidi_stream_call(
//     grpc: &mut Grpc<Channel>,
//     method: &str,
//     msgs: Vec<bytes::Bytes>,
//     md: MetadataMap,
//     mut on_item: impl FnMut(bytes::Bytes) -> (),
// ) -> Result<(), Status> {
//     let path: PathAndQuery = method
//         .parse()
//         .map_err(|_| Status::invalid_argument("invalid method path"))?;
//     let stream = stream::iter(msgs.into_iter().map(Ok::<_, Status>));
//     let mut request = Request::new(stream);
//     *request.metadata_mut() = md;
//     let resp = grpc.streaming(request, path, RawStreamingCodec).await?;
//     let mut s = resp.into_inner();
//     while let Some(item) = s.next().await {
//         on_item(item?);
//     }
//     Ok(())
// }


// helper для server streaming
// async fn server_stream_call(
//     grpc: &mut Grpc<Channel>,
//     method: &str,
//     req_body: bytes::Bytes,
//     md: MetadataMap,
//     mut on_item: impl FnMut(bytes::Bytes) -> (),
// ) -> Result<(), Status> {
//     let path: PathAndQuery = method
//         .parse()
//         .map_err(|_| Status::invalid_argument("invalid method path"))?;
//     let mut request = Request::new(req_body);
//     *request.metadata_mut() = md;
//     let mut stream = grpc
//         .server_streaming(request, path, RawCodec)
//         .await?
//         .into_inner();
//     while let Some(item) = stream.next().await {
//         let bytes = item?;
//         on_item(bytes);
//     }
//     Ok(())
// }