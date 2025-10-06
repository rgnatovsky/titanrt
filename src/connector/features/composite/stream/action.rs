use crate::connector::features::{
    grpc::stream::GrpcCommand, http::stream::actions::HttpAction, shared::actions::StreamActionRaw,
    websocket::stream::WebSocketCommand,
};

#[derive(Debug)]
pub enum CompositeAction {
    Http(StreamActionRaw<HttpAction>),
    Grpc(StreamActionRaw<GrpcCommand>),
    Ws(StreamActionRaw<WebSocketCommand>),
}
