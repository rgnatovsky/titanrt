use crate::connector::Stream;
use crate::connector::features::composite::stream::event::{StreamEvent, StreamEventParsed};
use crate::connector::features::grpc::stream::GrpcCommand;
use crate::connector::features::http::stream::actions::HttpAction;
use crate::connector::features::shared::actions::StreamActionRaw;
use crate::connector::features::websocket::stream::WebSocketCommand;
use crate::io::mpmc::MpmcReceiver;
use crate::io::ringbuffer::RingSender;
use crate::utils::StateCell;

pub enum StreamWrapper<E>
where
    E: StreamEventParsed,
{
    Http {
        stream: Stream<
            RingSender<StreamActionRaw<HttpAction>>,
            Option<MpmcReceiver<StreamEvent<E>>>,
            E::HttpState,
        >,
    },
    Grpc {
        stream: Stream<
            RingSender<StreamActionRaw<GrpcCommand>>,
            Option<MpmcReceiver<StreamEvent<E>>>,
            E::GrpcState,
        >,
    },
    Ws {
        stream: Stream<
            RingSender<StreamActionRaw<WebSocketCommand>>,
            Option<MpmcReceiver<StreamEvent<E>>>,
            E::WsState,
        >,
    },
    _Phantom(std::marker::PhantomData<E>),
}

impl<E> StreamWrapper<E>
where
    E: StreamEventParsed,
{
    pub fn get_http(
        &mut self,
    ) -> Option<
        &mut Stream<
            RingSender<StreamActionRaw<HttpAction>>,
            Option<MpmcReceiver<StreamEvent<E>>>,
            E::HttpState,
        >,
    > {
        match self {
            StreamWrapper::Http { stream } => Some(stream),
            _ => None,
        }
    }

    pub fn get_grpc(
        &mut self,
    ) -> Option<
        &mut Stream<
            RingSender<StreamActionRaw<GrpcCommand>>,
            Option<MpmcReceiver<StreamEvent<E>>>,
            E::GrpcState,
        >,
    > {
        match self {
            StreamWrapper::Grpc { stream } => Some(stream),
            _ => None,
        }
    }

    pub fn get_ws(
        &mut self,
    ) -> Option<
        &mut Stream<
            RingSender<StreamActionRaw<WebSocketCommand>>,
            Option<MpmcReceiver<StreamEvent<E>>>,
            E::WsState,
        >,
    > {
        match self {
            StreamWrapper::Ws { stream } => Some(stream),
            _ => None,
        }
    }

    pub fn cancel(&mut self) {
        match self {
            StreamWrapper::Http { stream } => stream.cancel(),
            StreamWrapper::Grpc { stream } => stream.cancel(),
            StreamWrapper::Ws { stream } => stream.cancel(),
            StreamWrapper::_Phantom(_) => {}
        }
    }

    pub fn is_alive(&self) -> bool {
        match self {
            StreamWrapper::Http { stream } => stream.is_healthy() && !stream.is_cancelled(),
            StreamWrapper::Grpc { stream } => stream.is_healthy() && !stream.is_cancelled(),
            StreamWrapper::Ws { stream } => stream.is_healthy() && !stream.is_cancelled(),
            StreamWrapper::_Phantom(_) => false,
        }
    }

    pub fn http_state(&self) -> Option<&StateCell<E::HttpState>> {
        match self {
            StreamWrapper::Http { stream } => Some(stream.state()),
            _ => None,
        }
    }
    pub fn grpc_state(&self) -> Option<&StateCell<E::GrpcState>> {
        match self {
            StreamWrapper::Grpc { stream } => Some(stream.state()),
            _ => None,
        }
    }
    pub fn ws_state(&self) -> Option<&StateCell<E::WsState>> {
        match self {
            StreamWrapper::Ws { stream } => Some(stream.state()),
            _ => None,
        }
    }
}
