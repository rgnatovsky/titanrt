use std::fmt::Display;

use crate::{
    connector::features::{
        composite::{CompositeConnector, stream::event::StreamEventParsed},
        grpc::stream::GrpcCommand,
        http::stream::actions::HttpAction,
        shared::actions::StreamActionRaw,
        websocket::stream::WebSocketCommand,
    },
    prelude::BaseTx,
    utils::pipeline::{EncodableRequest, EncoderId},
};

#[derive(Debug, Clone)]
pub enum Selector<'a> {
    Default,
    Id(EncoderId),
    Key(&'a str),
}

impl<'a> Display for Selector<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Selector::Default => write!(f, "default"),
            Selector::Id(h) => write!(f, "{}", h),
            Selector::Key(k) => write!(f, "{}", k),
        }
    }
}

pub struct Request<'a, A: EncodableRequest> {
    pub payload: A,
    pub stream: &'a str,
    pub ctx: Option<&'a A::Ctx>,
}

impl<'a, A: EncodableRequest> Request<'a, A> {
    pub fn new(payload: A, stream: &'a str, ctx: Option<&'a A::Ctx>) -> Self {
        Self {
            payload,
            stream,
            ctx,
        }
    }
}

#[derive(Debug, Clone)]
pub enum CompositeAction {
    Http(StreamActionRaw<HttpAction>),
    Grpc(StreamActionRaw<GrpcCommand>),
    Ws(StreamActionRaw<WebSocketCommand>),
}

impl<E: StreamEventParsed, A: EncodableRequest> CompositeConnector<E, A> {
    pub fn send_http(
        &mut self,
        stream: impl AsRef<str>,
        action: StreamActionRaw<HttpAction>,
    ) -> anyhow::Result<()> {
        let sender = self.http_sender_mut(stream.as_ref());

        match sender {
            Some(sender) => sender.try_send(action)?,
            None => {
                return Err(anyhow::anyhow!(
                    "{} unknown stream for http action",
                    stream.as_ref()
                ));
            }
        }

        Ok(())
    }

    pub fn send_grpc(
        &mut self,
        stream: impl AsRef<str>,
        action: StreamActionRaw<GrpcCommand>,
    ) -> anyhow::Result<()> {
        let sender = self.grpc_sender_mut(stream.as_ref());

        match sender {
            Some(sender) => sender.try_send(action)?,
            None => {
                return Err(anyhow::anyhow!(
                    "{} unknown stream for grpc action",
                    stream.as_ref()
                ));
            }
        }

        Ok(())
    }

    pub fn send_websocket(
        &mut self,
        stream: impl AsRef<str>,
        action: StreamActionRaw<WebSocketCommand>,
    ) -> anyhow::Result<()> {
        let sender = self.ws_sender_mut(stream.as_ref());

        match sender {
            Some(sender) => sender.try_send(action)?,
            None => {
                return Err(anyhow::anyhow!(
                    "{} unknown stream for ws action",
                    stream.as_ref()
                ));
            }
        }

        Ok(())
    }

    pub fn send_to_stream(&mut self, stream: &str, action: CompositeAction) -> anyhow::Result<()> {
        match action {
            CompositeAction::Http(a) => self.send_http(stream, a),
            CompositeAction::Grpc(a) => self.send_grpc(stream, a),
            CompositeAction::Ws(a) => self.send_websocket(stream, a),
        }
    }

    pub fn send_encoded(&mut self, via: Selector<'_>, req: Request<A>) -> anyhow::Result<()> {
        let Request {
            payload,
            stream,

            ctx,
        } = req;

        let pipeline = match via {
            Selector::Default => Some(self.action_pipelines.get_default()),
            Selector::Id(h) => self.action_pipelines.get(h),
            Selector::Key(k) => self.action_pipelines.get_by_key(k),
        };

        let pipeline = match pipeline {
            Some(p) => p,
            None => return Err(anyhow::anyhow!("unknown pipeline route {:?}", via)),
        };

        let actions = match ctx {
            Some(c) => pipeline.execute_with_context(payload, c)?,
            None => pipeline.execute(payload)?,
        };

        for action in actions {
            self.send_to_stream(stream, action)?;
        }

        Ok(())
    }
}
