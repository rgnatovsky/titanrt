use crate::{
    connector::features::{
        composite::{CompositeConnector, stream::event::StreamEventParsed},
        grpc::stream::GrpcCommand,
        http::stream::actions::HttpAction,
        shared::actions::StreamActionRaw,
        websocket::stream::WebSocketCommand,
    },
    prelude::BaseTx,
    utils::pipeline::{EncodableAction, PipelineHandle},
};

pub struct PipelineCommand<'a, A: EncodableAction> {
    pub payload: A,
    pub stream: &'a str,
    pub handle: Option<PipelineHandle>,
    pub ctx: Option<&'a A::Ctx>,
}

impl<'a, A: EncodableAction> PipelineCommand<'a, A> {
    pub fn new(payload: A, stream: &'a str) -> Self {
        Self {
            payload,
            stream,
            handle: None,
            ctx: None,
        }
    }

    pub fn with_stream(mut self, stream: &'a str) -> Self {
        self.stream = stream;
        self
    }

    pub fn with_handle(mut self, handle: PipelineHandle) -> Self {
        self.handle = Some(handle);
        self
    }

    pub fn without_routing_key(mut self) -> Self {
        self.handle = None;
        self
    }

    pub fn with_ctx(mut self, ctx: &'a A::Ctx) -> Self {
        self.ctx = Some(ctx);
        self
    }

    pub fn without_ctx(mut self) -> Self {
        self.ctx = None;
        self
    }

    pub fn with_payload(mut self, send_case: A) -> Self {
        self.payload = send_case;
        self
    }
}
#[derive(Debug, Clone)]
pub enum CompositeAction {
    Http(StreamActionRaw<HttpAction>),
    Grpc(StreamActionRaw<GrpcCommand>),
    Ws(StreamActionRaw<WebSocketCommand>),
}

impl<E: StreamEventParsed, A: EncodableAction> CompositeConnector<E, A> {
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

    pub fn send_via_pipeline(&mut self, cmd: PipelineCommand<A>) -> anyhow::Result<()> {
        let PipelineCommand {
            payload,
            stream,
            handle,
            ctx,
        } = cmd;

        let pipeline = match handle {
            Some(key) => match self.action_pipelines.get(key) {
                Some(p) => p,
                None => return Err(anyhow::anyhow!("{} unknown pipeline", key)),
            },
            None => self.action_pipelines.get_default(),
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
