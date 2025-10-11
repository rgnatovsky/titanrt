use anyhow::{Context, anyhow};

use crate::{
    connector::{
        BaseConnector, EventTxType,
        features::composite::{
            CompositeConnector,
            stream::{
                StreamKind, StreamSpec, StreamWrapper,
                event::{StreamEventContext, StreamEventParsed},
                hooks::{grpc_hook, http_hook, ws_hook},
            },
        },
    },
    utils::pipeline::EncodableAction,
};

impl<E: StreamEventParsed, A: EncodableAction> CompositeConnector<E, A> {
    pub fn spawn_stream(
        &self,
        spec: &StreamSpec,
        ctx: &StreamEventContext<E>,
    ) -> anyhow::Result<StreamWrapper<E>>
    where
        E: StreamEventParsed,
    {
        match spec.kind {
            StreamKind::Http => {
                let descriptor = spec
                    .maybe_http(&ctx)
                    .context("failed to build HTTP descriptor")?;

                let stream = self
                    .with_http(|conn| {
                        conn.spawn_stream(
                            descriptor,
                            EventTxType::External(self.event_tx.clone()),
                            http_hook,
                        )
                    })?
                    .ok_or_else(|| anyhow!("reqwest connector is not configured"))?;

                Ok(StreamWrapper::Http { stream })
            }
            StreamKind::Grpc => {
                let descriptor = spec
                    .maybe_grpc(&ctx)
                    .context("failed to build gRPC descriptor")?;

                let stream = self
                    .with_grpc(|conn| {
                        conn.spawn_stream(
                            descriptor,
                            EventTxType::External(self.event_tx.clone()),
                            grpc_hook,
                        )
                    })?
                    .ok_or_else(|| anyhow!("tonic connector is not configured"))?;

                Ok(StreamWrapper::Grpc { stream })
            }
            StreamKind::Ws => {
                let descriptor = spec
                    .maybe_ws(&ctx)
                    .context("failed to build WebSocket descriptor")?;

                let stream = self
                    .with_websocket(|conn| {
                        conn.spawn_stream(
                            descriptor,
                            EventTxType::External(self.event_tx.clone()),
                            ws_hook,
                        )
                    })?
                    .ok_or_else(|| anyhow!("websocket connector is not configured"))?;

                Ok(StreamWrapper::Ws { stream })
            }
        }
    }
}
