use anyhow::{Context, anyhow};

use crate::{
    connector::{
        BaseConnector, EventTxType,
        features::composite::{
            CompositeConnector,
            stream::{
                StreamKind, StreamSlot, StreamWrapper,
                event::{StreamEvent, StreamEventParsed},
                hooks::{grpc_hook, http_hook, ws_hook},
            },
        },
    },
    io::mpmc::MpmcSender,
};

impl<E: StreamEventParsed> CompositeConnector<E> {
    pub fn spawn_stream(
        &self,
        slot: &StreamSlot<E>,
        event_tx: &MpmcSender<StreamEvent<E>>,
    ) -> anyhow::Result<StreamWrapper<E>>
    where
        E: StreamEventParsed,
    {
        match slot.spec.kind {
            StreamKind::Http => {
                let descriptor = slot
                    .spec
                    .maybe_http(&slot.ctx)
                    .context("failed to build HTTP descriptor")?;

                let stream = self
                    .with_http(|conn| {
                        conn.spawn_stream(
                            descriptor,
                            EventTxType::External(event_tx.clone()),
                            http_hook,
                        )
                    })?
                    .ok_or_else(|| anyhow!("reqwest connector is not configured"))?;

                Ok(StreamWrapper::Http { stream })
            }
            StreamKind::Grpc => {
                let descriptor = slot
                    .spec
                    .maybe_grpc(&slot.ctx)
                    .context("failed to build gRPC descriptor")?;

                let stream = self
                    .with_grpc(|conn| {
                        conn.spawn_stream(
                            descriptor,
                            EventTxType::External(event_tx.clone()),
                            grpc_hook,
                        )
                    })?
                    .ok_or_else(|| anyhow!("tonic connector is not configured"))?;

                Ok(StreamWrapper::Grpc { stream })
            }
            StreamKind::Ws => {
                let descriptor = slot
                    .spec
                    .maybe_ws(&slot.ctx)
                    .context("failed to build WebSocket descriptor")?;

                let stream = self
                    .with_websocket(|conn| {
                        conn.spawn_stream(
                            descriptor,
                            EventTxType::External(event_tx.clone()),
                            ws_hook,
                        )
                    })?
                    .ok_or_else(|| anyhow!("websocket connector is not configured"))?;

                Ok(StreamWrapper::Ws { stream })
            }
        }
    }
}
