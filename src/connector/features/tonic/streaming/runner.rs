use crate::connector::errors::{StreamError, StreamResult};
use crate::connector::features::shared::actions::StreamAction;
use crate::connector::features::shared::clients_map::ClientsMap;
use crate::connector::features::shared::events::StreamEvent;
use crate::connector::features::shared::rate_limiter::RateLimitManager;
use crate::connector::features::tonic::client::{TonicChannelSpec, TonicClient};
use crate::connector::features::tonic::connector::TonicConnector;
use crate::connector::features::tonic::streaming::StreamingMode;
use crate::connector::features::tonic::streaming::actions::StreamingActionInner;
use crate::connector::features::tonic::streaming::descriptor::TonicStreamingDescriptor;
use crate::connector::features::tonic::streaming::event::StreamingEvent;
use crate::connector::features::tonic::streaming::handle_bidi::start_bidi_stream;
use crate::connector::features::tonic::streaming::handle_client::start_client_stream;
use crate::connector::features::tonic::streaming::handle_server::start_server_stream;
use crate::connector::features::tonic::streaming::utils::{
    ActiveStream, StreamContext, StreamLifecycle, emit_event,
};
use crate::connector::{Hook, HookArgs, IntoHook, RuntimeCtx, StreamRunner, StreamSpawner};
use crate::io::ringbuffer::RingSender;
use crate::prelude::{BaseRx, TxPairExt};
use crate::utils::{Reducer, StateMarker};

use anyhow::anyhow;
use crossbeam::channel::unbounded;
use std::collections::HashMap;
use std::rc::Rc;

use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::Mutex;
use tokio::task::LocalSet;
use tokio::time::sleep;
use tonic::Status;

impl<E, R, S> StreamSpawner<TonicStreamingDescriptor, E, R, S> for TonicConnector
where
    E: TxPairExt,
    S: StateMarker,
    R: Reducer,
{
}

impl<E, R, S> StreamRunner<TonicStreamingDescriptor, E, R, S> for TonicConnector
where
    E: TxPairExt,
    S: StateMarker,
    R: Reducer,
{
    type Config = ClientsMap<TonicClient, TonicChannelSpec>;
    type ActionTx = RingSender<StreamAction<StreamingActionInner>>;
    type RawEvent = StreamEvent<StreamingEvent>;
    type HookResult = ();

    fn build_config(&mut self, _desc: &TonicStreamingDescriptor) -> anyhow::Result<Self::Config> {
        Ok(self.clients_map())
    }

    fn run<H>(
        mut ctx: RuntimeCtx<
            ClientsMap<TonicClient, TonicChannelSpec>,
            TonicStreamingDescriptor,
            RingSender<StreamAction<StreamingActionInner>>,
            E,
            R,
            S,
        >,
        hook: H,
    ) -> StreamResult<()>
    where
        H: IntoHook<StreamEvent<StreamingEvent>, E, R, S, TonicStreamingDescriptor, ()>,
        E: TxPairExt,
        S: StateMarker,
    {
        let mut hook = hook.into_hook();
        let wait_async_tasks = Duration::from_micros(ctx.desc.wait_async_tasks_us);
        let rl_manager = Rc::new(Mutex::new(RateLimitManager::new(
            ctx.desc.rate_limits.clone(),
        )));

        let rt = Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .map_err(|e| StreamError::Unknown(anyhow!("Tokio Runtime error: {e}")))?;

        let local = LocalSet::new();
        let (res_tx, res_rx) = unbounded();
        let (lifecycle_tx, lifecycle_rx) = unbounded();

        let mut active_streams: HashMap<usize, ActiveStream> = HashMap::new();

        ctx.health.up();

        loop {
            if ctx.cancel.is_cancelled() {
                ctx.health.down();
                for (_, stream) in active_streams.drain() {
                    stream.stop();
                }
                break Err(StreamError::Cancelled);
            }

            while let Ok(StreamLifecycle::Closed { conn_id }) = lifecycle_rx.try_recv() {
                active_streams.remove(&conn_id);
            }

            while let Ok(action) = ctx.action_rx.try_recv() {
                let (inner, conn_id, req_id, label, timeout, rl_ctx, rl_weight, payload) =
                    action.into_parts();

                let inner = match inner {
                    Some(inner) => inner,
                    None => continue,
                };

                let conn_id = match conn_id {
                    Some(id) => id,
                    None => {
                        emit_event(
                            &res_tx,
                            None,
                            req_id,
                            None,
                            payload.as_ref(),
                            StreamingEvent::from_status(Status::invalid_argument(
                                "stream action missing conn_id",
                            )),
                        );
                        continue;
                    }
                };

                let label = label.map(|cow| cow.into_owned());
                let payload = payload;

                match inner {
                    StreamingActionInner::Connect(connect) => {
                        if let Some(prev) = active_streams.remove(&conn_id) {
                            prev.stop();
                        }

                        let Some(client) = ctx.cfg.get(&conn_id) else {
                            emit_event(
                                &res_tx,
                                Some(conn_id),
                                req_id,
                                label.as_ref(),
                                payload.as_ref(),
                                StreamingEvent::from_status(Status::unavailable(
                                    "unknown tonic connection id",
                                )),
                            );
                            continue;
                        };

                        let channel = client.channel();
                        let context = StreamContext {
                            req_id,
                            label: label.clone(),
                            payload: payload.clone(),
                        };

                        let rl_manager = if rl_ctx.is_some() {
                            Some(rl_manager.clone())
                        } else {
                            None
                        };

                        let result = match connect.mode {
                            StreamingMode::Server => start_server_stream(
                                connect,
                                conn_id,
                                context,
                                channel,
                                res_tx.clone(),
                                lifecycle_tx.clone(),
                                rl_manager,
                                rl_ctx,
                                rl_weight,
                                timeout,
                                ctx.desc.max_decoding_message_size,
                                ctx.desc.max_encoding_message_size,
                                &local,
                            ),
                            StreamingMode::Client => start_client_stream(
                                connect,
                                conn_id,
                                context,
                                channel,
                                res_tx.clone(),
                                lifecycle_tx.clone(),
                                rl_manager,
                                rl_ctx,
                                rl_weight,
                                timeout,
                                ctx.desc.max_decoding_message_size,
                                ctx.desc.max_encoding_message_size,
                                ctx.desc.outbound_buffer,
                                &local,
                            ),
                            StreamingMode::Bidi => start_bidi_stream(
                                connect,
                                conn_id,
                                context,
                                channel,
                                res_tx.clone(),
                                lifecycle_tx.clone(),
                                rl_manager,
                                rl_ctx,
                                rl_weight,
                                timeout,
                                ctx.desc.max_decoding_message_size,
                                ctx.desc.max_encoding_message_size,
                                ctx.desc.outbound_buffer,
                                &local,
                            ),
                        };

                        match result {
                            Ok(active) => {
                                active_streams.insert(conn_id, active);
                            }
                            Err(status) => {
                                emit_event(
                                    &res_tx,
                                    Some(conn_id),
                                    req_id,
                                    label.as_ref(),
                                    payload.as_ref(),
                                    StreamingEvent::from_status(status),
                                );
                            }
                        }
                    }
                    StreamingActionInner::Send(message) => {
                        let Some(active) = active_streams.get(&conn_id) else {
                            emit_event(
                                &res_tx,
                                Some(conn_id),
                                req_id,
                                label.as_ref(),
                                payload.as_ref(),
                                StreamingEvent::from_status(Status::failed_precondition(
                                    "stream is not connected",
                                )),
                            );
                            continue;
                        };

                        match active.mode {
                            StreamingMode::Server => {
                                emit_event(
                                    &res_tx,
                                    Some(conn_id),
                                    req_id,
                                    label.as_ref(),
                                    payload.as_ref(),
                                    StreamingEvent::from_status(Status::failed_precondition(
                                        "cannot send payload to server-streaming session",
                                    )),
                                );
                            }
                            StreamingMode::Client | StreamingMode::Bidi => {
                                let Some(sender) = active.sender.clone() else {
                                    emit_event(
                                        &res_tx,
                                        Some(conn_id),
                                        req_id,
                                        label.as_ref(),
                                        payload.as_ref(),
                                        StreamingEvent::from_status(Status::failed_precondition(
                                            "stream writer not available",
                                        )),
                                    );
                                    continue;
                                };

                                let rl_manager = if rl_ctx.is_some() {
                                    Some(rl_manager.clone())
                                } else {
                                    None
                                };
                                let res_tx_ref = res_tx.clone();

                                local.spawn_local(async move {
                                    if let (Some(manager), Some(ctx_bytes)) =
                                        (&rl_manager, rl_ctx.as_ref())
                                    {
                                        if let Some(plan) = {
                                            let mut guard = manager.lock().await;
                                            guard.plan(ctx_bytes, rl_weight)
                                        } {
                                            for (bucket, weight) in plan {
                                                bucket.wait(weight).await;
                                            }
                                        }
                                    }

                                    if let Err(err) = sender.send(message).await {
                                        emit_event(
                                            &res_tx_ref,
                                            Some(conn_id),
                                            req_id,
                                            label.as_ref(),
                                            payload.as_ref(),
                                            StreamingEvent::from_status(Status::unavailable(
                                                format!("streaming message send failed: {}", err),
                                            )),
                                        );
                                    }
                                });
                            }
                        }
                    }
                    StreamingActionInner::Finish => {
                        let Some(active) = active_streams.get_mut(&conn_id) else {
                            emit_event(
                                &res_tx,
                                Some(conn_id),
                                req_id,
                                label.as_ref(),
                                payload.as_ref(),
                                StreamingEvent::from_status(Status::failed_precondition(
                                    "stream is not connected",
                                )),
                            );
                            continue;
                        };

                        match active.mode {
                            StreamingMode::Client | StreamingMode::Bidi => {
                                if let Some(sender) = active.sender.take() {
                                    drop(sender);
                                }
                            }
                            StreamingMode::Server => {
                                emit_event(
                                    &res_tx,
                                    Some(conn_id),
                                    req_id,
                                    label.as_ref(),
                                    payload.as_ref(),
                                    StreamingEvent::from_status(Status::failed_precondition(
                                        "finish action is invalid for server streaming",
                                    )),
                                );
                            }
                        }
                    }
                    StreamingActionInner::Disconnect => {
                        let Some(active) = active_streams.remove(&conn_id) else {
                            emit_event(
                                &res_tx,
                                Some(conn_id),
                                req_id,
                                label.as_ref(),
                                payload.as_ref(),
                                StreamingEvent::from_status(Status::failed_precondition(
                                    "stream is not connected",
                                )),
                            );
                            continue;
                        };

                        active.stop();
                        emit_event(
                            &res_tx,
                            Some(conn_id),
                            req_id,
                            label.as_ref(),
                            payload.as_ref(),
                            StreamingEvent::from_status(Status::cancelled(
                                "stream disconnected by client",
                            )),
                        );
                    }
                }
            }

            let mut budget = ctx.desc.max_hook_calls_at_once;

            while budget > 0 {
                match res_rx.try_recv() {
                    Ok(event) => {
                        budget -= 1;
                        hook.call(HookArgs::new(
                            event,
                            &mut ctx.event_tx,
                            &mut ctx.reducer,
                            &ctx.state,
                            &ctx.desc,
                            &mut ctx.health,
                        ));
                    }
                    Err(_) => break,
                }
            }

            rt.block_on(local.run_until(async {
                if wait_async_tasks.is_zero() {
                    tokio::task::yield_now().await;
                } else {
                    sleep(wait_async_tasks).await;
                }
            }));
        }
    }
}
