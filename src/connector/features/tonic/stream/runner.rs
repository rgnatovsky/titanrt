use crate::connector::errors::{StreamError, StreamResult};
use crate::connector::features::shared::actions::StreamAction;
use crate::connector::features::shared::clients_map::ClientsMap;
use crate::connector::features::shared::events::StreamEvent;
use crate::connector::features::shared::rate_limiter::RateLimitManager;
use crate::connector::features::tonic::client::{TonicChannelSpec, TonicClient};
use crate::connector::features::tonic::codec::RawCodec;
use crate::connector::features::tonic::connector::TonicConnector;
use crate::connector::features::tonic::stream::GrpcStreamMode;
use crate::connector::features::tonic::stream::actions::{GrpcCommand, GrpcStreamCommand};
use crate::connector::features::tonic::stream::descriptor::TonicDescriptor;
use crate::connector::features::tonic::stream::event::{GrpcEvent, GrpcEventKind};
use crate::connector::features::tonic::stream::handle_bidi::start_bidi_stream;
use crate::connector::features::tonic::stream::handle_client::start_client_stream;
use crate::connector::features::tonic::stream::handle_server::start_server_stream;
use crate::connector::features::tonic::stream::utils::{
    ActiveStream, StreamContext, StreamLifecycle, emit_event,
};
use crate::connector::{Hook, HookArgs, IntoHook, RuntimeCtx, StreamRunner, StreamSpawner};
use crate::io::ringbuffer::RingSender;
use crate::prelude::{BaseRx, TxPairExt};
use crate::utils::{Reducer, StateMarker};

use crossbeam::channel::unbounded;
use std::collections::HashMap;
use std::fmt::Debug;
use std::rc::Rc;
use tokio::runtime::Runtime;

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::LocalSet;
use tokio::time::{self, sleep};
use tonic::Status;
use tonic::client::Grpc;

impl<E, R, S, T> StreamSpawner<TonicDescriptor<T>, E, R, S, T> for TonicConnector
where
    E: TxPairExt,
    S: StateMarker,
    R: Reducer,
       T: Debug + Clone + Send + 'static,
{
}

impl<E, R, S, T> StreamRunner<TonicDescriptor<T>, E, R, S, T> for TonicConnector
where
    E: TxPairExt,
    S: StateMarker,
    R: Reducer,
    T: Debug + Clone + Send + 'static,
{
    type Config = (ClientsMap<TonicClient, TonicChannelSpec>, Arc<Runtime>);
    type ActionTx = RingSender<StreamAction<GrpcCommand>>;
    type RawEvent = StreamEvent<GrpcEvent>;
    type HookResult = ();

    fn build_config(&mut self, _desc: &TonicDescriptor<T>) -> anyhow::Result<Self::Config> {
        Ok((self.clients_map(), self.rt_tokio()))
    }

    fn run<H>(
        mut ctx: RuntimeCtx<
            (ClientsMap<TonicClient, TonicChannelSpec>, Arc<Runtime>),
            TonicDescriptor<T>,
            RingSender<StreamAction<GrpcCommand>>,
            E,
            R,
            S,
            T,
        >,
        hook: H,
    ) -> StreamResult<()>
    where
        H: IntoHook<StreamEvent<GrpcEvent>, E, R, S, TonicDescriptor<T>, (), T>,
        E: TxPairExt,
        S: StateMarker,
    {
        let mut hook = hook.into_hook();
        let wait_async_tasks = Duration::from_micros(ctx.desc.wait_async_tasks_us);
        let rl_manager = Rc::new(Mutex::new(RateLimitManager::new(
            ctx.desc.rate_limits.clone(),
        )));

        let local = LocalSet::new();
        let (res_tx, res_rx) = unbounded();
        let (lifecycle_tx, lifecycle_rx) = unbounded();

        let mut active_streams: HashMap<String, ActiveStream> = HashMap::new();

        ctx.health.up();

        let (clients_map, rt) = ctx.cfg;

        loop {
            if ctx.cancel.is_cancelled() {
                ctx.health.down();
                for (_, stream) in active_streams.drain() {
                    stream.stop();
                }
                break Err(StreamError::Cancelled);
            }

            while let Ok(StreamLifecycle::Closed { stream_name }) = lifecycle_rx.try_recv() {
                active_streams.remove(&stream_name);
            }

            while let Ok(action) = ctx.action_rx.try_recv() {
                let (
                    maybe_inner,
                    maybe_conn_id,
                    req_id,
                    label,
                    timeout,
                    rl_ctx,
                    rl_weight,
                    payload,
                ) = action.into_parts();

                let inner = match maybe_inner {
                    Some(inner) => inner,
                    None => {
                        emit_event(
                            &res_tx,
                            maybe_conn_id,
                            req_id,
                            label,
                            payload.as_ref(),
                            GrpcEvent::from_status(
                                GrpcEventKind::BadCommand,
                                Status::invalid_argument("stream action missing inner action"),
                            ),
                        );
                        continue;
                    }
                };

                match inner {
                    GrpcCommand::Stream(command) => {
                        let stream_name = label
                            .clone()
                            .map(|cow| cow.into_owned())
                            .unwrap_or_default();

                        match command {
                            GrpcStreamCommand::Connect(connect) => {
                                let conn_id = maybe_conn_id.unwrap_or(clients_map.len());

                                let Some(client) = clients_map.get(&conn_id) else {
                                    emit_event(
                                        &res_tx,
                                        Some(conn_id),
                                        req_id,
                                        label,
                                        payload.as_ref(),
                                        GrpcEvent::from_status(
                                            GrpcEventKind::BadCommand,
                                            Status::unavailable(
                                                "cannot find tonic client by connection id provided",
                                            ),
                                        ),
                                    );
                                    continue;
                                };

                                if let Some(prev) = active_streams.remove(&stream_name) {
                                    prev.stop();
                                }

                                let channel = client.channel();

                                let context = StreamContext {
                                    req_id,
                                    name: stream_name.clone(),
                                    payload: payload.clone(),
                                };

                                let rl_manager = if rl_ctx.is_some() {
                                    Some(rl_manager.clone())
                                } else {
                                    None
                                };

                                let active = match connect.mode {
                                    GrpcStreamMode::Server => start_server_stream(
                                        connect,
                                        conn_id,
                                        label.clone(),
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
                                    GrpcStreamMode::Client => start_client_stream(
                                        connect,
                                        conn_id,
                                        label.clone(),
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
                                    GrpcStreamMode::Bidi => start_bidi_stream(
                                        connect,
                                        conn_id,
                                        label.clone(),
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

                                active_streams.insert(stream_name, active);

                                emit_event(
                                    &res_tx,
                                    maybe_conn_id,
                                    req_id,
                                    label,
                                    payload.as_ref(),
                                    GrpcEvent::from_ok(),
                                );
                            }
                            GrpcStreamCommand::Send(message) => {
                                let Some(active) = active_streams.get(&stream_name) else {
                                    emit_event(
                                        &res_tx,
                                        maybe_conn_id,
                                        req_id,
                                        label,
                                        payload.as_ref(),
                                        GrpcEvent::from_status(
                                            GrpcEventKind::BadCommand,
                                            Status::failed_precondition("stream is not connected"),
                                        ),
                                    );
                                    continue;
                                };

                                match active.mode {
                                    GrpcStreamMode::Server => {
                                        emit_event(
                                            &res_tx,
                                            maybe_conn_id,
                                            req_id,
                                            label,
                                            payload.as_ref(),
                                            GrpcEvent::from_status(
                                                GrpcEventKind::BadCommand,
                                                Status::failed_precondition(
                                                    "cannot send payload to server-streaming session",
                                                ),
                                            ),
                                        );
                                    }
                                    GrpcStreamMode::Client | GrpcStreamMode::Bidi => {
                                        let Some(sender) = active.sender.clone() else {
                                            emit_event(
                                                &res_tx,
                                                maybe_conn_id,
                                                req_id,
                                                label,
                                                payload.as_ref(),
                                                GrpcEvent::from_status(
                                                    GrpcEventKind::StreamItem,
                                                    Status::failed_precondition(
                                                        "stream writer not available",
                                                    ),
                                                ),
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
                                                    maybe_conn_id,
                                                    req_id,
                                                    label,
                                                    payload.as_ref(),
                                                    GrpcEvent::from_status(
                                                        GrpcEventKind::StreamItem,
                                                        Status::unavailable(format!(
                                                            "streaming message send failed: {}",
                                                            err
                                                        )),
                                                    ),
                                                );
                                            }
                                        });
                                    }
                                }
                            }
                            GrpcStreamCommand::Finish => {
                                let Some(active) = active_streams.get_mut(&stream_name) else {
                                    emit_event(
                                        &res_tx,
                                        maybe_conn_id,
                                        req_id,
                                        label,
                                        payload.as_ref(),
                                        GrpcEvent::from_status(
                                            GrpcEventKind::BadCommand,
                                            Status::failed_precondition("stream is not connected"),
                                        ),
                                    );
                                    continue;
                                };

                                match active.mode {
                                    GrpcStreamMode::Client | GrpcStreamMode::Bidi => {
                                        if let Some(sender) = active.sender.take() {
                                            drop(sender);
                                        }
                                    }
                                    GrpcStreamMode::Server => {
                                        emit_event(
                                            &res_tx,
                                            maybe_conn_id,
                                            req_id,
                                            label,
                                            payload.as_ref(),
                                            GrpcEvent::from_status(
                                                GrpcEventKind::BadCommand,
                                                Status::failed_precondition(
                                                    "finish action is invalid for server streaming",
                                                ),
                                            ),
                                        );
                                    }
                                }
                            }
                            GrpcStreamCommand::Disconnect => {
                                let Some(active) = active_streams.remove(&stream_name) else {
                                    emit_event(
                                        &res_tx,
                                        maybe_conn_id,
                                        req_id,
                                        label,
                                        payload.as_ref(),
                                        GrpcEvent::from_status(
                                            GrpcEventKind::BadCommand,
                                            Status::failed_precondition("stream is not connected"),
                                        ),
                                    );
                                    continue;
                                };

                                active.stop();
                                emit_event(
                                    &res_tx,
                                    maybe_conn_id,
                                    req_id,
                                    label,
                                    payload.as_ref(),
                                    GrpcEvent::from_status(
                                        GrpcEventKind::StreamDisconnected,
                                        Status::cancelled("stream disconnected by client"),
                                    ),
                                );
                            }
                        }
                    }
                    GrpcCommand::Unary(call) => {
                        let conn_id = maybe_conn_id.unwrap_or(clients_map.len());

                        let mut sync_err_builder = StreamEvent::builder(None)
                            .conn_id(Some(conn_id))
                            .req_id(req_id)
                            .label(label.clone());

                        if let Some(p) = payload.as_ref() {
                            sync_err_builder = sync_err_builder.payload(Some(p.clone()));
                        }

                        let Some(client) = clients_map.get(&conn_id) else {
                            if let Ok(event) = sync_err_builder
                                .inner(GrpcEvent::from_status(
                                    GrpcEventKind::UnaryResponse,
                                    Status::failed_precondition(
                                        "no channel found for connection id",
                                    ),
                                ))
                                .build()
                            {
                                let _ = res_tx.try_send(event);
                            }
                            continue;
                        };

                        let channel = client.channel();

                        let max_dec = ctx.desc.max_decoding_message_size;
                        let max_enc = ctx.desc.max_encoding_message_size;

                        let res_tx_task = res_tx.clone();
                        let label_task = label.clone();
                        let payload_task = payload.clone();

                        let (rl_mgr_task, rl_ctx_task) = match rl_ctx {
                            Some(ctx_bytes) => (Some(rl_manager.clone()), Some(ctx_bytes)),
                            _ => (None, None),
                        };

                        local.spawn_local(async move {
                            let mut builder = StreamEvent::builder(None)
                                .conn_id(Some(conn_id))
                                .req_id(req_id)
                                .label(label_task);

                            if let Some(p) = payload_task.as_ref() {
                                builder = builder.payload(Some(p.clone()));
                            }

                            let mut grpc = Grpc::new(channel);
                            if let Some(size) = max_dec {
                                grpc = grpc.max_decoding_message_size(size);
                            }
                            if let Some(size) = max_enc {
                                grpc = grpc.max_encoding_message_size(size);
                            }

                            if let (Some(manager), Some(ctx_bytes)) =
                                (rl_mgr_task, rl_ctx_task.as_ref())
                            {
                                if let Some(plan) = {
                                    let mut guard = manager.lock().await;
                                    guard.plan(ctx_bytes, rl_weight)
                                } {
                                    futures::future::join_all(
                                        plan.into_iter()
                                            .map(|(b, w)| async move { b.wait(w).await }),
                                    )
                                    .await;
                                }
                            }

                            if let Err(e) = grpc.ready().await {
                                let mut msg = String::from("channel not ready with error: ");
                                msg.push_str(&e.to_string());

                                if let Ok(event) = builder
                                    .inner(GrpcEvent::from_status(
                                        GrpcEventKind::UnaryResponse,
                                        Status::unavailable(msg),
                                    ))
                                    .build()
                                {
                                    let _ = res_tx_task.try_send(event);
                                }
                                return;
                            }

                            let response = match timeout {
                                Some(limit) => {
                                    let mut request = tonic::Request::new(call.msg.freeze());
                                    *request.metadata_mut() = call.meta;
                                    match time::timeout(
                                        limit,
                                        grpc.unary(request, call.method, RawCodec),
                                    )
                                    .await
                                    {
                                        Ok(res) => res,
                                        Err(_) => Err(Status::deadline_exceeded("client timeout")),
                                    }
                                }
                                None => {
                                    let mut request = tonic::Request::new(call.msg.freeze());
                                    *request.metadata_mut() = call.meta;
                                    grpc.unary(request, call.method, RawCodec).await
                                }
                            };

                            let inner = match response {
                                Ok(res) => GrpcEvent::from_ok_unary(res),
                                Err(status) => {
                                    GrpcEvent::from_status(GrpcEventKind::UnaryResponse, status)
                                }
                            };

                            if let Ok(event) = builder.inner(inner).build() {
                                let _ = res_tx_task.try_send(event);
                            }
                        });
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
