use crate::connector::errors::{StreamError, StreamResult};
use crate::connector::features::shared::actions::StreamAction;
use crate::connector::features::shared::clients_map::ClientsMap;
use crate::connector::features::shared::events::StreamEvent;
use crate::connector::features::shared::rate_limiter::RateLimitManager;
use crate::connector::features::tonic::client::{TonicChannelSpec, TonicClient};
use crate::connector::features::tonic::connector::TonicConnector;
use crate::connector::features::tonic::streaming::actions::{ConnectConfig, StreamingActionInner};
use crate::connector::features::tonic::streaming::codec::RawCodec;
use crate::connector::features::tonic::streaming::descriptor::{
    StreamingMode, TonicStreamingDescriptor,
};
use crate::connector::features::tonic::streaming::event::StreamingEvent;
use crate::connector::{Hook, HookArgs, IntoHook, RuntimeCtx, StreamRunner, StreamSpawner};
use crate::io::ringbuffer::RingSender;
use crate::prelude::{BaseRx, TxPairExt};
use crate::utils::{Reducer, StateMarker};

use anyhow::anyhow;
use bytes::Bytes;
use crossbeam::channel::{Sender, unbounded};
use futures::{Stream, StreamExt};
use serde_json::Value;
use std::borrow::Cow;
use std::collections::HashMap;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::{Mutex, mpsc};
use tokio::task::LocalSet;
use tokio::time::sleep;
use tonic::client::Grpc;
use tonic::transport::Channel;
use tonic::{Request, Status};
use uuid::Uuid;

struct ActiveStream {
    mode: StreamingMode,
    sender: Option<mpsc::Sender<Bytes>>,
    handle: tokio::task::JoinHandle<()>,
}

impl ActiveStream {
    fn stop(mut self) {
        if let Some(sender) = self.sender.take() {
            drop(sender);
        }
        self.handle.abort();
    }
}

enum StreamLifecycle {
    Closed { conn_id: usize },
}

#[derive(Clone)]
struct StreamContext {
    req_id: Option<Uuid>,
    label: Option<String>,
    payload: Option<Value>,
}

fn emit_event(
    sender: &Sender<StreamEvent<StreamingEvent>>,
    conn_id: usize,
    req_id: Option<Uuid>,
    label: Option<&String>,
    payload: Option<&Value>,
    inner: StreamingEvent,
) {
    let mut builder = StreamEvent::builder(Some(inner))
        .conn_id(Some(conn_id))
        .req_id(req_id);

    if let Some(label) = label {
        builder = builder.label(Some(Cow::Owned(label.clone())));
    }

    if let Some(payload) = payload {
        builder = builder.payload(Some(payload.clone()));
    }

    if let Ok(event) = builder.build() {
        let _ = sender.try_send(event);
    }
}
struct MpscBytesStream {
    inner: mpsc::Receiver<Bytes>,
}

impl MpscBytesStream {
    fn new(inner: mpsc::Receiver<Bytes>) -> Self {
        Self { inner }
    }
}

impl Stream for MpscBytesStream {
    type Item = Bytes;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.inner.poll_recv(cx)
    }
}

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
                let (inner, conn_id_opt, req_id, label, timeout, rl_ctx, rl_weight, payload) =
                    action.into_parts();

                let inner = match inner {
                    Some(inner) => inner,
                    None => continue,
                };

                let conn_id = match conn_id_opt {
                    Some(id) => id,
                    None => {
                        emit_event(
                            &res_tx,
                            usize::MAX,
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

                        let Some(client) = ctx.cfg.get(&conn_id).cloned() else {
                            emit_event(
                                &res_tx,
                                conn_id,
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

                        let result = start_stream(
                            ctx.desc.mode(),
                            connect,
                            conn_id,
                            context,
                            channel,
                            res_tx.clone(),
                            lifecycle_tx.clone(),
                            if rl_ctx.is_some() {
                                Some(rl_manager.clone())
                            } else {
                                None
                            },
                            rl_ctx.clone(),
                            rl_weight,
                            timeout,
                            ctx.desc.max_decoding_message_size,
                            ctx.desc.max_encoding_message_size,
                            ctx.desc.outbound_buffer,
                            &local,
                        );

                        match result {
                            Ok(active) => {
                                active_streams.insert(conn_id, active);
                            }
                            Err(status) => {
                                emit_event(
                                    &res_tx,
                                    conn_id,
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
                                conn_id,
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
                                    conn_id,
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
                                        conn_id,
                                        req_id,
                                        label.as_ref(),
                                        payload.as_ref(),
                                        StreamingEvent::from_status(Status::failed_precondition(
                                            "stream writer not available",
                                        )),
                                    );
                                    continue;
                                };

                                spawn_send_task(
                                    &local,
                                    sender,
                                    message,
                                    conn_id,
                                    req_id,
                                    label.clone(),
                                    payload.clone(),
                                    res_tx.clone(),
                                    if rl_ctx.is_some() {
                                        Some(rl_manager.clone())
                                    } else {
                                        None
                                    },
                                    rl_ctx.clone(),
                                    rl_weight,
                                );
                            }
                        }
                    }
                    StreamingActionInner::Finish => {
                        let Some(active) = active_streams.get_mut(&conn_id) else {
                            emit_event(
                                &res_tx,
                                conn_id,
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
                                    conn_id,
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
                                conn_id,
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
                            conn_id,
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
fn start_stream(
    mode: StreamingMode,
    connect: ConnectConfig,
    conn_id: usize,
    context: StreamContext,
    channel: Channel,
    res_tx: Sender<StreamEvent<StreamingEvent>>,
    lifecycle_tx: Sender<StreamLifecycle>,
    rl_manager: Option<Rc<Mutex<RateLimitManager>>>,
    rl_ctx: Option<Bytes>,
    rl_weight: Option<usize>,
    timeout: Option<Duration>,
    max_dec_size: Option<usize>,
    max_enc_size: Option<usize>,
    outbound_buffer: usize,
    local: &LocalSet,
) -> Result<ActiveStream, Status> {
    match mode {
        StreamingMode::Server => start_server_stream(
            connect,
            conn_id,
            context,
            channel,
            res_tx,
            lifecycle_tx,
            rl_manager,
            rl_ctx,
            rl_weight,
            timeout,
            max_dec_size,
            max_enc_size,
            local,
        ),
        StreamingMode::Client => start_client_stream(
            connect,
            conn_id,
            context,
            channel,
            res_tx,
            lifecycle_tx,
            rl_manager,
            rl_ctx,
            rl_weight,
            timeout,
            max_dec_size,
            max_enc_size,
            outbound_buffer,
            local,
        ),
        StreamingMode::Bidi => start_bidi_stream(
            connect,
            conn_id,
            context,
            channel,
            res_tx,
            lifecycle_tx,
            rl_manager,
            rl_ctx,
            rl_weight,
            timeout,
            max_dec_size,
            max_enc_size,
            outbound_buffer,
            local,
        ),
    }
}

fn start_server_stream(
    connect: ConnectConfig,
    conn_id: usize,
    context: StreamContext,
    channel: Channel,
    res_tx: Sender<StreamEvent<StreamingEvent>>,
    lifecycle_tx: Sender<StreamLifecycle>,
    rl_manager: Option<Rc<Mutex<RateLimitManager>>>,
    rl_ctx: Option<Bytes>,
    rl_weight: Option<usize>,
    timeout: Option<Duration>,
    max_dec_size: Option<usize>,
    max_enc_size: Option<usize>,
    local: &LocalSet,
) -> Result<ActiveStream, Status> {
    let ConnectConfig {
        method,
        initial_message,
        metadata,
    } = connect;

    let initial_payload = initial_message.unwrap_or_else(Bytes::new);
    let handle = local.spawn_local(async move {
        let StreamContext {
            req_id,
            label,
            payload,
        } = context;
        let mut grpc = Grpc::new(channel);

        if let Some(size) = max_dec_size {
            grpc = grpc.max_decoding_message_size(size);
        }
        if let Some(size) = max_enc_size {
            grpc = grpc.max_encoding_message_size(size);
        }

        if let (Some(manager), Some(ctx_bytes)) = (&rl_manager, rl_ctx.as_ref()) {
            if let Some(plan) = {
                let mut guard = manager.lock().await;
                guard.plan(ctx_bytes, rl_weight)
            } {
                for (bucket, weight) in plan {
                    bucket.wait(weight).await;
                }
            }
        }

        if let Err(e) = grpc.ready().await {
            emit_event(
                &res_tx,
                conn_id,
                req_id,
                label.as_ref(),
                payload.as_ref(),
                StreamingEvent::from_status(Status::unavailable(format!(
                    "[TonicStream - Runner] channel not ready with error: {e}",
                ))),
            );
            let _ = lifecycle_tx.send(StreamLifecycle::Closed { conn_id });
            return;
        }

        let mut request = Request::new(initial_payload);
        *request.metadata_mut() = metadata.clone();

        let call = grpc.server_streaming(request, method.clone(), RawCodec);
        let response = match timeout {
            Some(t) => match tokio::time::timeout(t, call).await {
                Ok(res) => res,
                Err(_) => {
                    emit_event(
                        &res_tx,
                        conn_id,
                        req_id,
                        label.as_ref(),
                        payload.as_ref(),
                        StreamingEvent::from_status(Status::deadline_exceeded("connect timeout")),
                    );
                    let _ = lifecycle_tx.send(StreamLifecycle::Closed { conn_id });
                    return;
                }
            },
            None => call.await,
        };

        match response {
            Ok(resp) => {
                let mut inbound = resp.into_inner();
                while let Some(item) = inbound.next().await {
                    match item {
                        Ok(bytes) => {
                            emit_event(
                                &res_tx,
                                conn_id,
                                req_id,
                                label.as_ref(),
                                payload.as_ref(),
                                StreamingEvent::from_ok_stream_item(bytes),
                            );
                        }
                        Err(status) => {
                            emit_event(
                                &res_tx,
                                conn_id,
                                req_id,
                                label.as_ref(),
                                payload.as_ref(),
                                StreamingEvent::from_status(status),
                            );
                            break;
                        }
                    }
                }

                match inbound.trailers().await {
                    Ok(Some(trailers)) => emit_event(
                        &res_tx,
                        conn_id,
                        req_id,
                        label.as_ref(),
                        payload.as_ref(),
                        StreamingEvent::from_ok_stream_close(trailers),
                    ),
                    Ok(None) => emit_event(
                        &res_tx,
                        conn_id,
                        req_id,
                        label.as_ref(),
                        payload.as_ref(),
                        StreamingEvent::from_ok_stream_close(metadata.clone()),
                    ),
                    Err(status) => emit_event(
                        &res_tx,
                        conn_id,
                        req_id,
                        label.as_ref(),
                        payload.as_ref(),
                        StreamingEvent::from_status(status),
                    ),
                }
            }
            Err(status) => {
                emit_event(
                    &res_tx,
                    conn_id,
                    req_id,
                    label.as_ref(),
                    payload.as_ref(),
                    StreamingEvent::from_status(status),
                );
            }
        }

        let _ = lifecycle_tx.send(StreamLifecycle::Closed { conn_id });
    });

    Ok(ActiveStream {
        mode: StreamingMode::Server,
        sender: None,
        handle,
    })
}
fn start_client_stream(
    connect: ConnectConfig,
    conn_id: usize,
    context: StreamContext,
    channel: Channel,
    res_tx: Sender<StreamEvent<StreamingEvent>>,
    lifecycle_tx: Sender<StreamLifecycle>,
    rl_manager: Option<Rc<Mutex<RateLimitManager>>>,
    rl_ctx: Option<Bytes>,
    rl_weight: Option<usize>,
    timeout: Option<Duration>,
    max_dec_size: Option<usize>,
    max_enc_size: Option<usize>,
    outbound_buffer: usize,
    local: &LocalSet,
) -> Result<ActiveStream, Status> {
    let ConnectConfig {
        method,
        initial_message,
        metadata,
    } = connect;

    let buffer = outbound_buffer.max(1);
    let (tx, rx) = mpsc::channel::<Bytes>(buffer);

    if let Some(initial) = initial_message {
        tx.try_send(initial).map_err(|e| {
            Status::resource_exhausted(format!("initial message buffered send failed: {e}"))
        })?;
    }

    let handle = local.spawn_local(async move {
        let StreamContext {
            req_id,
            label,
            payload,
        } = context;
        let mut grpc = Grpc::new(channel);

        if let Some(size) = max_dec_size {
            grpc = grpc.max_decoding_message_size(size);
        }
        if let Some(size) = max_enc_size {
            grpc = grpc.max_encoding_message_size(size);
        }

        if let (Some(manager), Some(ctx_bytes)) = (&rl_manager, rl_ctx.as_ref()) {
            if let Some(plan) = {
                let mut guard = manager.lock().await;
                guard.plan(ctx_bytes, rl_weight)
            } {
                for (bucket, weight) in plan {
                    bucket.wait(weight).await;
                }
            }
        }

        if let Err(e) = grpc.ready().await {
            emit_event(
                &res_tx,
                conn_id,
                req_id,
                label.as_ref(),
                payload.as_ref(),
                StreamingEvent::from_status(Status::unavailable(format!(
                    "[TonicStream - Runner] channel not ready with error: {e}",
                ))),
            );
            let _ = lifecycle_tx.send(StreamLifecycle::Closed { conn_id });
            return;
        }

        let outbound = MpscBytesStream::new(rx);
        let mut request = Request::new(outbound);
        *request.metadata_mut() = metadata.clone();

        let call = grpc.client_streaming(request, method.clone(), RawCodec);
        let response = match timeout {
            Some(t) => match tokio::time::timeout(t, call).await {
                Ok(res) => res,
                Err(_) => {
                    emit_event(
                        &res_tx,
                        conn_id,
                        req_id,
                        label.as_ref(),
                        payload.as_ref(),
                        StreamingEvent::from_status(Status::deadline_exceeded("connect timeout")),
                    );
                    let _ = lifecycle_tx.send(StreamLifecycle::Closed { conn_id });
                    return;
                }
            },
            None => call.await,
        };

        match response {
            Ok(resp) => {
                emit_event(
                    &res_tx,
                    conn_id,
                    req_id,
                    label.as_ref(),
                    payload.as_ref(),
                    StreamingEvent::from_ok_unary(resp),
                );
            }
            Err(status) => {
                emit_event(
                    &res_tx,
                    conn_id,
                    req_id,
                    label.as_ref(),
                    payload.as_ref(),
                    StreamingEvent::from_status(status),
                );
            }
        }

        let _ = lifecycle_tx.send(StreamLifecycle::Closed { conn_id });
    });

    Ok(ActiveStream {
        mode: StreamingMode::Client,
        sender: Some(tx),
        handle,
    })
}
fn start_bidi_stream(
    connect: ConnectConfig,
    conn_id: usize,
    context: StreamContext,
    channel: Channel,
    res_tx: Sender<StreamEvent<StreamingEvent>>,
    lifecycle_tx: Sender<StreamLifecycle>,
    rl_manager: Option<Rc<Mutex<RateLimitManager>>>,
    rl_ctx: Option<Bytes>,
    rl_weight: Option<usize>,
    timeout: Option<Duration>,
    max_dec_size: Option<usize>,
    max_enc_size: Option<usize>,
    outbound_buffer: usize,
    local: &LocalSet,
) -> Result<ActiveStream, Status> {
    let ConnectConfig {
        method,
        initial_message,
        metadata,
    } = connect;

    let buffer = outbound_buffer.max(1);
    let (tx, rx) = mpsc::channel::<Bytes>(buffer);

    if let Some(initial) = initial_message {
        tx.try_send(initial).map_err(|e| {
            Status::resource_exhausted(format!("initial message buffered send failed: {e}"))
        })?;
    }

    let handle = local.spawn_local(async move {
        let StreamContext {
            req_id,
            label,
            payload,
        } = context;
        let mut grpc = Grpc::new(channel);

        if let Some(size) = max_dec_size {
            grpc = grpc.max_decoding_message_size(size);
        }
        if let Some(size) = max_enc_size {
            grpc = grpc.max_encoding_message_size(size);
        }

        if let (Some(manager), Some(ctx_bytes)) = (&rl_manager, rl_ctx.as_ref()) {
            if let Some(plan) = {
                let mut guard = manager.lock().await;
                guard.plan(ctx_bytes, rl_weight)
            } {
                for (bucket, weight) in plan {
                    bucket.wait(weight).await;
                }
            }
        }

        if let Err(e) = grpc.ready().await {
            emit_event(
                &res_tx,
                conn_id,
                req_id,
                label.as_ref(),
                payload.as_ref(),
                StreamingEvent::from_status(Status::unavailable(format!(
                    "[TonicStream - Runner] channel not ready with error: {e}",
                ))),
            );
            let _ = lifecycle_tx.send(StreamLifecycle::Closed { conn_id });
            return;
        }

        let outbound = MpscBytesStream::new(rx);
        let mut request = Request::new(outbound);
        *request.metadata_mut() = metadata.clone();

        let call = grpc.streaming(request, method.clone(), RawCodec);
        let response = match timeout {
            Some(t) => match tokio::time::timeout(t, call).await {
                Ok(res) => res,
                Err(_) => {
                    emit_event(
                        &res_tx,
                        conn_id,
                        req_id,
                        label.as_ref(),
                        payload.as_ref(),
                        StreamingEvent::from_status(Status::deadline_exceeded("connect timeout")),
                    );
                    let _ = lifecycle_tx.send(StreamLifecycle::Closed { conn_id });
                    return;
                }
            },
            None => call.await,
        };

        match response {
            Ok(resp) => {
                let mut inbound = resp.into_inner();
                while let Some(item) = inbound.next().await {
                    match item {
                        Ok(bytes) => {
                            emit_event(
                                &res_tx,
                                conn_id,
                                req_id,
                                label.as_ref(),
                                payload.as_ref(),
                                StreamingEvent::from_ok_stream_item(bytes),
                            );
                        }
                        Err(status) => {
                            emit_event(
                                &res_tx,
                                conn_id,
                                req_id,
                                label.as_ref(),
                                payload.as_ref(),
                                StreamingEvent::from_status(status),
                            );
                            break;
                        }
                    }
                }

                match inbound.trailers().await {
                    Ok(Some(trailers)) => emit_event(
                        &res_tx,
                        conn_id,
                        req_id,
                        label.as_ref(),
                        payload.as_ref(),
                        StreamingEvent::from_ok_stream_close(trailers),
                    ),
                    Ok(None) => emit_event(
                        &res_tx,
                        conn_id,
                        req_id,
                        label.as_ref(),
                        payload.as_ref(),
                        StreamingEvent::from_ok_stream_close(metadata.clone()),
                    ),
                    Err(status) => emit_event(
                        &res_tx,
                        conn_id,
                        req_id,
                        label.as_ref(),
                        payload.as_ref(),
                        StreamingEvent::from_status(status),
                    ),
                }
            }
            Err(status) => {
                emit_event(
                    &res_tx,
                    conn_id,
                    req_id,
                    label.as_ref(),
                    payload.as_ref(),
                    StreamingEvent::from_status(status),
                );
            }
        }

        let _ = lifecycle_tx.send(StreamLifecycle::Closed { conn_id });
    });

    Ok(ActiveStream {
        mode: StreamingMode::Bidi,
        sender: Some(tx),
        handle,
    })
}
fn spawn_send_task(
    local: &LocalSet,
    sender: mpsc::Sender<Bytes>,
    message: Bytes,
    conn_id: usize,
    req_id: Option<Uuid>,
    label: Option<String>,
    payload: Option<Value>,
    res_tx: Sender<StreamEvent<StreamingEvent>>,
    rl_manager: Option<Rc<Mutex<RateLimitManager>>>,
    rl_ctx: Option<Bytes>,
    rl_weight: Option<usize>,
) {
    local.spawn_local(async move {
        if let (Some(manager), Some(ctx_bytes)) = (&rl_manager, rl_ctx.as_ref()) {
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
                &res_tx,
                conn_id,
                req_id,
                label.as_ref(),
                payload.as_ref(),
                StreamingEvent::from_status(Status::unavailable(format!(
                    "stream send failed: {}",
                    err
                ))),
            );
        }
    });
}
