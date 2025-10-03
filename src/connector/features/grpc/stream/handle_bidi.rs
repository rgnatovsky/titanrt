use crate::connector::features::shared::events::StreamEvent;
use crate::connector::features::shared::rate_limiter::RateLimitManager;
use crate::connector::features::grpc::codec::RawCodec;
use crate::connector::features::grpc::stream::GrpcStreamMode;
use crate::connector::features::grpc::stream::actions::GrpcStreamConnect;
use crate::connector::features::grpc::stream::event::{GrpcEvent, GrpcEventKind};
use crate::connector::features::grpc::stream::utils::{
    ActiveStream, MpscBytesStream, StreamContext, StreamLifecycle, emit_event,
};

use bytes::Bytes;
use crossbeam::channel::Sender;
use futures::StreamExt;

use std::borrow::Cow;
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::{Mutex, mpsc};
use tokio::task::LocalSet;
use tonic::client::Grpc;
use tonic::transport::Channel;
use tonic::{Request, Status};

pub fn start_bidi_stream(
    connect: GrpcStreamConnect,
    conn_id: usize,
    label: Option<Cow<'static, str>>,
    context: StreamContext,
    channel: Channel,
    res_tx: Sender<StreamEvent<GrpcEvent>>,
    lifecycle_tx: Sender<StreamLifecycle>,
    rl_manager: Option<Rc<Mutex<RateLimitManager>>>,
    rl_ctx: Option<Bytes>,
    rl_weight: Option<usize>,
    timeout: Option<Duration>,
    max_dec_size: Option<usize>,
    max_enc_size: Option<usize>,
    outbound_buffer: usize,
    local: &LocalSet,
) -> ActiveStream {
    let GrpcStreamConnect {
        mode: _,
        method,
        initial_message,
        metadata,
    } = connect;

    let buffer = outbound_buffer.max(1);
    let (tx, rx) = mpsc::channel::<Bytes>(buffer);

    if let Some(initial) = initial_message {
        tx.try_send(initial).unwrap()
    }

    let handle = local.spawn_local(async move {
        let StreamContext {
            req_id,
            name: stream_name,
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
                Some(conn_id),
                req_id,
                label,
                payload.as_ref(),
                GrpcEvent::from_status(
                    GrpcEventKind::StreamConnected,
                    Status::unavailable(format!("channel not ready with error: {e}",)),
                ),
            );
            let _ = lifecycle_tx.send(StreamLifecycle::Closed { stream_name });
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
                        Some(conn_id),
                        req_id,
                        label,
                        payload.as_ref(),
                        GrpcEvent::from_status(
                            GrpcEventKind::StreamConnected,
                            Status::deadline_exceeded("connect timeout"),
                        ),
                    );
                    let _ = lifecycle_tx.send(StreamLifecycle::Closed { stream_name });
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
                                Some(conn_id),
                                None,
                                label.clone(),
                                payload.as_ref(),
                                GrpcEvent::from_ok_stream_item(bytes),
                            );
                        }
                        Err(status) => {
                            emit_event(
                                &res_tx,
                                Some(conn_id),
                                None,
                                label.clone(),
                                payload.as_ref(),
                                GrpcEvent::from_status(GrpcEventKind::StreamItem, status),
                            );
                            break;
                        }
                    }
                }

                match inbound.trailers().await {
                    Ok(Some(trailers)) => emit_event(
                        &res_tx,
                        Some(conn_id),
                        None,
                        label,
                        payload.as_ref(),
                        GrpcEvent::from_ok_stream_close(trailers),
                    ),
                    Ok(None) => emit_event(
                        &res_tx,
                        Some(conn_id),
                        None,
                        label,
                        payload.as_ref(),
                        GrpcEvent::from_ok_stream_close(metadata.clone()),
                    ),
                    Err(status) => emit_event(
                        &res_tx,
                        Some(conn_id),
                        None,
                        label,
                        payload.as_ref(),
                        GrpcEvent::from_status(GrpcEventKind::StreamDisconnected, status),
                    ),
                }
            }
            Err(status) => {
                emit_event(
                    &res_tx,
                    Some(conn_id),
                    req_id,
                    label,
                    payload.as_ref(),
                    GrpcEvent::from_status(GrpcEventKind::StreamConnected, status),
                );
            }
        }

        let _ = lifecycle_tx.send(StreamLifecycle::Closed { stream_name });
    });

    ActiveStream {
        mode: GrpcStreamMode::Bidi,
        sender: Some(tx),
        handle,
    }
}
