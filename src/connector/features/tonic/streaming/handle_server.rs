use crate::connector::features::shared::events::StreamEvent;
use crate::connector::features::shared::rate_limiter::RateLimitManager;
use crate::connector::features::tonic::codec::RawCodec;
use crate::connector::features::tonic::streaming::StreamingMode;
use crate::connector::features::tonic::streaming::actions::ConnectConfig;
use crate::connector::features::tonic::streaming::event::StreamingEvent;
use crate::connector::features::tonic::streaming::utils::{
    ActiveStream, StreamContext, StreamLifecycle, emit_event,
};

use bytes::Bytes;
use crossbeam::channel::Sender;
use futures::StreamExt;
use std::borrow::Cow;
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::LocalSet;
use tonic::client::Grpc;
use tonic::transport::Channel;
use tonic::{Request, Status};

pub fn start_server_stream(
    connect: ConnectConfig,
    conn_id: usize,
    label: Option<Cow<'static, str>>,
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
) -> ActiveStream {
    let ConnectConfig {
        mode: _,
        method,
        initial_message,
        metadata,
    } = connect;

    let initial_payload = initial_message.unwrap_or_else(Bytes::new);

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
                StreamingEvent::from_status(Status::unavailable(format!(
                    "[TonicStream - Runner] channel not ready with error: {e}",
                ))),
            );
            let _ = lifecycle_tx.send(StreamLifecycle::Closed { stream_name });
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
                        Some(conn_id),
                        req_id,
                        label,
                        payload.as_ref(),
                        StreamingEvent::from_status(Status::deadline_exceeded("connect timeout")),
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
                                req_id,
                                label.clone(),
                                payload.as_ref(),
                                StreamingEvent::from_ok_stream_item(bytes),
                            );
                        }
                        Err(status) => {
                            emit_event(
                                &res_tx,
                                Some(conn_id),
                                req_id,
                                label.clone(),
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
                        Some(conn_id),
                        req_id,
                        label,
                        payload.as_ref(),
                        StreamingEvent::from_ok_stream_close(trailers),
                    ),
                    Ok(None) => emit_event(
                        &res_tx,
                        Some(conn_id),
                        req_id,
                        label,
                        payload.as_ref(),
                        StreamingEvent::from_ok_stream_close(metadata.clone()),
                    ),
                    Err(status) => emit_event(
                        &res_tx,
                        Some(conn_id),
                        req_id,
                        label,
                        payload.as_ref(),
                        StreamingEvent::from_status(status),
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
                    StreamingEvent::from_status(status),
                );
            }
        }

        let _ = lifecycle_tx.send(StreamLifecycle::Closed { stream_name });
    });

    ActiveStream {
        mode: StreamingMode::Server,
        sender: None,
        handle,
    }
}
