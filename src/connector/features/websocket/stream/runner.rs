use std::borrow::Cow;
use std::fmt::Debug;
use std::rc::Rc;
use std::time::Duration;

use crate::connector::hook::Hook;
use ahash::AHashMap;
use anyhow::anyhow;
use bytes::Bytes;
use crossbeam::channel::{Sender, unbounded};
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::runtime::Builder;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::task::{LocalSet, yield_now};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::tungstenite::{
    Message,
    client::IntoClientRequest,
    http::{HeaderValue, Request, header::SEC_WEBSOCKET_PROTOCOL},
    protocol::{CloseFrame, frame::coding::CloseCode},
};
use tracing::{error, warn};
use tungstenite::protocol::WebSocketConfig;
use uuid::Uuid;

use crate::connector::errors::{StreamError, StreamResult};
use crate::connector::features::shared::actions::StreamActionRaw;
use crate::connector::features::shared::clients_map::ClientsMap;
use crate::connector::features::shared::events::StreamEventRaw;
use crate::connector::features::shared::rate_limiter::RateLimitManager;
use crate::connector::features::websocket::client::{WebSocketClient, WebSocketClientSpec};
use crate::connector::features::websocket::connector::WebSocketConnector;
use crate::connector::features::websocket::stream::actions::{
    WebSocketClose, WebSocketCommand, WebSocketConnect,
};
use crate::connector::features::websocket::stream::descriptor::WebSocketStreamDescriptor;
use crate::connector::features::websocket::stream::event::WebSocketEvent;
use crate::connector::features::websocket::stream::message::WebSocketMessage;
use crate::connector::{HookArgs, IntoHook, RuntimeCtx, StreamRunner, StreamSpawner};
use crate::io::ringbuffer::RingSender;
use crate::prelude::{BaseRx, TxPairExt};
use crate::utils::{CancelToken, Reducer, StateMarker};

struct ConnectionHandle {
    cmd_tx: UnboundedSender<WsTaskCommand>,
    client_id: usize,
}

type ConnectionMap = AHashMap<String, ConnectionHandle>;

#[derive(Debug)]
enum WsTaskCommand {
    Send(WebSocketMessage),
    Ping(Bytes),
    Pong(Bytes),
    Close(WebSocketClose),
    Disconnect,
}

impl<E, R, S, T> StreamSpawner<WebSocketStreamDescriptor<T>, E, R, S, T> for WebSocketConnector
where
    Self: StreamRunner<WebSocketStreamDescriptor<T>, E, R, S, T>,
    E: TxPairExt,
    R: Reducer,
    S: StateMarker,
    T: Debug + Clone + Send + 'static,
{
}

impl<E, R, S, T> StreamRunner<WebSocketStreamDescriptor<T>, E, R, S, T> for WebSocketConnector
where
    E: TxPairExt,
    R: Reducer,
    S: StateMarker,
    T: Debug + Clone + Send + 'static,
{
    type Config = ClientsMap<WebSocketClient, WebSocketClientSpec>;
    type ActionTx = RingSender<StreamActionRaw<WebSocketCommand>>;
    type RawEvent = StreamEventRaw<WebSocketEvent>;
    type HookResult = ();

    fn build_config(
        &mut self,
        _desc: &WebSocketStreamDescriptor<T>,
    ) -> anyhow::Result<Self::Config> {
        Ok(self.clients_map())
    }

    fn run<H>(
        mut ctx: RuntimeCtx<
            ClientsMap<WebSocketClient, WebSocketClientSpec>,
            WebSocketStreamDescriptor<T>,
            RingSender<StreamActionRaw<WebSocketCommand>>,
            E,
            R,
            S,
            T,
        >,
        hook: H,
    ) -> StreamResult<()>
    where
        H: IntoHook<StreamEventRaw<WebSocketEvent>, E, R, S, WebSocketStreamDescriptor<T>, (), T>,
    {
        let mut hook = hook.into_hook();
        let wait_async_tasks = Duration::from_micros(ctx.desc.wait_async_tasks_us);
        let mut connections: ConnectionMap = AHashMap::new();
        let (res_tx, res_rx) = unbounded::<StreamEventRaw<WebSocketEvent>>();

        let rl_manager = Rc::new(Mutex::new(RateLimitManager::new(
            ctx.desc.rate_limits.clone(),
        )));

        let rt = Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .map_err(|err| StreamError::Unknown(anyhow!("Tokio runtime error: {err}")))?;
        let local = LocalSet::new();

        ctx.health.up();

        loop {
            if ctx.cancel.is_cancelled() {
                for handle in connections.values() {
                    let _ = handle.cmd_tx.send(WsTaskCommand::Disconnect);
                }
                ctx.health.down();
                break Err(StreamError::Cancelled);
            }

            while let Ok(mut action) = ctx.action_rx.try_recv() {
                let command = match action.inner_take() {
                    Some(cmd) => cmd,
                    None => continue,
                };

                let requested_conn_id = action.conn_id();
                let req_id = action.req_id();
                let mut label = action.label_take();
                let payload = action.json_take();

                let label_key = match label.take() {
                    Some(l) => l.into_owned(),
                    None => {
                        push_event(
                            &res_tx,
                            0,
                            req_id,
                            None,
                            payload.as_ref(),
                            WebSocketEvent::error("ws action requires label"),
                        );
                        continue;
                    }
                };

                let label_for_err = Cow::Owned(label_key.clone());

                match command {
                    WebSocketCommand::Connect(connect_cfg) => {
                        let Some(conn_id) = select_client_id(requested_conn_id, &ctx.cfg) else {
                            push_event(
                                &res_tx,
                                0,
                                req_id,
                                Some(&label_for_err),
                                payload.as_ref(),
                                WebSocketEvent::error("client spec not found"),
                            );
                            continue;
                        };

                        let Some(client) = ctx.cfg.get(&conn_id) else {
                            push_event(
                                &res_tx,
                                conn_id,
                                req_id,
                                Some(&label_for_err),
                                payload.as_ref(),
                                WebSocketEvent::error("client spec not found"),
                            );
                            continue;
                        };

                        {
                            if let Some(existing) = connections.remove(&label_key) {
                                let _ = existing.cmd_tx.send(WsTaskCommand::Disconnect);
                            }
                        }

                        let (cmd_tx, cmd_rx) = unbounded_channel();
                        let session_res_tx = res_tx.clone();
                        let cancel = ctx.cancel.clone();
                        let req_id_local = req_id;
                        let label_string_for_spawn = label_key.clone();
                        let payload_clone = payload.clone();
                        let client_cfg = client.as_ref().clone();

                        local.spawn_local(async move {
                            let run_label: Option<Cow<'static, str>> =
                                Some(Cow::Owned(label_string_for_spawn.clone()));
                            let run_payload = payload_clone.clone();

                            if let Err(err) = run_session(
                                conn_id,
                                client_cfg,
                                connect_cfg,
                                cmd_rx,
                                session_res_tx.clone(),
                                cancel,
                                req_id_local,
                                run_label,
                                run_payload,
                            )
                            .await
                            {
                                let label_cow = Cow::Owned(label_string_for_spawn);
                                push_event(
                                    &session_res_tx,
                                    conn_id,
                                    None,
                                    Some(&label_cow),
                                    payload_clone.as_ref(),
                                    WebSocketEvent::error(format!("session error: {err}")),
                                );
                            }
                        });

                        {
                            connections.insert(
                                label_key.clone(),
                                ConnectionHandle {
                                    cmd_tx,
                                    client_id: conn_id,
                                },
                            );
                        }
                    }
                    WebSocketCommand::Send(message) => {
                        let (cmd_tx, client_id) = {
                            match connections.get(&label_key) {
                                Some(handle) => (handle.cmd_tx.clone(), handle.client_id),
                                None => {
                                    push_event(
                                        &res_tx,
                                        requested_conn_id.unwrap_or(0),
                                        req_id,
                                        Some(&label_for_err),
                                        payload.as_ref(),
                                        WebSocketEvent::error("connection is not established"),
                                    );
                                    continue;
                                }
                            }
                        };

                        let rl_manager_clone = if action.rl_ctx().is_some() {
                            Some(rl_manager.clone())
                        } else {
                            None
                        };
                        let rl_ctx = action.rl_ctx().cloned();
                        let rl_weight = action.rl_weight();
                        let res_tx_clone = res_tx.clone();
                        let label_string_for_spawn = label_key.clone();
                        let payload_clone = payload.clone();

                        local.spawn_local(async move {
                            if let Some(rl_mgr) = rl_manager_clone {
                                if let Some(ctx) = rl_ctx.as_ref() {
                                    if let Some(plan) = {
                                        let mut mgr = rl_mgr.lock().await;
                                        mgr.plan(ctx, rl_weight)
                                    } {
                                        for (bucket, weight) in plan {
                                            bucket.wait(weight).await;
                                        }
                                    }
                                }
                            }

                            if cmd_tx.send(WsTaskCommand::Send(message)).is_err() {
                                let label_cow = Cow::Owned(label_string_for_spawn);
                                push_event(
                                    &res_tx_clone,
                                    client_id,
                                    req_id,
                                    Some(&label_cow),
                                    payload_clone.as_ref(),
                                    WebSocketEvent::error("connection is not available"),
                                );
                            }
                        });
                    }
                    WebSocketCommand::Ping(bytes) => {
                        let (cmd_tx, client_id) = {
                            match connections.get(&label_key) {
                                Some(handle) => (handle.cmd_tx.clone(), handle.client_id),
                                None => {
                                    push_event(
                                        &res_tx,
                                        requested_conn_id.unwrap_or(0),
                                        req_id,
                                        Some(&label_for_err),
                                        payload.as_ref(),
                                        WebSocketEvent::error("connection is not established"),
                                    );
                                    continue;
                                }
                            }
                        };

                        let rl_manager_clone = if action.rl_ctx().is_some() {
                            Some(rl_manager.clone())
                        } else {
                            None
                        };
                        let rl_ctx = action.rl_ctx().cloned();
                        let rl_weight = action.rl_weight();
                        let res_tx_clone = res_tx.clone();
                        let label_string_for_spawn = label_key.clone();
                        let payload_clone = payload.clone();

                        local.spawn_local(async move {
                            if let Some(rl_mgr) = rl_manager_clone {
                                if let Some(ctx) = rl_ctx.as_ref() {
                                    if let Some(plan) = {
                                        let mut mgr = rl_mgr.lock().await;
                                        mgr.plan(ctx, rl_weight)
                                    } {
                                        for (bucket, weight) in plan {
                                            bucket.wait(weight).await;
                                        }
                                    }
                                }
                            }

                            if cmd_tx.send(WsTaskCommand::Ping(bytes)).is_err() {
                                let label_cow = Cow::Owned(label_string_for_spawn);
                                push_event(
                                    &res_tx_clone,
                                    client_id,
                                    req_id,
                                    Some(&label_cow),
                                    payload_clone.as_ref(),
                                    WebSocketEvent::error("connection is not available"),
                                );
                            }
                        });
                    }
                    WebSocketCommand::Pong(bytes) => {
                        let (cmd_tx, client_id) = {
                            match connections.get(&label_key) {
                                Some(handle) => (handle.cmd_tx.clone(), handle.client_id),
                                None => {
                                    push_event(
                                        &res_tx,
                                        requested_conn_id.unwrap_or(0),
                                        req_id,
                                        Some(&label_for_err),
                                        payload.as_ref(),
                                        WebSocketEvent::error("connection is not established"),
                                    );
                                    continue;
                                }
                            }
                        };

                        let rl_manager_clone = if action.rl_ctx().is_some() {
                            Some(rl_manager.clone())
                        } else {
                            None
                        };
                        let rl_ctx = action.rl_ctx().cloned();
                        let rl_weight = action.rl_weight();
                        let res_tx_clone = res_tx.clone();
                        let label_string_for_spawn = label_key.clone();
                        let payload_clone = payload.clone();

                        local.spawn_local(async move {
                            if let Some(rl_mgr) = rl_manager_clone {
                                if let Some(ctx) = rl_ctx.as_ref() {
                                    if let Some(plan) = {
                                        let mut mgr = rl_mgr.lock().await;
                                        mgr.plan(ctx, rl_weight)
                                    } {
                                        for (bucket, weight) in plan {
                                            bucket.wait(weight).await;
                                        }
                                    }
                                }
                            }

                            if cmd_tx.send(WsTaskCommand::Pong(bytes)).is_err() {
                                let label_cow = Cow::Owned(label_string_for_spawn);
                                push_event(
                                    &res_tx_clone,
                                    client_id,
                                    req_id,
                                    Some(&label_cow),
                                    payload_clone.as_ref(),
                                    WebSocketEvent::error("connection is not available"),
                                );
                            }
                        });
                    }
                    WebSocketCommand::Close(close_cfg) => {
                        let (cmd_tx, client_id) = {
                            match connections.get(&label_key) {
                                Some(handle) => (handle.cmd_tx.clone(), handle.client_id),
                                None => {
                                    push_event(
                                        &res_tx,
                                        requested_conn_id.unwrap_or(0),
                                        req_id,
                                        Some(&label_for_err),
                                        payload.as_ref(),
                                        WebSocketEvent::error("connection is not established"),
                                    );
                                    continue;
                                }
                            }
                        };

                        let rl_manager_clone = if action.rl_ctx().is_some() {
                            Some(rl_manager.clone())
                        } else {
                            None
                        };
                        let rl_ctx = action.rl_ctx().cloned();
                        let rl_weight = action.rl_weight();
                        let res_tx_clone = res_tx.clone();
                        let label_string_for_spawn = label_key.clone();
                        let payload_clone = payload.clone();

                        local.spawn_local(async move {
                            if let Some(rl_mgr) = rl_manager_clone {
                                if let Some(ctx) = rl_ctx.as_ref() {
                                    if let Some(plan) = {
                                        let mut mgr = rl_mgr.lock().await;
                                        mgr.plan(ctx, rl_weight)
                                    } {
                                        for (bucket, weight) in plan {
                                            bucket.wait(weight).await;
                                        }
                                    }
                                }
                            }

                            if cmd_tx.send(WsTaskCommand::Close(close_cfg)).is_err() {
                                let label_cow = Cow::Owned(label_string_for_spawn);
                                push_event(
                                    &res_tx_clone,
                                    client_id,
                                    req_id,
                                    Some(&label_cow),
                                    payload_clone.as_ref(),
                                    WebSocketEvent::error("connection is not available"),
                                );
                            }
                        });
                    }
                    WebSocketCommand::Disconnect => {
                        if let Some(handle) = connections.remove(&label_key) {
                            let _ = handle.cmd_tx.send(WsTaskCommand::Disconnect);
                        } else {
                            push_event(
                                &res_tx,
                                0,
                                req_id,
                                Some(&label_for_err),
                                payload.as_ref(),
                                WebSocketEvent::error("connection is not established"),
                            );
                        }
                    }
                }
            }

            let mut budget = ctx.desc.max_hook_calls_at_once;
            while budget > 0 {
                match res_rx.try_recv() {
                    Ok(event) => {
                        budget -= 1;
                        let remove_after = matches!(event.inner(), WebSocketEvent::Closed { .. });
                        let label = event.label().map(|l| l.to_string());
                        hook.call(HookArgs::new(
                            event,
                            &mut ctx.event_tx,
                            &mut ctx.reducer,
                            &ctx.state,
                            &ctx.desc,
                            &ctx.health,
                        ));
                        if remove_after {
                            if let Some(label_key) = label {
                                connections.remove(&label_key);
                            }
                        }
                    }
                    Err(_) => break,
                }
            }

            rt.block_on(local.run_until(async {
                if wait_async_tasks.is_zero() {
                    yield_now().await;
                } else {
                    sleep(wait_async_tasks).await;
                }
            }));
        }
    }
}

fn select_client_id(
    explicit: Option<usize>,
    cfg: &ClientsMap<WebSocketClient, WebSocketClientSpec>,
) -> Option<usize> {
    explicit
        .filter(|id| cfg.contains(id))
        .or_else(|| cfg.sole_entry_id())
}

fn push_event(
    tx: &Sender<StreamEventRaw<WebSocketEvent>>,
    conn_id: usize,
    req_id: Option<Uuid>,
    label: Option<&Cow<'static, str>>,
    payload: Option<&Value>,
    inner: WebSocketEvent,
) {
    let mut builder = StreamEventRaw::builder(Some(inner)).conn_id(Some(conn_id));
    if let Some(req_id) = req_id {
        builder = builder.req_id(Some(req_id));
    }
    if let Some(label) = label {
        builder = builder.label(Some(label.clone()));
    }
    if let Some(payload) = payload {
        builder = builder.payload(Some(payload.clone()));
    }

    match builder.build() {
        Ok(event) => {
            let _ = tx.try_send(event);
        }
        Err(err) => {
            error!(conn_id = conn_id, ?err, "unable to build websocket event");
        }
    }
}

fn build_request(
    client: &WebSocketClient,
    connect: &WebSocketConnect,
) -> anyhow::Result<Request<()>> {
    let url = connect
        .override_url
        .clone()
        .unwrap_or_else(|| client.url.clone());
    let mut request = url.into_client_request()?;
    let headers = request.headers_mut();

    for (name, value) in &client.headers {
        headers.append(name.clone(), value.clone());
    }
    for (name, value) in &connect.extra_headers {
        headers.append(name.clone(), value.clone());
    }

    let mut protocols: Vec<String> = Vec::new();
    protocols.extend(client.protocols.iter().cloned());
    protocols.extend(connect.extra_protocols.iter().cloned());
    if !protocols.is_empty() {
        let joined = protocols.join(", ");
        headers.insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_str(&joined)?);
    }

    Ok(request)
}

fn message_to_ws(message: WebSocketMessage) -> Message {
    match message {
        WebSocketMessage::Text(text) => Message::Text(text),
        WebSocketMessage::Binary(bytes) => Message::Binary(bytes.to_vec()),
    }
}

async fn wait_for_cancel(token: CancelToken) {
    while !token.is_cancelled() {
        sleep(Duration::from_millis(50)).await;
    }
}

async fn run_session(
    conn_id: usize,
    client: WebSocketClient,
    connect: WebSocketConnect,
    mut cmd_rx: tokio::sync::mpsc::UnboundedReceiver<WsTaskCommand>,
    res_tx: Sender<StreamEventRaw<WebSocketEvent>>,
    cancel: CancelToken,
    initial_req_id: Option<Uuid>,
    label: Option<Cow<'static, str>>,
    payload: Option<Value>,
) -> anyhow::Result<()> {
    if client.local_ip.is_some() {
        warn!(conn_id, "local ip binding is not supported yet; ignoring");
    }

    let request = build_request(&client, &connect)?;
    let mut ws_cfg = WebSocketConfig::default();
    ws_cfg.max_message_size = client.max_message_size;

    let connect_future = connect_async_with_config(request, Some(ws_cfg), client.tcp_nodelay);

    let (mut ws_stream, response) = if let Some(connect_deadline) = client.connect_timeout {
        match timeout(connect_deadline, connect_future).await {
            Ok(result) => result?,
            Err(_) => {
                push_event(
                    &res_tx,
                    conn_id,
                    initial_req_id,
                    label.as_ref(),
                    payload.as_ref(),
                    WebSocketEvent::error("connection timed out"),
                );
                return Ok(());
            }
        }
    } else {
        connect_future.await?
    };

    let protocol = response
        .headers()
        .get(SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .map(|s| s.to_string());

    push_event(
        &res_tx,
        conn_id,
        initial_req_id,
        label.as_ref(),
        payload.as_ref(),
        WebSocketEvent::connected(response.status(), protocol, response.headers().clone()),
    );

    for message in connect.initial_messages {
        if let Err(err) = ws_stream.send(message_to_ws(message)).await {
            push_event(
                &res_tx,
                conn_id,
                None,
                label.as_ref(),
                payload.as_ref(),
                WebSocketEvent::error(format!("failed sending initial message: {err}")),
            );
            break;
        }
    }

    let mut closed_emitted = false;

    loop {
        tokio::select! {
            _ = wait_for_cancel(cancel.clone()) => {
                let _ = ws_stream.close(None).await;
                break;
            }
            maybe_cmd = cmd_rx.recv() => {
                match maybe_cmd {
                    Some(WsTaskCommand::Send(message)) => {
                        if let Err(err) = ws_stream.send(message_to_ws(message)).await {
                            push_event(
                                &res_tx,
                                conn_id,
                                None,
                                label.as_ref(),
                                payload.as_ref(),
                                WebSocketEvent::error(format!("send failed: {err}")),
                            );
                            break;
                        }
                    }
                    Some(WsTaskCommand::Ping(bytes)) => {
                        if let Err(err) = ws_stream.send(Message::Ping(bytes.to_vec())).await {
                            push_event(
                                &res_tx,
                                conn_id,
                                None,
                                label.as_ref(),
                                payload.as_ref(),
                                WebSocketEvent::error(format!("ping failed: {err}")),
                            );
                            break;
                        }
                    }
                    Some(WsTaskCommand::Pong(bytes)) => {
                        if let Err(err) = ws_stream.send(Message::Pong(bytes.to_vec())).await {
                            push_event(
                                &res_tx,
                                conn_id,
                                None,
                                label.as_ref(),
                                payload.as_ref(),
                                WebSocketEvent::error(format!("pong failed: {err}")),
                            );
                            break;
                        }
                    }
                    Some(WsTaskCommand::Close(close_cfg)) => {
                        let frame = CloseFrame {
                            code: close_cfg
                                .code
                                .map(CloseCode::from)
                                .unwrap_or(CloseCode::Normal),
                            reason: close_cfg.reason.unwrap_or_default().into(),
                        };
                        if let Err(err) = ws_stream.send(Message::Close(Some(frame))).await {
                            push_event(
                                &res_tx,
                                conn_id,
                                None,
                                label.as_ref(),
                                payload.as_ref(),
                                WebSocketEvent::error(format!("close send failed: {err}")),
                            );
                            break;
                        }
                    }
                    Some(WsTaskCommand::Disconnect) => {
                        let _ = ws_stream.close(None).await;
                        break;
                    }
                    None => {
                        let _ = ws_stream.close(None).await;
                        break;
                    }
                }
            }
            incoming = ws_stream.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        push_event(
                            &res_tx,
                            conn_id,
                            None,
                            label.as_ref(),
                            payload.as_ref(),
                            WebSocketEvent::Message(WebSocketMessage::Text(text)),
                        );
                    }
                    Some(Ok(Message::Binary(data))) => {
                        push_event(
                            &res_tx,
                            conn_id,
                            None,
                            label.as_ref(),
                            payload.as_ref(),
                            WebSocketEvent::Message(WebSocketMessage::Binary(Bytes::from(data))),
                        );
                    }
                    Some(Ok(Message::Ping(data))) => {
                        push_event(
                            &res_tx,
                            conn_id,
                            None,
                            label.as_ref(),
                            payload.as_ref(),
                            WebSocketEvent::Ping(Bytes::from(data)),
                        );
                    }
                    Some(Ok(Message::Pong(data))) => {
                        push_event(
                            &res_tx,
                            conn_id,
                            None,
                            label.as_ref(),
                            payload.as_ref(),
                            WebSocketEvent::Pong(Bytes::from(data)),
                        );
                    }
                    Some(Ok(Message::Close(frame))) => {
                        let (code, reason) = frame
                            .map(|frame| (Some(frame.code.into()), Some(frame.reason.into_owned())))
                            .unwrap_or((None, None));
                        push_event(
                            &res_tx,
                            conn_id,
                            None,
                            label.as_ref(),
                            payload.as_ref(),
                            WebSocketEvent::closed(code, reason),
                        );
                        closed_emitted = true;
                        break;
                    }
                    Some(Ok(Message::Frame(_))) => {}
                    Some(Err(err)) => {
                        push_event(
                            &res_tx,
                            conn_id,
                            None,
                            label.as_ref(),
                            payload.as_ref(),
                            WebSocketEvent::error(format!("receive failed: {err}")),
                        );
                        break;
                    }
                    None => {
                        push_event(
                            &res_tx,
                            conn_id,
                            None,
                            label.as_ref(),
                            payload.as_ref(),
                            WebSocketEvent::closed(None, Some("stream ended".to_string())),
                        );
                        closed_emitted = true;
                        break;
                    }
                }
            }
        }
    }

    if !closed_emitted {
        push_event(
            &res_tx,
            conn_id,
            None,
            label.as_ref(),
            payload.as_ref(),
            WebSocketEvent::closed(None, None),
        );
    }

    Ok(())
}
