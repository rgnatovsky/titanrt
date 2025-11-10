use std::borrow::Cow;
use std::fmt::Debug;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use crate::connector::hook::Hook;
use ahash::AHashMap;
use anyhow::anyhow;
use bytes::Bytes;
use crossbeam::channel::{Sender, unbounded};
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::{TcpSocket, TcpStream};
use tokio::runtime::Builder;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::task::{LocalSet, yield_now};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::client_async_with_config;
use tokio_tungstenite::tungstenite::{
    Message,
    client::IntoClientRequest,
    http::{
        HeaderValue,
        header::{HOST, SEC_WEBSOCKET_PROTOCOL},
    },
};
use uuid::Uuid;

use crate::connector::errors::{StreamError, StreamResult};
use crate::connector::features::shared::actions::StreamActionRaw;
use crate::connector::features::shared::clients_map::ClientsMap;
use crate::connector::features::shared::events::StreamEventRaw;
use crate::connector::features::shared::rate_limiter::RateLimitManager;
use crate::connector::features::websocket::client::{WebSocketClient, WebSocketClientSpec};
use crate::connector::features::websocket::connector::WebSocketConnector;
use crate::connector::features::websocket::stream::actions::{WebSocketCommand, WebSocketConnect};
use crate::connector::features::websocket::stream::descriptor::WebSocketStreamDescriptor;
use crate::connector::features::websocket::stream::event::WebSocketEvent;
use crate::connector::features::websocket::stream::message::WebSocketMessage;
use crate::connector::{HookArgs, IntoHook, RuntimeCtx, StreamRunner, StreamSpawner};
use crate::io::ringbuffer::RingSender;
use crate::prelude::{BaseRx, TxPairExt};
use crate::utils::{CancelToken, Reducer, StateMarker};
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

struct ConnectionHandle {
    cmd_tx: UnboundedSender<(WsTaskCommand, Option<Uuid>, Option<Bytes>, Option<usize>)>,
    client_id: usize,
}

type ConnectionMap = AHashMap<String, ConnectionHandle>;

#[derive(Debug)]
enum WsTaskCommand {
    Send(WebSocketMessage),
    Ping(Bytes),
    Pong(Bytes),
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

        let rl_manager = Arc::new(Mutex::new(RateLimitManager::new(
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
                    let _ = handle
                        .cmd_tx
                        .send((WsTaskCommand::Disconnect, None, None, None));
                }
                ctx.health.down();
                break Err(StreamError::Cancelled);
            }

            while let Ok(mut action) = ctx.action_rx.try_recv() {
                let command = match action.inner_take() {
                    Some(cmd) => cmd,
                    None => {
                        send_error_event(
                            &res_tx,
                            &mut action,
                            "ws action requires inner command",
                            None,
                        );
                        continue;
                    }
                };

                match action.label() {
                    Some(_) => {}
                    None => {
                        send_error_event(&res_tx, &mut action, "ws action requires label", None);
                        continue;
                    }
                };

                match command {
                    WebSocketCommand::Connect(connect_cfg) => {
                        let Some(conn_id) = action
                            .conn_id()
                            .filter(|id| ctx.cfg.contains(id))
                            .or_else(|| ctx.cfg.default_id())
                            .or_else(|| ctx.cfg.sole_entry_id())
                        else {
                            send_error_event(
                                &res_tx,
                                &mut action,
                                "ws connection is not configured",
                                None,
                            );
                            continue;
                        };

                        let Some(client) = ctx.cfg.get(&conn_id) else {
                            send_error_event(
                                &res_tx,
                                &mut action,
                                "client spec not found",
                                Some(conn_id),
                            );
                            continue;
                        };

                        {
                            if let Some(existing) = connections.remove(action.label().unwrap()) {
                                tracing::info!(
                                    "disconnecting existing connection for conn_id <{}> with label <{:?}>",
                                    conn_id,
                                    action.label().unwrap()
                                );
                                let _ = existing.cmd_tx.send((
                                    WsTaskCommand::Disconnect,
                                    None,
                                    None,
                                    None,
                                ));
                            }
                        }

                        let (cmd_tx, cmd_rx) = unbounded_channel();
                        let client_cfg = client.clone();
                        let cancel = ctx.cancel.clone();
                        let session_res_tx = res_tx.clone();
                        let error_res_tx = res_tx.clone();
                        let rl_manager_clone = rl_manager.clone();
                        let label_clone = action.label().unwrap().to_string();

                        local.spawn_local(async move {
                            let label_cow = action.label_take();
                            let payload = action.json_take();
                            if let Err(err) = run_session(
                                conn_id,
                                client_cfg,
                                connect_cfg,
                                cmd_rx,
                                session_res_tx,
                                cancel,
                                rl_manager_clone,
                                action.req_id(),
                                label_cow.clone(),
                                payload.clone(),
                            )
                            .await
                            {
                                error_res_tx
                                    .try_send(
                                        StreamEventRaw::builder(Some(WebSocketEvent::error(
                                            format!("connection failed internally: {err}"),
                                        )))
                                        .conn_id(Some(conn_id))
                                        .req_id(action.req_id())
                                        .label(label_cow.clone())
                                        .payload(payload.clone())
                                        .build()
                                        .unwrap(),
                                    )
                                    .ok();

                                error_res_tx
                                    .try_send(
                                        StreamEventRaw::builder(Some(WebSocketEvent::closed(
                                            None,
                                            Some(format!("connection failed internally")),
                                        )))
                                        .conn_id(Some(conn_id))
                                        .req_id(action.req_id())
                                        .label(label_cow)
                                        .payload(payload)
                                        .build()
                                        .unwrap(),
                                    )
                                    .ok();
                            }
                        });

                        connections.insert(
                            label_clone,
                            ConnectionHandle {
                                cmd_tx,
                                client_id: conn_id,
                            },
                        );
                    }
                    WebSocketCommand::Send(message) => {
                        match connections.get(action.label().unwrap()) {
                            Some(handle) => {
                                if handle
                                    .cmd_tx
                                    .send((
                                        WsTaskCommand::Send(message),
                                        action.req_id(),
                                        action.rl_ctx_take(),
                                        action.rl_weight(),
                                    ))
                                    .is_err()
                                {
                                    send_error_event(
                                        &res_tx,
                                        &mut action,
                                        "cmd channel is not available",
                                        Some(handle.client_id),
                                    );
                                }
                            }
                            None => {
                                send_error_event(
                                    &res_tx,
                                    &mut action,
                                    "connection is not established",
                                    None,
                                );
                            }
                        };
                    }
                    WebSocketCommand::Ping(bytes) => {
                        match connections.get(action.label().unwrap()) {
                            Some(handle) => {
                                if handle
                                    .cmd_tx
                                    .send((
                                        WsTaskCommand::Ping(bytes),
                                        action.req_id(),
                                        action.rl_ctx_take(),
                                        action.rl_weight(),
                                    ))
                                    .is_err()
                                {
                                    send_error_event(
                                        &res_tx,
                                        &mut action,
                                        "cmd channel is not available",
                                        Some(handle.client_id),
                                    );
                                }
                            }
                            None => {
                                send_error_event(
                                    &res_tx,
                                    &mut action,
                                    "connection is not established",
                                    None,
                                );
                            }
                        };
                    }
                    WebSocketCommand::Pong(bytes) => {
                        match connections.get(action.label().unwrap()) {
                            Some(handle) => {
                                if handle
                                    .cmd_tx
                                    .send((
                                        WsTaskCommand::Pong(bytes),
                                        action.req_id(),
                                        action.rl_ctx_take(),
                                        action.rl_weight(),
                                    ))
                                    .is_err()
                                {
                                    send_error_event(
                                        &res_tx,
                                        &mut action,
                                        "cmd channel is not available",
                                        Some(handle.client_id),
                                    );
                                }
                            }
                            None => {
                                send_error_event(
                                    &res_tx,
                                    &mut action,
                                    "connection is not established",
                                    None,
                                );
                            }
                        };
                    }
                    WebSocketCommand::Disconnect => {
                        match connections.get(action.label().unwrap()) {
                            Some(handle) => {
                                if handle
                                    .cmd_tx
                                    .send((WsTaskCommand::Disconnect, action.req_id(), None, None))
                                    .is_err()
                                {
                                    send_error_event(
                                        &res_tx,
                                        &mut action,
                                        "cmd channel is not available",
                                        Some(handle.client_id),
                                    );
                                }
                            }
                            None => {
                                send_error_event(
                                    &res_tx,
                                    &mut action,
                                    "connection is not established",
                                    None,
                                );
                            }
                        };
                    }
                }
            }

            let mut budget = ctx.desc.max_hook_calls_at_once;

            while budget > 0 {
                match res_rx.try_recv() {
                    Ok(event) => {
                        budget -= 1;
                        if matches!(event.inner(), WebSocketEvent::Closed { .. }) {
                            tracing::info!(
                                "websocket connection closed for conn_id <{}> with label <{:?}> ",
                                event.conn_id().unwrap(),
                                event.label().unwrap(),
                            );
                            connections.remove(event.label().unwrap());
                        }

                        hook.call(HookArgs::new(
                            event,
                            &mut ctx.event_tx,
                            &mut ctx.reducer,
                            &ctx.state,
                            &ctx.desc,
                            &ctx.health,
                        ));
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

fn send_error_event<E: Into<String>>(
    res_tx: &Sender<StreamEventRaw<WebSocketEvent>>,
    action: &mut StreamActionRaw<WebSocketCommand>,
    error_message: E,
    conn_id: Option<usize>,
) {
    let error_message = error_message.into();

    tracing::error!("websocket error event: {error_message}");

    match res_tx.try_send(
        StreamEventRaw::builder(Some(WebSocketEvent::error(error_message)))
            .conn_id(conn_id.or(action.conn_id()))
            .req_id(action.req_id())
            .label(action.label_take())
            .payload(action.json_take())
            .build()
            .unwrap(),
    ) {
        Ok(_) => (),
        Err(err) => {
            tracing::error!(
                "failed to send websocket error event for action: {:?}: {err}",
                action
            );
        }
    }
}

async fn run_session(
    conn_id: usize,
    client: Arc<WebSocketClient>,
    connect: WebSocketConnect,
    mut cmd_rx: tokio::sync::mpsc::UnboundedReceiver<(
        WsTaskCommand,
        Option<Uuid>,
        Option<Bytes>,
        Option<usize>,
    )>,
    res_tx: Sender<StreamEventRaw<WebSocketEvent>>,
    cancel: CancelToken,
    rl_manager: Arc<Mutex<RateLimitManager>>,
    initial_req_id: Option<Uuid>,
    label: Option<Cow<'static, str>>,
    payload: Option<Value>,
) -> anyhow::Result<()> {
    let ws_cfg = client.ws_config.clone();

    let url = connect
        .override_url
        .clone()
        .unwrap_or_else(|| client.url.clone());

    // Строим HTTP request для WebSocket
    let mut request = url.clone().into_client_request()?;
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
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("URL must have a host"))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| anyhow!("URL must have a port"))?;

    let host_header_value = if port == 80 || port == 443 {
        host.to_string()
    } else {
        format!("{}:{}", host, port)
    };
    headers.insert(HOST, HeaderValue::from_str(&host_header_value)?);

    tracing::debug!(
        "websocket request for conn_id <{}> with label <{:?}>: {} {}",
        conn_id,
        label.as_ref(),
        request.method(),
        request.uri()
    );

    let server_addr = format!("{}:{}", host, port);

    let addr: SocketAddr = if let Ok(ip) = host.parse::<IpAddr>() {
        SocketAddr::new(ip, port)
    } else {
        tokio::net::lookup_host(&server_addr)
            .await
            .map_err(|e| anyhow!("DNS lookup failed for {}: {}", server_addr, e))?
            .next()
            .ok_or_else(|| anyhow!("No addresses found for {}", server_addr))?
    };

    tracing::info!(
        "step 1/3: establishing TCP connection to {} (addr: {}) for conn_id <{}> with label <{:?}>",
        server_addr,
        addr,
        conn_id,
        label.as_ref()
    );

    let tcp_stream = if let Some(local_ip) = client.local_ip {
        let local_addr = SocketAddr::new(local_ip, 0);
        let socket = if addr.is_ipv4() {
            TcpSocket::new_v4()?
        } else {
            TcpSocket::new_v6()?
        };
        socket
            .bind(local_addr)
            .map_err(|e| anyhow!("Failed to bind to local address {}: {}", local_ip, e))?;
        socket
            .connect(addr)
            .await
            .map_err(|e| anyhow!("Failed to connect to {}: {}", server_addr, e))?
    } else {
        TcpStream::connect(addr)
            .await
            .map_err(|e| anyhow!("Failed to connect to {}: {}", server_addr, e))?
    };

    if client.tcp_nodelay {
        tcp_stream
            .set_nodelay(true)
            .map_err(|e| anyhow!("Failed to set TCP_NODELAY: {}", e))?;
    }

    tracing::info!(
        "step 1/3: TCP connection established to {} (addr: {}) for conn_id <{}> with label <{:?}>",
        server_addr,
        addr,
        conn_id,
        label.as_ref()
    );

    // Для wss:// нужно установить TLS соединение
    let stream: tokio_tungstenite::MaybeTlsStream<TcpStream> = if url.scheme() == "wss" {
        tracing::info!(
            "step 2/3: establishing TLS handshake for conn_id <{}> with label <{:?}> to {}",
            conn_id,
            label.as_ref(),
            server_addr
        );

        let mut root_cert_store = RootCertStore::empty();
        for cert in rustls_native_certs::load_native_certs()
            .map_err(|e| anyhow!("Failed to load native certificates: {}", e))?
        {
            root_cert_store
                .add(cert)
                .map_err(|e| anyhow!("Failed to add certificate: {}", e))?;
        }

        let config = Arc::new(
            ClientConfig::builder()
                .with_root_certificates(root_cert_store)
                .with_no_client_auth(),
        );

        let connector = TlsConnector::from(config);
        let domain = ServerName::try_from(host.to_string())
            .map_err(|_| anyhow!("Invalid server name: {}", host))?;

        let tls_stream = connector.connect(domain, tcp_stream).await.map_err(|e| {
            tracing::error!(
                "step 2/3: TLS handshake failed for conn_id <{}> with label <{:?}> to {}: {}",
                conn_id,
                label.as_ref(),
                server_addr,
                e
            );
            anyhow!("TLS handshake failed: {}", e)
        })?;

        tracing::info!(
            "step 2/3: TLS handshake completed for conn_id <{}> with label <{:?}> to {}",
            conn_id,
            label.as_ref(),
            server_addr
        );

        tokio_tungstenite::MaybeTlsStream::Rustls(tls_stream)
    } else {
        tokio_tungstenite::MaybeTlsStream::Plain(tcp_stream)
    };

    tracing::info!(
        "step 3/3: establishing WebSocket handshake for conn_id <{}> with label <{:?}> to {}",
        conn_id,
        label.as_ref(),
        server_addr
    );

    // Используем построенный request с заголовками вместо url
    let connect_future = client_async_with_config(request, stream, Some(ws_cfg));

    let (mut ws_stream, response) = if let Some(connect_deadline) = client.connect_timeout {
        match timeout(connect_deadline, connect_future).await {
            Ok(result) => match result {
                Ok((stream, resp)) => {
                    tracing::info!(
                        "step 3/3: WebSocket handshake completed for conn_id <{}> with label <{:?}> to {}: status={}",
                        conn_id,
                        label.as_ref(),
                        server_addr,
                        resp.status()
                    );
                    (stream, resp)
                }
                Err(e) => {
                    tracing::error!(
                        "step 3/3: WebSocket handshake failed for conn_id <{}> with label <{:?}> to {}: {}",
                        conn_id,
                        label.as_ref(),
                        server_addr,
                        e
                    );
                    return Err(anyhow!("failed to connect to websocket: {e}"));
                }
            },
            Err(_) => {
                tracing::warn!(
                    "websocket connection timed out for conn_id <{}> with label <{:?}> to {}",
                    conn_id,
                    label.as_ref(),
                    server_addr
                );
                return Err(anyhow!("connection timed out"));
            }
        }
    } else {
        match connect_future.await {
            Ok((stream, resp)) => {
                tracing::info!(
                    "step 3/3: WebSocket handshake completed for conn_id <{}> with label <{:?}> to {}: status={}",
                    conn_id,
                    label.as_ref(),
                    server_addr,
                    resp.status()
                );
                (stream, resp)
            }
            Err(e) => {
                tracing::error!(
                    "step 3/3: WebSocket handshake failed for conn_id <{}> with label <{:?}> to {}: {}",
                    conn_id,
                    label.as_ref(),
                    server_addr,
                    e
                );
                return Err(anyhow!("failed to connect to websocket: {e}"));
            }
        }
    };

    let protocol = response
        .headers()
        .get(SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .map(|s| s.to_string());

    let _ = res_tx.try_send(
        StreamEventRaw::builder(Some(WebSocketEvent::connected(
            response.status(),
            protocol,
            response.headers().clone(),
        )))
        .conn_id(Some(conn_id))
        .req_id(initial_req_id)
        .label(label.clone())
        .payload(payload.as_ref().cloned())
        .build()
        .unwrap(),
    );

    for message in connect.initial_messages {
        let message_to_ws = match message {
            WebSocketMessage::Text(text) => Message::Text(text),
            WebSocketMessage::Binary(bytes) => Message::Binary(bytes.to_vec()),
        };
        ws_stream.send(message_to_ws).await?;
    }

    let conn_id_opt = Some(conn_id);
    let label_clone = label.clone();
    let payload_clone = payload.clone();

    let mut closed_emitted = false;

    tracing::info!(
        "running websocket stream session for conn_id <{}> with label <{:?}> to {}",
        conn_id,
        label_clone.as_ref(),
        server_addr
    );

    loop {
        tokio::select! {
            _ = async {
                while !cancel.is_cancelled() {
                    sleep(Duration::from_millis(1000)).await;
                }
            } => {
                let _ = ws_stream.close(None).await;
                tracing::info!(
                    "cancelled websocket stream session for conn_id <{}> with label <{:?}> to {}",
                    conn_id,
                    label_clone.as_ref(),
                    server_addr
                );
                break;
            }
            maybe_cmd = cmd_rx.recv() => {
                match maybe_cmd {
                    Some((WsTaskCommand::Send(message), req_id, rl_ctx, rl_weight)) => {
                        // Обработка rate limiting перед отправкой
                        if let Some(ctx) = rl_ctx.as_ref() {
                            if let Some(plan) = {
                                let mut mgr = rl_manager.lock().await;
                                mgr.plan(ctx, rl_weight)
                            } {
                                for (bucket, weight) in plan {
                                    bucket.wait(weight).await;
                                }
                            }
                        }

                        let message_to_ws = match message {
                            WebSocketMessage::Text(text) => Message::Text(text),
                            WebSocketMessage::Binary(bytes) => Message::Binary(bytes.to_vec()),
                        };
                        if ws_stream.send(message_to_ws).await.is_err() {
                            tracing::warn!(
                                "websocket send failed for conn_id <{}> with label <{:?}> to {}",
                                conn_id,
                                label_clone.as_ref(),
                                server_addr
                            );
                            let _ = res_tx.try_send(
                                StreamEventRaw::builder(Some(WebSocketEvent::error(
                                    "send failed"
                                )))
                                .conn_id(conn_id_opt)
                                .req_id(req_id)
                                .label(label_clone.clone())
                                .payload(payload_clone.clone())
                                .build()
                                .unwrap(),
                            );
                        }
                    }
                    Some((WsTaskCommand::Ping(bytes), req_id, rl_ctx, rl_weight)) => {
                        // Обработка rate limiting перед отправкой
                        if let Some(ctx) = rl_ctx.as_ref() {
                            if let Some(plan) = {
                                let mut mgr = rl_manager.lock().await;
                                mgr.plan(ctx, rl_weight)
                            } {
                                for (bucket, weight) in plan {
                                    bucket.wait(weight).await;
                                }
                            }
                        }

                        if ws_stream.send(Message::Ping(bytes.to_vec())).await.is_err() {
                            tracing::warn!(
                                "websocket ping failed for conn_id <{}> with label <{:?}> to {}",
                                conn_id,
                                label_clone.as_ref(),
                                server_addr
                            );
                            let _ = res_tx.try_send(
                                StreamEventRaw::builder(Some(WebSocketEvent::error(
                                    "ping failed"
                                )))
                                .conn_id(conn_id_opt)
                                .req_id(req_id)
                                .label(label_clone.clone())
                                .payload(payload_clone.clone())
                                .build()
                                .unwrap(),
                            );

                        }
                    }
                    Some((WsTaskCommand::Pong(bytes), req_id, rl_ctx, rl_weight)) => {
                        // Обработка rate limiting перед отправкой
                        if let Some(ctx) = rl_ctx.as_ref() {
                            if let Some(plan) = {
                                let mut mgr = rl_manager.lock().await;
                                mgr.plan(ctx, rl_weight)
                            } {
                                for (bucket, weight) in plan {
                                    bucket.wait(weight).await;
                                }
                            }
                        }

                        if ws_stream.send(Message::Pong(bytes.to_vec())).await.is_err() {
                            tracing::warn!(
                                "websocket pong failed for conn_id <{}> with label <{:?}> to {}",
                                conn_id,
                                label_clone.as_ref(),
                                server_addr
                            );
                            let _ = res_tx.try_send(
                                StreamEventRaw::builder(Some(WebSocketEvent::error(
                                    "pong failed"
                                )))
                                .conn_id(conn_id_opt)
                                .req_id(req_id)
                                .label(label_clone.clone())
                                .payload(payload_clone.clone())
                                .build()
                                .unwrap(),
                            );
                        }
                    }
                    Some((WsTaskCommand::Disconnect, req_id, _, _)) => {
                        tracing::info!(
                            "websocket disconnect command received for conn_id <{}> with label <{:?}> to {}",
                            conn_id,
                            label_clone.as_ref(),
                            server_addr
                        );
                        let _ = res_tx.try_send(
                            StreamEventRaw::builder(Some(WebSocketEvent::closed(None, None)))
                                .conn_id(conn_id_opt)
                                .req_id(req_id)
                                .label(label_clone.clone())
                                .payload(payload_clone.clone())
                                .build()
                                .unwrap(),
                        );
                        closed_emitted = true;
                        break;
                    }
                    None => {
                        closed_emitted = true;
                        break;
                    }
                }
            }
            incoming = ws_stream.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        let _ = res_tx.try_send(
                            StreamEventRaw::builder(Some(WebSocketEvent::Message(
                                WebSocketMessage::Text(text),
                            )))
                            .conn_id(conn_id_opt)
                            .label(label_clone.clone())
                            .payload(payload_clone.clone())
                            .build()
                            .unwrap(),
                        );
                    }
                    Some(Ok(Message::Binary(data))) => {
                        let _ = res_tx.try_send(
                            StreamEventRaw::builder(Some(WebSocketEvent::Message(
                                WebSocketMessage::Binary(Bytes::from(data)),
                            )))
                            .conn_id(conn_id_opt)
                            .label(label_clone.clone())
                            .payload(payload_clone.clone())
                            .build()
                            .unwrap(),
                        );
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = res_tx.try_send(
                            StreamEventRaw::builder(Some(WebSocketEvent::Ping(Bytes::from(data))))
                                .conn_id(conn_id_opt)
                                .label(label_clone.clone())
                                .payload(payload_clone.clone())
                                .build()
                                .unwrap(),
                        );
                    }
                    Some(Ok(Message::Pong(data))) => {
                        let _ = res_tx.try_send(
                            StreamEventRaw::builder(Some(WebSocketEvent::Pong(Bytes::from(data))))
                                .conn_id(conn_id_opt)
                                .label(label_clone.clone())
                                .payload(payload_clone.clone())
                                .build()
                                .unwrap(),
                        );
                    }
                    Some(Ok(Message::Close(frame))) => {
                        let (code, reason) = frame
                            .map(|frame| (Some(frame.code.into()), Some(frame.reason.into_owned())))
                            .unwrap_or((None, None));
                        tracing::info!(
                            "websocket close frame received for conn_id <{}> with label <{:?}> to {}: code={:?}, reason={:?}",
                            conn_id,
                            label_clone.as_ref(),
                            server_addr,
                            code,
                            reason
                        );
                        let _ = res_tx.try_send(
                            StreamEventRaw::builder(Some(WebSocketEvent::closed(code, reason)))
                                .conn_id(conn_id_opt)
                                .label(label_clone.clone())
                                .payload(payload_clone.clone())
                                .build()
                                .unwrap(),
                        );
                        closed_emitted = true;
                        break;
                    }
                    Some(Ok(Message::Frame(_))) => {}
                    Some(Err(_)) => {
                        tracing::warn!(
                            "websocket receive failed for conn_id <{}> with label <{:?}> to {}",
                            conn_id,
                            label_clone.as_ref(),
                            server_addr
                        );
                        let _ = res_tx.try_send(
                            StreamEventRaw::builder(Some(WebSocketEvent::error(
                                "receive failed"
                            )))
                            .conn_id(conn_id_opt)
                            .label(label_clone.clone())
                            .payload(payload_clone.clone())
                            .build()
                            .unwrap(),
                        );
                        break;
                    }
                    None => {
                        tracing::info!(
                            "websocket stream ended with None for conn_id <{}> with label <{:?}> to {}",
                            conn_id,
                            label_clone.as_ref(),
                            server_addr
                        );
                        let _ = res_tx.try_send(
                            StreamEventRaw::builder(Some(WebSocketEvent::closed(
                                None,
                                Some("stream ended".to_string()),
                            )))
                            .conn_id(conn_id_opt)
                            .label(label_clone.clone())
                            .payload(payload_clone.clone())
                            .build()
                            .unwrap(),
                        );
                        closed_emitted = true;
                        break;
                    }
                }
            }
        }
    }

    if !closed_emitted {
        let _ = res_tx.try_send(
            StreamEventRaw::builder(Some(WebSocketEvent::closed(None, None)))
                .conn_id(Some(conn_id))
                .req_id(None)
                .label(label.as_ref().map(|l| l.clone()))
                .payload(payload.as_ref().cloned())
                .build()
                .unwrap(),
        );
    }

    ws_stream.close(None).await.ok();

    tracing::debug!(
        "websocket stream session ended for conn_id <{}> with label <{:?}> to {}",
        conn_id,
        label.as_ref(),
        server_addr
    );

    Ok(())
}
