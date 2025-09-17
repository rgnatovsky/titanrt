use crate::connector::errors::{StreamError, StreamResult};
use crate::connector::features::grpc::connector::{DEFAULT_CONN_ID, GrpcClients, GrpcConnector};
use crate::connector::features::grpc::stream::actions::{GrpcAction, GrpcCall};
use crate::connector::features::grpc::stream::codec::RawCodec;
use crate::connector::features::grpc::stream::descriptor::GrpcStreamDescriptor;
use crate::connector::features::grpc::stream::event::GrpcEvent;

use crate::connector::features::grpc::stream::RawStreamingCodec;
use crate::connector::features::reqwest::rate_limiter::RateLimitManager;
use crate::connector::{Hook, HookArgs, IntoHook, RuntimeCtx, StreamRunner, StreamSpawner};
use crate::io::ringbuffer::RingSender;
use crate::prelude::{BaseRx, TxPairExt};
use crate::utils::{Reducer, StateMarker};

use ahash::AHashMap;
use anyhow::anyhow;
use crossbeam::channel::unbounded;
use futures::{StreamExt, stream};
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::Mutex;
use tokio::task::LocalSet;
use tokio::time::{sleep, timeout};
use tonic::client::Grpc;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::metadata::MetadataMap;
use tonic::transport::Channel;
use tonic::{Request, Status};

impl<E, R, S> StreamSpawner<GrpcStreamDescriptor, E, R, S> for GrpcConnector
where
    E: TxPairExt,
    S: StateMarker,
    R: Reducer,
{
}

impl<E, R, S> StreamRunner<GrpcStreamDescriptor, E, R, S> for GrpcConnector
where
    E: TxPairExt,
    S: StateMarker,
    R: Reducer,
{
    type Config = AHashMap<u16, GrpcClients>;
    type ActionTx = RingSender<GrpcAction>;
    type RawEvent = GrpcEvent;
    type HookResult = ();

    fn build_config(&mut self, _desc: &GrpcStreamDescriptor) -> anyhow::Result<Self::Config> {
        Ok(self.clients_map().clone())
    }

    fn run<H>(
        mut ctx: RuntimeCtx<
            AHashMap<u16, GrpcClients>,
            GrpcStreamDescriptor,
            RingSender<GrpcAction>,
            E,
            R,
            S,
        >,
        hook: H,
    ) -> StreamResult<()>
    where
        H: IntoHook<GrpcEvent, E, R, S, GrpcStreamDescriptor, ()>,
        E: TxPairExt,
        S: StateMarker,
    {
        let mut hook = hook.into_hook();
        let wait_async_tasks = Duration::from_micros(ctx.desc.wait_async_tasks_us);
        let rl_manager = std::rc::Rc::new(Mutex::new(RateLimitManager::new(
            ctx.desc.rate_limits.clone(),
        )));

        let rt = Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .map_err(|e| StreamError::Unknown(anyhow!("Tokio Runtime error: {e}")))?;
        let local = LocalSet::new();

        let (res_tx, res_rx) = unbounded();
        ctx.health.up();
        // Слушает входящие действия (GrpcAction) через ctx.action_rx.try_recv().
        loop {
            if ctx.cancel.is_cancelled() {
                ctx.health.down();
                break Err(StreamError::Cancelled);
            }

            while let Ok(mut action) = ctx.action_rx.try_recv() {
                // достаёт клиента (GrpcClients по conn_id);
                let clients = action
                    .conn_id
                    .and_then(|id| ctx.cfg.get(&id))
                    .or_else(|| ctx.cfg.get(&DEFAULT_CONN_ID))
                    .cloned();
                let Some(clients) = clients else { continue };

                // capture
                let req_id = action.req_id;
                let label = action.label.take();
                let timeout_opt = action.timeout;
                let rl_ctx = action.rl_ctx.take();
                let rl_weight = action.rl_weight;
                let payload = action.payload.take();
                let md = action.metadata.clone();
                let call = action.call.clone();

                let res_tx = res_tx.clone();
                let rl_mgr = rl_ctx.as_ref().map(|_| rl_manager.clone());
                // спаунит асинхронную задачу на LocalSet.
                local.spawn_local(async move {
                    // выполняет планирование в RateLimitManager.
                    if let Some(mgr) = rl_mgr {
                        let plan = {
                            let mut m = mgr.lock().await;
                            m.plan(rl_ctx.as_ref().unwrap(), rl_weight)
                        };
                        if let Some(plan) = plan {
                            for (b, w) in plan {
                                b.wait(w).await;
                            }
                        }
                    }

                    let mut grpc = Grpc::new(clients.channel.clone());
                    if let Err(e) = grpc.ready().await {
                        let _ = res_tx.try_send(GrpcEvent::from_status(
                            Status::unavailable(format!("channel not ready: {e}")),
                            req_id,
                            label,
                            payload,
                        ));
                        return;
                    }

                    // helper для unary с RawCodec
                    async fn unary_call(
                        grpc: &mut Grpc<Channel>,
                        method: &str,
                        req_body: bytes::Bytes,
                        md: MetadataMap,
                    ) -> Result<(bytes::Bytes, MetadataMap), Status> {
                        let path: PathAndQuery = method
                            .parse()
                            .map_err(|_| Status::invalid_argument("invalid method path"))?;
                        let mut request = Request::new(req_body);
                        *request.metadata_mut() = md;
                        let resp = grpc.unary(request, path, RawCodec).await?;
                        let meta = resp.metadata().clone();
                        Ok((resp.into_inner(), meta))
                    }

                    // helper для server streaming
                    async fn server_stream_call(
                        grpc: &mut Grpc<Channel>,
                        method: &str,
                        req_body: bytes::Bytes,
                        md: MetadataMap,
                        mut on_item: impl FnMut(bytes::Bytes) -> (),
                    ) -> Result<(), Status> {
                        let path: PathAndQuery = method
                            .parse()
                            .map_err(|_| Status::invalid_argument("invalid method path"))?;
                        let mut request = Request::new(req_body);
                        *request.metadata_mut() = md;
                        let mut stream = grpc
                            .server_streaming(request, path, RawCodec)
                            .await?
                            .into_inner();
                        while let Some(item) = stream.next().await {
                            let bytes = item?;
                            on_item(bytes);
                        }
                        Ok(())
                    }

                    // client & bidi streaming — подаём заранее подготовленные фреймы (Vec<Bytes>)
                    async fn client_stream_call(
                        grpc: &mut Grpc<Channel>,
                        method: &str,
                        msgs: Vec<bytes::Bytes>,
                        md: MetadataMap,
                    ) -> Result<(bytes::Bytes, MetadataMap), Status> {
                        let path: PathAndQuery = method
                            .parse()
                            .map_err(|_| Status::invalid_argument("invalid method path"))?;
                        let stream = stream::iter(msgs.into_iter().map(Ok::<_, Status>));
                        let mut request = Request::new(stream);
                        *request.metadata_mut() = md;
                        let resp = grpc
                            .client_streaming(request, path, RawStreamingCodec)
                            .await?;
                        let meta = resp.metadata().clone();
                        Ok((resp.into_inner(), meta))
                    }

                    async fn bidi_stream_call(
                        grpc: &mut Grpc<Channel>,
                        method: &str,
                        msgs: Vec<bytes::Bytes>,
                        md: MetadataMap,
                        mut on_item: impl FnMut(bytes::Bytes) -> (),
                    ) -> Result<(), Status> {
                        let path: PathAndQuery = method
                            .parse()
                            .map_err(|_| Status::invalid_argument("invalid method path"))?;
                        let stream = stream::iter(msgs.into_iter().map(Ok::<_, Status>));
                        let mut request = Request::new(stream);
                        *request.metadata_mut() = md;
                        let resp = grpc.streaming(request, path, RawStreamingCodec).await?;
                        let mut s = resp.into_inner();
                        while let Some(item) = s.next().await {
                            on_item(item?);
                        }
                        Ok(())
                    }

                    let fut = async {
                        match call {
                            GrpcCall::Unary { method, message } => unary_call(
                                &mut grpc, &method, message, md,
                            )
                            .await
                            .map(|(b, meta)| {
                                GrpcEvent::from_ok_unary(b, meta, req_id, label, payload.clone())
                            }),
                            GrpcCall::ServerStreaming { method, message } => {
                                let rtx = res_tx.clone();
                                let payload_stream = payload.clone();
                                server_stream_call(&mut grpc, &method, message, md, move |item| {
                                    // в стримах каждое сообщение пушится в res_tx.
                                    let _ = rtx.send(GrpcEvent::from_ok_stream_item(
                                        item,
                                        req_id,
                                        label,
                                        payload_stream.clone(),
                                    ));
                                })
                                .await
                                .map(|_| {
                                    GrpcEvent::from_ok_unary(
                                        bytes::Bytes::new(),
                                        MetadataMap::new(),
                                        req_id,
                                        label,
                                        payload.clone(),
                                    )
                                })
                            }
                            GrpcCall::ClientStreaming { method, messages } => {
                                client_stream_call(&mut grpc, &method, messages, md)
                                    .await
                                    .map(|(b, meta)| {
                                        GrpcEvent::from_ok_unary(
                                            b,
                                            meta,
                                            req_id,
                                            label,
                                            payload.clone(),
                                        )
                                    })
                            }
                            GrpcCall::BidiStreaming { method, messages } => {
                                let rtx = res_tx.clone();
                                let payload_bidi = payload.clone();
                                bidi_stream_call(&mut grpc, &method, messages, md, move |item| {
                                    let _ = rtx.send(GrpcEvent::from_ok_stream_item(
                                        item,
                                        req_id,
                                        label,
                                        payload_bidi.clone(),
                                    ));
                                })
                                .await
                                .map(|_| {
                                    GrpcEvent::from_ok_unary(
                                        bytes::Bytes::new(),
                                        MetadataMap::new(),
                                        req_id,
                                        label,
                                        payload.clone(),
                                    )
                                })
                            }
                        }
                    };

                    let result = if let Some(t) = timeout_opt {
                        match timeout(t, fut).await {
                            Ok(r) => r,
                            Err(_) => Err(Status::deadline_exceeded("client timeout")),
                        }
                    } else {
                        fut.await
                    };

                    match result {
                        Ok(ev) => {
                            let _ = res_tx.try_send(ev);
                        }
                        Err(status) => {
                            let _ = res_tx
                                .try_send(GrpcEvent::from_status(status, req_id, label, payload));
                        }
                    }
                });
            }

            // доставка в hook
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

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use std::time::Duration;

    use crate::connector::base::BaseConnector;
    use crate::connector::features::grpc::connector::{
        DEFAULT_CONN_ID, GrpcConnector, GrpcConnectorConfig,
    };
    use crate::connector::features::grpc::stream::actions::unary;
    use crate::connector::features::grpc::stream::descriptor::GrpcStreamDescriptor;
    use crate::connector::features::grpc::stream::event::GrpcEvent;

    use crate::io::ringbuffer::RingSender; // <-- тип для E
    use crate::utils::{CancelToken, NullReducer, NullState}; // <-- готовые заглушки

    use crate::connector::{Hook, HookArgs};
    use crate::utils::{Reducer, StateMarker};

    // Простейший Hook: печатаем, что прилетело из раннера
    #[derive(Default, Clone)]
    struct PrintHook;

    impl<R, S> Hook<GrpcEvent, RingSender<GrpcEvent>, R, S, GrpcStreamDescriptor, ()> for PrintHook
    where
        R: Reducer,
        S: StateMarker,
    {
        fn call(
            &mut self,
            args: HookArgs<GrpcEvent, RingSender<GrpcEvent>, R, S, GrpcStreamDescriptor>,
        ) -> () {
            let ev = args.raw;
            if ev.is_stream_item() {
                println!(
                    "[stream item] {} bytes",
                    ev.body_bytes().map(|b| b.len()).unwrap_or(0)
                );
            } else if ev.is_ok() {
                println!(
                    "[final ok] {} bytes",
                    ev.body_bytes().map(|b| b.len()).unwrap_or(0)
                );
            } else {
                println!("[error] {}", ev.maybe_error_msg().unwrap_or_default());
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn grpc_test() -> anyhow::Result<()> {
        // 1) Коннектор -> grpcbin (TLS)
        let cfg = GrpcConnectorConfig {
            default_max_cores: Some(1),
            specific_core_ids: vec![],
            use_core_stats: false,
            endpoints: Some(vec![(DEFAULT_CONN_ID, "http://grpcb.in:9000".to_string())]),
            connect_timeout_ms: Some(10_000),
            http2_keepalive_interval_ms: Some(20_000),
            http2_keepalive_timeout_ms: Some(10_000),
            request_timeout_ms: Some(5_000),
        };
        let cancel = CancelToken::new_root();
        let mut conn = GrpcConnector::init(cfg, cancel.clone(), None)?;

        // 2) Дескриптор
        let desc = GrpcStreamDescriptor::low_latency();

        // 3) Запускаем стрим. ЯВНО указываем типы E, R, S:
        let hook = PrintHook::default();
        let mut stream = conn.spawn_stream::<GrpcStreamDescriptor, RingSender<GrpcEvent>, NullReducer, NullState, PrintHook>(desc.clone(), hook)?;

        // 4) 10 секунд шлём unary Index — через stream.try_send(...)
        let started = tokio::time::Instant::now();
        while started.elapsed() < Duration::from_secs(10) {
            let action = unary("/grpcbin.GRPCBin/Index", Bytes::new())
                .label("smoke")
                .timeout(Duration::from_secs(3))
                .build();

            stream.try_send(action).expect("send action to stream");

            tokio::time::sleep(Duration::from_millis(1000)).await;
        }

        // 5) Остановить стрим
        stream.stop()?;
        Ok(())
    }
}
