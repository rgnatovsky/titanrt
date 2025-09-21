use crate::connector::errors::{StreamError, StreamResult};
use crate::connector::features::shared::actions::StreamAction;
use crate::connector::features::shared::clients_map::ClientsMap;
use crate::connector::features::shared::events::StreamEvent;
use crate::connector::features::shared::rate_limiter::RateLimitManager;
use crate::connector::features::tonic::client::{TonicChannelSpec, TonicClient};
use crate::connector::features::tonic::connector::TonicConnector;
use crate::connector::features::tonic::unary::UnaryEvent;
use crate::connector::features::tonic::unary::actions::UnaryActionInner;
use crate::connector::features::tonic::unary::descriptor::TonicUnaryDescriptor;
use crate::connector::{Hook, HookArgs, IntoHook, RuntimeCtx, StreamRunner, StreamSpawner};
use crate::io::ringbuffer::RingSender;
use crate::prelude::{BaseRx, TxPairExt};
use crate::utils::{Reducer, StateMarker};

use anyhow::anyhow;
use crossbeam::channel::unbounded;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::Mutex;
use tokio::task::LocalSet;
use tokio::time::sleep;
use tonic::Status;
use tonic::client::Grpc;

impl<E, R, S> StreamSpawner<TonicUnaryDescriptor, E, R, S> for TonicConnector
where
    E: TxPairExt,
    S: StateMarker,
    R: Reducer,
{
}

impl<E, R, S> StreamRunner<TonicUnaryDescriptor, E, R, S> for TonicConnector
where
    E: TxPairExt,
    S: StateMarker,
    R: Reducer,
{
    type Config = ClientsMap<TonicClient, TonicChannelSpec>;
    type ActionTx = RingSender<StreamAction<UnaryActionInner>>;
    type RawEvent = StreamEvent<UnaryEvent>;
    type HookResult = ();

    fn build_config(&mut self, _desc: &TonicUnaryDescriptor) -> anyhow::Result<Self::Config> {
        Ok(self.clients_map().clone())
    }

    fn run<H>(
        mut ctx: RuntimeCtx<
            ClientsMap<TonicClient, TonicChannelSpec>,
            TonicUnaryDescriptor,
            RingSender<StreamAction<UnaryActionInner>>,
            E,
            R,
            S,
        >,
        hook: H,
    ) -> StreamResult<()>
    where
        H: IntoHook<StreamEvent<UnaryEvent>, E, R, S, TonicUnaryDescriptor, ()>,
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

        let clients_map = ctx.cfg.clone();

        ctx.health.up();

        loop {
            if ctx.cancel.is_cancelled() {
                ctx.health.down();
                break Err(StreamError::Cancelled);
            }

            while let Ok(action) = ctx.action_rx.try_recv() {
                let cl_map = clients_map.clone();
                let res_tx_ref = res_tx.clone();
                let rl_mgr = action.rl_ctx().as_ref().map(|_| rl_manager.clone());

                // спаунит асинхронную задачу на LocalSet.
                local.spawn_local(async move {
                    let (inner, conn_id, req_id, label, timeout, rl_ctx, rl_weight, json) =
                        action.into_parts();

                    let inner = match inner {
                        Some(i) => i,
                        None => {
                            return;
                        }
                    };

                    let channel = if let Some(conn_id) = conn_id {
                        let Some(client) = cl_map.get(&conn_id) else {
                            return;
                        };
                        client.channel()
                    } else {
                        return;
                    };

                    let stream_event = StreamEvent::builder(None)
                        .conn_id(conn_id)
                        .req_id(req_id)
                        .label(label)
                        .payload(json);

                    let mut grpc = Grpc::new(channel);

                    if let Some(s) = ctx.desc.max_decoding_message_size {
                        grpc = grpc.max_decoding_message_size(s);
                    }
                    if let Some(s) = ctx.desc.max_encoding_message_size {
                        grpc = grpc.max_encoding_message_size(s);
                    }

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

                    if let Err(e) = grpc.ready().await {
                        let inner_event = UnaryEvent::from_status(Status::unavailable(format!(
                            "[TonicStream - Runner] channel not ready with error: {e}"
                        )));

                        let stream_event = stream_event.inner(inner_event).build().unwrap();

                        let _ = res_tx_ref.try_send(stream_event);
                        return;
                    }

                    let result = inner.execute(&mut grpc, timeout).await;

                    match result {
                        Ok(ev) => {
                            let ev = stream_event.inner(ev).build().unwrap();
                            let _ = res_tx_ref.try_send(ev);
                        }
                        Err(status) => {
                            let inner_event = UnaryEvent::from_status(status);
                            let ev = stream_event.inner(inner_event).build().unwrap();
                            let _ = res_tx_ref.try_send(ev);
                        }
                    }
                });
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

// #[cfg(test)]
// mod tests {
//     use bytes::Bytes;
//     use std::time::Duration;

//     use crate::connector::base::BaseConnector;
//     use crate::connector::features::grpc::connector::{
//         DEFAULT_CONN_ID, GrpcConnector, GrpcConnectorConfig,
//     };
//     use crate::connector::features::grpc::stream::actions::unary;
//     use crate::connector::features::grpc::stream::descriptor::GrpcStreamDescriptor;
//     use crate::connector::features::grpc::stream::event::GrpcEvent;

//     use crate::io::ringbuffer::RingSender; // <-- тип для E
//     use crate::utils::{CancelToken, NullReducer, NullState}; // <-- готовые заглушки

//     use crate::connector::{Hook, HookArgs};
//     use crate::utils::{Reducer, StateMarker};

//     // Простейший Hook: печатаем, что прилетело из раннера
//     #[derive(Default, Clone)]
//     struct PrintHook;

//     impl<R, S> Hook<GrpcEvent, RingSender<GrpcEvent>, R, S, GrpcStreamDescriptor, ()> for PrintHook
//     where
//         R: Reducer,
//         S: StateMarker,
//     {
//         fn call(
//             &mut self,
//             args: HookArgs<GrpcEvent, RingSender<GrpcEvent>, R, S, GrpcStreamDescriptor>,
//         ) -> () {
//             let ev = args.raw;
//             if ev.is_stream_item() {
//                 println!(
//                     "[stream item] {} bytes",
//                     ev.body_bytes().map(|b| b.len()).unwrap_or(0)
//                 );
//             } else if ev.is_ok() {
//                 println!(
//                     "[final ok] {} bytes",
//                     ev.body_bytes().map(|b| b.len()).unwrap_or(0)
//                 );
//             } else {
//                 println!("[error] {}", ev.maybe_error_msg().unwrap_or_default());
//             }
//         }
//     }

//     #[tokio::test(flavor = "multi_thread")]
//     async fn grpc_test() -> anyhow::Result<()> {
//         // 1) Коннектор -> grpcbin (TLS)
//         let cfg = GrpcConnectorConfig {
//             default_max_cores: Some(1),
//             specific_core_ids: vec![],
//             use_core_stats: false,
//             endpoints: Some(vec![(DEFAULT_CONN_ID, "http://grpcb.in:9000".to_string())]),
//             connect_timeout_ms: Some(10_000),
//             http2_keepalive_interval_ms: Some(20_000),
//             http2_keepalive_timeout_ms: Some(10_000),
//             request_timeout_ms: Some(5_000),
//         };
//         let cancel = CancelToken::new_root();
//         let mut conn = GrpcConnector::init(cfg, cancel.clone(), None)?;

//         // 2) Дескриптор
//         let desc = GrpcStreamDescriptor::low_latency();

//         // 3) Запускаем стрим. ЯВНО указываем типы E, R, S:
//         let hook = PrintHook::default();
//         let mut stream = conn.spawn_stream::<GrpcStreamDescriptor, RingSender<GrpcEvent>, NullReducer, NullState, PrintHook>(desc.clone(), hook)?;

//         // 4) 10 секунд шлём unary Index — через stream.try_send(...)
//         let started = tokio::time::Instant::now();
//         while started.elapsed() < Duration::from_secs(10) {
//             let action = unary("/grpcbin.GRPCBin/Index", Bytes::new())
//                 .label("smoke")
//                 .timeout(Duration::from_secs(3))
//                 .build();

//             stream.try_send(action).expect("send action to stream");

//             tokio::time::sleep(Duration::from_millis(1000)).await;
//         }

//         // 5) Остановить стрим
//         stream.stop()?;
//         Ok(())
//     }
// }
