use crate::connector::errors::{StreamError, StreamResult};
use crate::connector::features::shared::actions::StreamAction;
use crate::connector::features::shared::clients_map::ClientsMap;
use crate::connector::features::shared::events::StreamEvent;
use crate::connector::features::shared::rate_limiter::RateLimitManager;
use crate::connector::features::tonic::client::{TonicChannelSpec, TonicClient};
use crate::connector::features::tonic::connector::TonicConnector;
use crate::connector::features::tonic::unary::UnaryEvent;
use crate::connector::features::tonic::unary::actions::UnaryAction;
use crate::connector::features::tonic::unary::descriptor::TonicUnaryDescriptor;
use crate::connector::{Hook, HookArgs, IntoHook, RuntimeCtx, StreamRunner, StreamSpawner};
use crate::io::ringbuffer::RingSender;
use crate::prelude::{BaseRx, TxPairExt};
use crate::utils::{Reducer, StateMarker};

use crossbeam::channel::unbounded;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
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
    type Config = (ClientsMap<TonicClient, TonicChannelSpec>, Arc<Runtime>);
    type ActionTx = RingSender<StreamAction<UnaryAction>>;
    type RawEvent = StreamEvent<UnaryEvent>;
    type HookResult = ();

    fn build_config(&mut self, _desc: &TonicUnaryDescriptor) -> anyhow::Result<Self::Config> {
        Ok((self.clients_map(), self.rt_tokio()))
    }

    fn run<H>(
        mut ctx: RuntimeCtx<
            (ClientsMap<TonicClient, TonicChannelSpec>, Arc<Runtime>),
            TonicUnaryDescriptor,
            RingSender<StreamAction<UnaryAction>>,
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

        let local = LocalSet::new();

        let (res_tx, res_rx) = unbounded();

        let (clients_map, rt) = ctx.cfg;

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
                tracing::debug!("Received unary action: {:?}", action);
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

                    tracing::debug!("Using channel: {:?}", channel);
                    
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
                    tracing::debug!("Sending unary request: {:?}", inner);
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
