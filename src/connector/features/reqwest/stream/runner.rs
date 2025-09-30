use crate::connector::errors::{StreamError, StreamResult};
use crate::connector::features::reqwest::client::{ReqwestClient, ReqwestClientSpec};
use crate::connector::features::reqwest::connector::ReqwestConnector;
use crate::connector::features::reqwest::stream::actions::ReqwestAction;
use crate::connector::features::reqwest::stream::descriptor::ReqwestStreamDescriptor;
use crate::connector::features::reqwest::stream::event::ReqwestEvent;
use crate::connector::features::shared::actions::StreamAction;
use crate::connector::features::shared::clients_map::ClientsMap;
use crate::connector::features::shared::events::StreamEvent;
use crate::connector::features::shared::rate_limiter::RateLimitManager;
use crate::connector::{Hook, HookArgs, IntoHook, RuntimeCtx, StreamRunner, StreamSpawner};
use crate::io::ringbuffer::RingSender;
use crate::prelude::{BaseRx, TxPairExt};
use crate::utils::{Reducer, StateMarker};
use anyhow::anyhow;
use crossbeam::channel::unbounded;
use std::fmt::Debug;
use std::rc::Rc;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::Mutex;
use tokio::task::LocalSet;
use tokio::time::sleep;

impl<E, R, S, T> StreamSpawner<ReqwestStreamDescriptor<T>, E, R, S, T> for ReqwestConnector
where
    E: TxPairExt,
    S: StateMarker,
    R: Reducer,
    T: Debug + Clone + Send + 'static,
{
}

impl<E, R, S, T> StreamRunner<ReqwestStreamDescriptor<T>, E, R, S, T> for ReqwestConnector
where
    E: TxPairExt,
    S: StateMarker,
    R: Reducer,
    T: Debug + Clone + Send + 'static,
{
    type Config = ClientsMap<ReqwestClient, ReqwestClientSpec>;
    type ActionTx = RingSender<StreamAction<ReqwestAction>>;
    type RawEvent = StreamEvent<ReqwestEvent>;
    type HookResult = ();

    fn build_config(&mut self, _desc: &ReqwestStreamDescriptor<T>) -> anyhow::Result<Self::Config> {
        Ok(self.clients_map())
    }

    fn run<H>(
        mut ctx: RuntimeCtx<
            ClientsMap<ReqwestClient, ReqwestClientSpec>,
            ReqwestStreamDescriptor<T>,
            RingSender<StreamAction<ReqwestAction>>,
            E,
            R,
            S,
            T,
        >,
        hook: H,
    ) -> StreamResult<()>
    where
        H: IntoHook<StreamEvent<ReqwestEvent>, E, R, S, ReqwestStreamDescriptor<T>, (), T>,
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

        let (res_tx, res_rx) = unbounded(); // подставь свой способ

        ctx.health.up();

        loop {
            if ctx.cancel.is_cancelled() {
                ctx.health.down();
                break Err(StreamError::Cancelled);
            }

            while let Ok(mut action) = ctx.action_rx.try_recv() {
                let Some(client) = action.conn_id().and_then(|id| ctx.cfg.get(&id)) else {
                    continue;
                };
                let inner = match action.inner_take() {
                    Some(inner) => inner,
                    None => continue,
                };

                let request = match inner
                    .to_request_builder(&client.as_ref().0, action.timeout())
                    .build()
                {
                    Ok(request) => request,
                    Err(e) => {
                        let inner = ReqwestEvent::from_error(e);
                        let stream_event = StreamEvent::builder(None)
                            .conn_id(action.conn_id())
                            .req_id(action.req_id())
                            .label(action.label_take())
                            .payload(action.json_take())
                            .inner(inner)
                            .build()?;

                        let _ = res_tx.try_send(stream_event);
                        continue;
                    }
                };

                let res_tx = res_tx.clone();

                let rl_manager = if action.rl_ctx().is_some() {
                    Some(rl_manager.clone())
                } else {
                    None
                };

                local.spawn_local(async move {
                    if let Some(rl_manager) = rl_manager {
                        let plan = {
                            let mut mgr = rl_manager.lock().await;
                            mgr.plan(action.rl_ctx().as_ref().unwrap(), action.rl_weight())
                        };

                        if let Some(plan) = plan {
                            for (bucket, weight) in plan {
                                bucket.wait(weight).await;
                            }
                        }
                    }
                    let out = client.as_ref().0.execute(request).await;
                    let inner = match out {
                        Ok(resp) => ReqwestEvent::from_raw(resp).await,
                        Err(e) => ReqwestEvent::from_error(e),
                    };

                    let stream_event = StreamEvent::builder(None)
                        .conn_id(action.conn_id())
                        .req_id(action.req_id())
                        .label(action.label_take())
                        .payload(action.json_take())
                        .inner(inner)
                        .build()
                        .unwrap();

                    let _ = res_tx.try_send(stream_event);
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
                        ))
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
