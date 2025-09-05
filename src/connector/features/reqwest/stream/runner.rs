use crate::connector::errors::{StreamError, StreamResult};
use crate::connector::features::reqwest::connector::{ReqwestConnector, DEFAULT_IP_ID};
use crate::connector::features::reqwest::rate_limiter::RateLimitManager;
use crate::connector::features::reqwest::stream::actions::ReqwestAction;
use crate::connector::features::reqwest::stream::descriptor::ReqwestStreamDescriptor;
use crate::connector::features::reqwest::stream::event::ReqwestEvent;
use crate::connector::{Hook, HookArgs, IntoHook, RuntimeCtx, StreamRunner, StreamSpawner};
use crate::io::ringbuffer::RingSender;
use crate::prelude::{BaseRx, TxPairExt};
use crate::utils::{Reducer, StateMarker};
use ahash::AHashMap;
use anyhow::anyhow;
use crossbeam::channel::unbounded;
use reqwest::Client;
use std::rc::Rc;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::Mutex;
use tokio::task::LocalSet;
use tokio::time::sleep;

impl<E, R, S> StreamSpawner<ReqwestStreamDescriptor, E, R, S> for ReqwestConnector
where
    E: TxPairExt,
    S: StateMarker,
    R: Reducer,
{
}

impl<E, R, S> StreamRunner<ReqwestStreamDescriptor, E, R, S> for ReqwestConnector
where
    E: TxPairExt,
    S: StateMarker,
    R: Reducer,
{
    type Config = AHashMap<u16, Client>;
    type ActionTx = RingSender<ReqwestAction>;
    type RawEvent = ReqwestEvent;
    type HookResult = ();

    fn build_config(&mut self, _desc: &ReqwestStreamDescriptor) -> anyhow::Result<Self::Config> {
        Ok(self.clients_map().clone())
    }

    fn run<H>(
        mut ctx: RuntimeCtx<
            AHashMap<u16, Client>,
            ReqwestStreamDescriptor,
            RingSender<ReqwestAction>,
            E,
            R,
            S,
        >,
        hook: H,
    ) -> StreamResult<()>
    where
        H: IntoHook<ReqwestEvent, E, R, S, ReqwestStreamDescriptor, ()>,
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
                let client = action
                    .ip_id
                    .and_then(|id| ctx.cfg.get(&id))
                    .or_else(|| ctx.cfg.get(&DEFAULT_IP_ID))
                    .cloned();

                let Some(client) = client else { continue };

                let req_id = action.req_id;
                let rl_ctx = action.rl_ctx.take();
                let label = action.label.take();

                let request = match action.to_request_builder(&client).build() {
                    Ok(request) => request,
                    Err(e) => {
                        let _ = res_tx.try_send(ReqwestEvent::from_error(e, req_id, label));
                        continue;
                    }
                };

                let res_tx = res_tx.clone();

                let rl_manager = if rl_ctx.is_some() {
                    Some(rl_manager.clone())
                } else {
                    None
                };

                local.spawn_local(async move {
                    if let Some(rl_manager) = rl_manager {
                        let plan = {
                            let mut mgr = rl_manager.lock().await;
                            mgr.plan(rl_ctx.as_ref().unwrap())
                        };

                        if let Some(plan) = plan {
                            for (bucket, weight) in plan {
                                bucket.wait(weight).await;
                            }
                        }
                    }
                    let out = client.execute(request).await;
                    let event = match out {
                        Ok(resp) => ReqwestEvent::from_raw(resp, req_id, label).await,
                        Err(e) => ReqwestEvent::from_error(e, req_id, label),
                    };
                    let _ = res_tx.try_send(event);
                });
            }

            let mut budget = ctx.desc.max_hook_calls_at_once;

            while budget > 0 {
                match res_rx.try_recv() {
                    Ok(event) => {
                        budget -= 1;
                        hook.call(HookArgs::new(
                            &event,
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
