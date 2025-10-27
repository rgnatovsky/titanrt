use crate::connector::errors::{StreamError, StreamResult};
use crate::connector::features::http::client::{ReqwestClient, ReqwestClientSpec};
use crate::connector::features::http::connector::HttpConnector;
use crate::connector::features::http::stream::actions::HttpAction;
use crate::connector::features::http::stream::descriptor::HttpDescriptor;
use crate::connector::features::http::stream::event::HttpEvent;
use crate::connector::features::shared::actions::StreamActionRaw;
use crate::connector::features::shared::clients_map::ClientsMap;
use crate::connector::features::shared::events::StreamEventRaw;
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

impl<E, R, S, T> StreamSpawner<HttpDescriptor<T>, E, R, S, T> for HttpConnector
where
    E: TxPairExt,
    S: StateMarker,
    R: Reducer,
    T: Debug + Clone + Send + 'static,
{
}

impl<E, R, S, T> StreamRunner<HttpDescriptor<T>, E, R, S, T> for HttpConnector
where
    E: TxPairExt,
    S: StateMarker,
    R: Reducer,
    T: Debug + Clone + Send + 'static,
{
    type Config = ClientsMap<ReqwestClient, ReqwestClientSpec>;
    type ActionTx = RingSender<StreamActionRaw<HttpAction>>;
    type RawEvent = StreamEventRaw<HttpEvent>;
    type HookResult = ();

    fn build_config(&mut self, _desc: &HttpDescriptor<T>) -> anyhow::Result<Self::Config> {
        Ok(self.clients_map())
    }

    fn run<H>(
        mut ctx: RuntimeCtx<
            ClientsMap<ReqwestClient, ReqwestClientSpec>,
            HttpDescriptor<T>,
            RingSender<StreamActionRaw<HttpAction>>,
            E,
            R,
            S,
            T,
        >,
        hook: H,
    ) -> StreamResult<()>
    where
        H: IntoHook<StreamEventRaw<HttpEvent>, E, R, S, HttpDescriptor<T>, (), T>,
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

                let http_action = match action.inner_take() {
                    Some(inner) => inner,
                    None => continue,
                };

                let timeout = action.timeout();
                let retry_cfg = http_action.retry.clone();
                let res_tx = res_tx.clone();

                let rl_manager = if action.rl_ctx().is_some() {
                    Some(rl_manager.clone())
                } else {
                    None
                };

                local.spawn_local(async move {
                    let mut attempt = 0usize;
                    let max_attempts = retry_cfg.as_ref().map(|cfg| cfg.attempts()).unwrap_or(1);

                    let inner = loop {
                        let request = match http_action.build_request(&client.as_ref().0, timeout) {
                            Ok(request) => request,
                            Err(err) => break HttpEvent::from_error(err),
                        };

                        if let Some(ref rl_manager) = rl_manager {
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

                        match client.as_ref().0.execute(request).await {
                            Ok(resp) => break HttpEvent::from_raw(resp).await,
                            Err(err) => {
                                attempt = attempt.saturating_add(1);
                                if attempt >= max_attempts {
                                    break HttpEvent::from_error(err);
                                }

                                if let Some(cfg) = retry_cfg.as_ref() {
                                    let delay = cfg.delay_for_attempt(attempt);
                                    if !delay.is_zero() {
                                        sleep(delay).await;
                                    }
                                }
                                continue;
                            }
                        }
                    };

                    let stream_event = StreamEventRaw::builder(None)
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
