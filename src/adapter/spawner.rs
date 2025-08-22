use crate::adapter::{RuntimeCtx, Stream, StreamDescriptor, StreamRunner};
use crate::io::base::{BaseTx, RxPairExt, TxPairExt};
use crate::utils::*;
use std::{sync::Arc, thread};
use uuid::fmt::Simple;
use uuid::Uuid;

pub trait StreamSpawner<D, E, S>
where
    Self: StreamRunner<D, E, S>,
    D: StreamDescriptor,
    S: StateMarker,
    E: BaseTx + TxPairExt,
{
    fn thread_name(&self, desc: &D, id: Simple) -> String {
        format!("{}-{}-{}", desc.venue(), desc.kind(), id)
    }

    fn spawn(
        &mut self,
        desc: D,
        hook: Self::Hook,
        cancel: CancelToken,
        core_stats: Option<Arc<CoreStats>>,
    ) -> anyhow::Result<Stream<Self::ActionTx, E::RxHalf, S>> {
        let ctx = match self.build_config(&desc) {
            Ok(ctx) => ctx,
            Err(err) => {
                tracing::error!(
                    "[{}-{}] failed to build yellowstone_grpc context: {}",
                    desc.venue(),
                    desc.kind(),
                    err
                );
                return Err(err);
            }
        };

        let state = StateCell::<S>::new_default();

        let health = HealthFlag::new(desc.health_at_start());

        let (action_tx, action_rx) = if let Some(bound) = desc.max_pending_actions() {
            <Self::ActionTx as TxPairExt>::bound(bound)
        } else {
            <Self::ActionTx as TxPairExt>::unbound()
        };

        let (event_tx, event_rx) = if let Some(bound) = desc.max_pending_events() {
            E::bound(bound)
        } else {
            E::unbound()
        };

        let stream_id = Uuid::new_v4().simple();

        let handle = thread::Builder::new()
            .name(self.thread_name(&desc, stream_id))
            .spawn({
                let rt_ctx = RuntimeCtx::new(
                    ctx,
                    desc,
                    action_rx,
                    event_tx,
                    state.clone(),
                    cancel.clone(),
                    health.clone(),
                );

                let health = health.clone();

                move || {
                    let mut _core_lease = None;

                    if let Some(policy) = rt_ctx.desc.core_pick_policy() {
                        match (core_stats, policy.specific()) {
                            // есть статистика — резервируем ядро по policy и пиннимся на него
                            (Some(cs), _) => {
                                let lease = cs.reserve(policy);
                                match try_pin_core(lease.core_id) {
                                    Ok(core_id) => {
                                        tracing::info!("pinned core {} successfully", core_id);
                                        _core_lease = Some(lease); // держим guard живым
                                    }
                                    Err(err) => {
                                        tracing::error!("failed to pin core {}: {}", lease.core_id, err);
                                    }
                                }
                            }

                            (None, Some(core_id)) => {
                                tracing::warn!("core pinning policy is set to specific, but no core stats are available");
                                match try_pin_core(core_id) {
                                    Ok(core_id) => {
                                        tracing::info!("pinned core {} successfully", core_id);
                                    }
                                    Err(err) => {
                                        tracing::error!("failed to pin core {}: {}", core_id, err);
                                    }
                                }
                            }

                            (None, None) => {
                                tracing::warn!("core pinning policy requires core stats, but none are available");
                            }
                        }
                    }

                    match Self::run(rt_ctx, hook) {
                        Ok(()) => {}
                        Err(err) => {
                            tracing::error!("stream {} failed: {}", stream_id, err);
                        }
                    }

                    health.down();

                    Ok(())
                }
            })?;

        let stream = Stream::<Self::ActionTx, E::RxHalf, S>::new(
            stream_id, handle, health, action_tx, event_rx, state, cancel,
        );

        Ok(stream)
    }
}
