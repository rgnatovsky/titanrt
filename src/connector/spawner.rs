use crate::connector::hook::IntoHook;
use crate::connector::{RuntimeCtx, Stream, StreamDescriptor, StreamRunner};
use crate::io::base::{BaseTx, TxPairExt};
use crate::utils::*;
use std::fmt::Debug;
use std::{sync::Arc, thread};
use uuid::Uuid;
use uuid::fmt::Simple;

#[derive(Debug)]
pub enum EventTxType<E> {
    External(E),
    Own,
}

impl<E> EventTxType<E> {
    #[inline]
    pub fn external(tx: E) -> Self {
        Self::External(tx)
    }
}

impl<E> Default for EventTxType<E> {
    fn default() -> Self {
        Self::Own
    }
}

/// Blanket helper for spawning typed streams from a connector.
///
/// A `StreamSpawner` is implemented on top of a [`StreamRunner`].
/// It allocates channels, state, health flag, applies core pinning policy,
/// and starts the worker thread. The runner then executes its loop
/// inside that thread with a [`RuntimeCtx`].
pub trait StreamSpawner<D, E, R, S, T>
where
    Self: StreamRunner<D, E, R, S, T>,
    D: StreamDescriptor<T>,
    S: StateMarker,
    E: BaseTx + TxPairExt,
    R: Reducer,
    T: Debug + Clone + Send + 'static,
{
    /// Build a human-readable thread name from descriptor and UUID.
    fn thread_name(&self, desc: &D, id: Simple) -> String {
        format!("{}-{}-{}", desc.venue(), desc.kind(), id)
    }

    /// Spawn a new worker stream given a descriptor and event hook.
    ///
    /// - Builds config via [`StreamRunner::build_config`].
    /// - Allocates action/event channels (bounded/unbounded from descriptor).
    /// - Creates state cell and health flag.
    /// - Applies core pinning policy if provided.
    /// - Spawns the worker thread running [`StreamRunner::run`].
    /// - Returns a [`Stream`] handle with action TX, optional event RX, state, cancel, and health.
    fn spawn<H>(
        &mut self,
        desc: D,
        event_tx_type: EventTxType<E>,
        hook: H,
        cancel: CancelToken,
        core_stats: Option<Arc<CoreStats>>,
    ) -> anyhow::Result<Stream<Self::ActionTx, Option<E::RxHalf>, S>>
    where
        H: IntoHook<Self::RawEvent, E, R, S, D, Self::HookResult, T>,
    {
        // Per-stream config
        let ctx = match self.build_config(&desc) {
            Ok(ctx) => ctx,
            Err(err) => {
                tracing::error!(
                    "[{}-{}] failed to build stream config: {}",
                    desc.venue(),
                    desc.kind(),
                    err
                );
                return Err(err);
            }
        };
        let mut reducer = R::default();

        let mut state = S::default();
        state.configure(&desc, &mut reducer);

        let state = Arc::new(StateCell::<S>::new(state));

        let health = HealthFlag::new(desc.health_at_start());

        // Channels: actions (model->worker)
        let (action_tx, action_rx) = if let Some(bound) = desc.max_pending_actions() {
            <Self::ActionTx as TxPairExt>::bound(bound)
        } else {
            <Self::ActionTx as TxPairExt>::unbound()
        };

        // Channels: events (worker->model)
        let (event_tx, event_rx) = match event_tx_type {
            EventTxType::Own => {
                if let Some(bound) = desc.max_pending_events() {
                    let (tx, rx) = E::bound(bound);
                    (tx, Some(rx))
                } else {
                    let (tx, rx) = E::unbound();
                    (tx, Some(rx))
                }
            }
            EventTxType::External(tx) => (tx, None),
        };

        // Unique stream id
        let stream_id = Uuid::new_v4().simple();
        let stream_name = self.thread_name(&desc, stream_id);
        // Spawn worker thread
        let handle = thread::Builder::new()
            .name(stream_name.clone())
            .spawn({
                let rt_ctx = RuntimeCtx::new(
                    ctx,
                    desc,
                    action_rx,
                    event_tx,
                    reducer,
                    state.clone(),
                    cancel.clone(),
                    health.clone(),
                );

                let health = health.clone();

                move || {
                    let mut _core_lease = None;

                    // Apply core pinning policy if any
                    if let Some(policy) = rt_ctx.desc.core_pick_policy() {
                        match (core_stats, policy.specific()) {
                            // Reserve/pin via CoreStats
                            (Some(cs), _) => {
                                let lease = cs.reserve(policy);
                                match try_pin_core(lease.core_id) {
                                    Ok(core_id) => {
                                        tracing::info!("pinned core {} successfully", core_id);
                                        _core_lease = Some(lease); // keep guard alive
                                    }
                                    Err(err) => {
                                        tracing::error!(
                                            "failed to pin core {}: {}",
                                            lease.core_id,
                                            err
                                        );
                                    }
                                }
                            }
                            // Specific core requested but no CoreStats available
                            (None, Some(core_id)) => {
                                tracing::warn!(
                                    "core pinning policy is set to specific, but no core stats available"
                                );
                                match try_pin_core(core_id) {
                                    Ok(core_id) => {
                                        tracing::info!("pinned core {} successfully", core_id);
                                    }
                                    Err(err) => {
                                        tracing::error!(
                                            "failed to pin core {}: {}",
                                            core_id,
                                            err
                                        );
                                    }
                                }
                            }
                            // Policy requires stats but none provided
                            (None, None) => {
                                tracing::warn!(
                                    "core pinning policy requires core stats, but none are available"
                                );
                            }
                        }
                    }

                    // Run the worker
                    let res = Self::run(rt_ctx, hook);

                    health.down();

                    if let Err(err) = &res {
                        tracing::error!("stream {} failed: {}", stream_name, err);
                    }

                    res
                }
            })?;

        // Wrap into a `Stream` handle
        let stream = Stream::<Self::ActionTx, Option<E::RxHalf>, S>::new(
            stream_id, handle, health, action_tx, event_rx, state, cancel,
        );

        Ok(stream)
    }
}
