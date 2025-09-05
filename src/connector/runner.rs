use crate::connector::errors::StreamResult;
use crate::connector::hook::IntoHook;
use crate::connector::StreamDescriptor;
use crate::io::base::{BaseTx, TxPairExt};
use crate::utils::*;
use std::sync::Arc;

/// Runtime context passed to a spawned stream worker.
///
/// Bundles together the descriptor, config, typed channels,
/// shared state, cancel token, and health flag. This is the
/// handle a [`StreamRunner`] implementation uses inside `run()`.
pub struct RuntimeCtx<C, D, A, E, R, S>
where
    C: Send + 'static,
    D: StreamDescriptor,
    A: BaseTx + TxPairExt,
    S: StateMarker,
    E: BaseTx,
    R: Reducer,
{
    /// Stream descriptor (venue, kind, bounds, policy).
    pub desc: D,
    /// Per-stream config built by the runner.
    pub cfg: C,
    /// Reducer for the stream.
    pub reducer: R,
    /// Shared state snapshot cell.
    pub state: Arc<StateCell<S>>,
    /// Channel for actions coming from the model/user.
    pub action_rx: <A as TxPairExt>::RxHalf,
    /// Channel for events going back to the model/user.
    pub event_tx: E,
    /// Cancellation token (child of connector/runtime root).
    pub cancel: CancelToken,
    /// Health flag of this worker.
    pub health: HealthFlag,
}

impl<C, D, A, E, R, S> RuntimeCtx<C, D, A, E, R, S>
where
    C: Send + 'static,
    D: StreamDescriptor,
    A: BaseTx + TxPairExt,
    S: StateMarker,
    E: BaseTx,
    R: Reducer,
{
    /// Construct a new runtime context.
    #[inline]
    pub fn new(
        cfg: C,
        desc: D,
        action_rx: <A as TxPairExt>::RxHalf,
        event_tx: E,
        reducer: R,
        state: Arc<StateCell<S>>,
        cancel: CancelToken,
        health: HealthFlag,
    ) -> Self {
        Self {
            desc,
            cfg,
            reducer,
            action_rx,
            event_tx,
            state,
            cancel,
            health,
        }
    }
}

/// Trait for stream workers owned by a connector.
///
/// A `StreamRunner` defines how to build a per-stream config
/// and how to run the worker loop given a [`RuntimeCtx`].
pub trait StreamRunner<D, E, R, S>: Sized + Send + 'static
where
    D: StreamDescriptor,
    S: StateMarker,
    E: BaseTx,
    R: Reducer,
{
    /// Config type built from the descriptor (passed into the context).
    type Config: Send + 'static;
    /// Actions channel TX half (model → worker).
    type ActionTx: BaseTx + TxPairExt;
    /// Raw events produced inside the worker loop.
    type RawEvent: Send + 'static;
    /// Hook result type.
    type HookResult;

    /// Build a per-stream config from the descriptor.
    fn build_config(&mut self, desc: &D) -> anyhow::Result<Self::Config>;

    /// Run the worker loop with the given context and event hook.
    fn run<H>(
        ctx: RuntimeCtx<Self::Config, D, Self::ActionTx, E, R, S>,
        hook: H,
    ) -> StreamResult<()>
    where
        H: IntoHook<Self::RawEvent, E, R, S, D, Self::HookResult>;
}
