use crate::adapter::errors::StreamResult;
use crate::adapter::StreamDescriptor;

use crate::io::base::{BaseTx, TxPairExt};
use crate::utils::*;
use std::sync::Arc;

pub struct RuntimeCtx<D, R, E, S>
where
    D: StreamDescriptor,
    R: StreamRunner<D, E, S>,
    S: StateMarker,
    E: BaseTx,
{
    pub desc: D,
    pub cfg: R::Config,
    pub state: Arc<StateCell<S>>,
    pub action_rx: <R::ActionTx as TxPairExt>::RxHalf,
    pub event_tx: E,
    pub cancel: CancelToken,
    pub health: HealthFlag,
}

impl<D, R, E, S> RuntimeCtx<D, R, E, S>
where
    D: StreamDescriptor,
    R: StreamRunner<D, E, S>,
    S: StateMarker,
    E: BaseTx,
{
    #[inline]
    pub fn new(
        ctx: R::Config,
        desc: D,
        action_rx: <R::ActionTx as TxPairExt>::RxHalf,
        event_tx: E,
        state: Arc<StateCell<S>>,
        cancel: CancelToken,
        health: HealthFlag,
    ) -> Self {
        Self {
            desc,
            cfg: ctx,
            action_rx,
            event_tx,
            state,
            cancel,
            health,
        }
    }
}

pub trait StreamRunner<D, E, S>: Sized + Send + 'static
where
    D: StreamDescriptor,
    S: StateMarker,
    E: BaseTx,
{
    type Config: Send + 'static;
    type ActionTx: BaseTx + TxPairExt;
    type RawEvent: Send + 'static;
    type Hook: Fn(&Self::RawEvent, &mut E, &StateCell<S>) + Send + 'static;

    fn build_config(&mut self, desc: &D) -> anyhow::Result<Self::Config>;

    fn run(ctx: RuntimeCtx<D, Self, E, S>, hook: Self::Hook) -> StreamResult<()>;
}
