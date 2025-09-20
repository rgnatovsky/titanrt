use crate::connector::StreamDescriptor;
use crate::prelude::BaseTx;
use crate::utils::{HealthFlag, Reducer, StateCell, StateMarker};

#[non_exhaustive]
pub struct HookArgs<'a, Ev, EvTx, R, State, Desc>
where
    Ev: Send + 'static,
    EvTx: BaseTx,
    R: Reducer,
    State: StateMarker,
    Desc: StreamDescriptor,
{
    pub raw: Ev,
    pub event_tx: &'a mut EvTx,
    pub reducer: &'a mut R,
    pub state: &'a StateCell<State>,
    pub desc: &'a Desc,
    pub health: &'a HealthFlag,
}

impl<'a, Ev, EvTx, R, State, Desc> HookArgs<'a, Ev, EvTx, R, State, Desc>
where
    Ev: Send + 'static,
    EvTx: BaseTx,
    R: Reducer,
    State: StateMarker,
    Desc: StreamDescriptor,
{
    #[inline]
    pub fn new(
        raw: Ev,
        event_tx: &'a mut EvTx,
        reducer: &'a mut R,
        state: &'a StateCell<State>,
        desc: &'a Desc,
        health: &'a HealthFlag,
    ) -> Self {
        Self {
            raw,
            event_tx,
            reducer,
            state,
            desc,
            health,
        }
    }
}

pub trait Hook<Ev, EvTx, R, State, Desc, Res>
where
    Ev: Send + 'static,
    EvTx: BaseTx,
    R: Reducer,
    State: StateMarker,
    Desc: StreamDescriptor,
{
    fn call<'a>(&'a mut self, args: HookArgs<'a, Ev, EvTx, R, State, Desc>) -> Res;
}

// Любое замыкание вида FnMut(HookArgs...) автоматически становится Hook.
impl<Ev, EvTx, R, State, Desc, Res, F> Hook<Ev, EvTx, R, State, Desc, Res> for F
where
    Ev: Send + 'static,
    EvTx: BaseTx,
    R: Reducer,
    State: StateMarker,
    Desc: StreamDescriptor,
    for<'a> F: FnMut(HookArgs<'a, Ev, EvTx, R, State, Desc>) -> Res,
{
    fn call<'a>(&'a mut self, args: HookArgs<'a, Ev, EvTx, R, State, Desc>) -> Res {
        self(args)
    }
}
pub trait IntoHook<Ev, EvTx, R, State, Desc, Res>: Send + 'static
where
    Ev: Send + 'static,
    EvTx: BaseTx,
    R: Reducer,
    State: StateMarker,
    Desc: StreamDescriptor,
{
    type Out: Hook<Ev, EvTx, R, State, Desc, Res> + Send + 'static;
    fn into_hook(self) -> Self::Out;
}

impl<Ev, EvTx, R, State, Desc, Res, F> IntoHook<Ev, EvTx, R, State, Desc, Res> for F
where
    Ev: Send + 'static,
    EvTx: BaseTx,
    R: Reducer,
    State: StateMarker,
    Desc: StreamDescriptor,
    F: Hook<Ev, EvTx, R, State, Desc, Res> + Send + 'static,
{
    type Out = F;
    fn into_hook(self) -> Self::Out {
        self
    }
}
