use crate::connector::StreamDescriptor;
use crate::prelude::BaseTx;
use crate::utils::{HealthFlag, Reducer, StateCell, StateMarker};

#[non_exhaustive]
pub struct HookArgs<'a, Ev, EvTx, R, State, Desc, T>
where
    Ev: Send + 'static,
    EvTx: BaseTx,
    R: Reducer,
    State: StateMarker,
    Desc: StreamDescriptor<T>,
{
    pub raw: Ev,
    pub event_tx: &'a mut EvTx,
    pub reducer: &'a mut R,
    pub state: &'a StateCell<State>,
    pub desc: &'a Desc,
    pub health: &'a HealthFlag,
    _marker: std::marker::PhantomData<T>,
}

impl<'a, Ev, EvTx, R, State, Desc, T> HookArgs<'a, Ev, EvTx, R, State, Desc, T>
where
    Ev: Send + 'static,
    EvTx: BaseTx,
    R: Reducer,
    State: StateMarker,
    Desc: StreamDescriptor<T>,
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
            _marker: std::marker::PhantomData,
        }
    }
}

pub trait Hook<Ev, EvTx, R, State, Desc, Res, T>
where
    Ev: Send + 'static,
    EvTx: BaseTx,
    R: Reducer,
    State: StateMarker,
    Desc: StreamDescriptor<T>,
{
    fn call<'a>(&'a mut self, args: HookArgs<'a, Ev, EvTx, R, State, Desc, T>) -> Res;
}

// Любое замыкание вида FnMut(HookArgs...) автоматически становится Hook.
impl<Ev, EvTx, R, State, Desc, Res, F, T> Hook<Ev, EvTx, R, State, Desc, Res, T> for F
where
    Ev: Send + 'static,
    EvTx: BaseTx,
    R: Reducer,
    State: StateMarker,
    Desc: StreamDescriptor<T>,
    for<'a> F: FnMut(HookArgs<'a, Ev, EvTx, R, State, Desc, T>) -> Res,
{
    fn call<'a>(&'a mut self, args: HookArgs<'a, Ev, EvTx, R, State, Desc, T>) -> Res {
        self(args)
    }
}
pub trait IntoHook<Ev, EvTx, R, State, Desc, Res, T>: Send + 'static
where
    Ev: Send + 'static,
    EvTx: BaseTx,
    R: Reducer,
    State: StateMarker,
    Desc: StreamDescriptor<T>,
{
    type Out: Hook<Ev, EvTx, R, State, Desc, Res, T> + Send + 'static;
    fn into_hook(self) -> Self::Out;
}

impl<Ev, EvTx, R, State, Desc, Res, F, T> IntoHook<Ev, EvTx, R, State, Desc, Res, T> for F
where
    Ev: Send + 'static,
    EvTx: BaseTx,
    R: Reducer,
    State: StateMarker,
    Desc: StreamDescriptor<T>,
    F: Hook<Ev, EvTx, R, State, Desc, Res, T> + Send + 'static,
{
    type Out = F;
    fn into_hook(self) -> Self::Out {
        self
    }
}
