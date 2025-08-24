use crate::connector::StreamDescriptor;
use crate::prelude::BaseTx;
use crate::utils::{StateCell, StateMarker};

#[non_exhaustive]
pub struct HookArgs<'a, Ev, EvTx, State, Desc>
where
    Ev: Send + 'static,
    EvTx: BaseTx,
    State: StateMarker,
    Desc: StreamDescriptor,
{
    pub raw: &'a Ev,
    pub event_tx: &'a mut EvTx,
    pub state: &'a StateCell<State>,
    pub desc: &'a Desc,
}

impl<'a, Ev, EvTx, State, Desc> HookArgs<'a, Ev, EvTx, State, Desc>
where
    Ev: Send + 'static,
    EvTx: BaseTx,
    State: StateMarker,
    Desc: StreamDescriptor,
{
    #[inline]
    pub fn new(
        raw: &'a Ev,
        event_tx: &'a mut EvTx,
        state: &'a StateCell<State>,
        desc: &'a Desc,
    ) -> Self {
        Self {
            raw,
            event_tx,
            state,
            desc,
        }
    }
}

pub trait Hook<Ev, EvTx, State, Desc, Res>
where
    Ev: Send + 'static,
    EvTx: BaseTx,
    State: StateMarker,
    Desc: StreamDescriptor,
{
    fn call<'a>(&'a mut self, args: HookArgs<'a, Ev, EvTx, State, Desc>) -> Res;
}

// Любое замыкание вида FnMut(HookArgs...) автоматически становится Hook.
impl<Ev, EvTx, State, Desc, Res, F> Hook<Ev, EvTx, State, Desc, Res> for F
where
    Ev: Send + 'static,
    EvTx: BaseTx,
    State: StateMarker,
    Desc: StreamDescriptor,
    for<'a> F: FnMut(HookArgs<'a, Ev, EvTx, State, Desc>) -> Res,
{
    fn call<'a>(&'a mut self, args: HookArgs<'a, Ev, EvTx, State, Desc>) -> Res {
        self(args)
    }
}
pub trait IntoHook<Ev, EvTx, State, Desc, Res>: Send + 'static
where
    Ev: Send + 'static,
    EvTx: BaseTx,
    State: StateMarker,
    Desc: StreamDescriptor,
{
    type Out: Hook<Ev, EvTx, State, Desc, Res> + Send + 'static;
    fn into_hook(self) -> Self::Out;
}

impl<Ev, EvTx, State, Desc, Res, F> IntoHook<Ev, EvTx, State, Desc, Res> for F
where
    Ev: Send + 'static,
    EvTx: BaseTx,
    State: StateMarker,
    Desc: StreamDescriptor,
    F: Hook<Ev, EvTx, State, Desc, Res> + Send + 'static,
{
    type Out = F;
    fn into_hook(self) -> Self::Out {
        self
    }
}
