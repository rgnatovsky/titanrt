use crate::control::controller::{Controller, ControllerResult};
use crate::control::inputs::Input;
use crate::model::{BaseModel, ModelEvent};
use crate::utils::CancelToken;
use std::ops::ControlFlow;

pub trait Policy<E: ModelEvent> {
    fn handle(&mut self, inp: Input<E>) -> ControlFlow<ControllerResult>;
}

pub struct WithModel<'m, M: BaseModel> {
    pub model: &'m mut M,
    pub cfg: &'m mut M::Config,
    pub cancel: &'m CancelToken,
    pub stop_timeout_secs: u64,
}

impl<'m, M: BaseModel> Policy<M::Event> for WithModel<'m, M> {
    #[inline(always)]
    fn handle(&mut self, inp: Input<M::Event>) -> ControlFlow<ControllerResult> {
        match Controller::<M>::handle_with_model(inp, self) {
            ControlFlow::Continue(()) => ControlFlow::Continue(()),
            ControlFlow::Break(r) => ControlFlow::Break(r),
        }
    }
}

pub struct NoModel<'m, M: BaseModel> {
    pub c: &'m mut M::Config,
}

impl<'m, M: BaseModel> Policy<M::Event> for NoModel<'m, M> {
    #[inline(always)]
    fn handle(&mut self, inp: Input<M::Event>) -> ControlFlow<ControllerResult> {
        match Controller::<M>::handle_no_model(inp, self.c) {
            ControlFlow::Continue(()) => ControlFlow::Continue(()),
            ControlFlow::Break(r) => ControlFlow::Break(r),
        }
    }
}
