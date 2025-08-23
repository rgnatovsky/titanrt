use crate::control::controller::{Controller, ControllerResult};
use crate::control::inputs::Input;
use crate::model::{BaseModel, ModelEvent};
use crate::utils::{CancelToken, HealthFlag};
use std::ops::ControlFlow;

pub trait Policy<E: ModelEvent, H> {
    fn handle(
        &mut self,
        inp: Input<E>,
        model_health: &HealthFlag,
        output_tx: &mut H,
    ) -> ControlFlow<ControllerResult>;
}

pub struct WithModel<'m, M: BaseModel> {
    pub model: &'m mut M,
    pub cfg: &'m mut M::Config,
    pub cancel: &'m CancelToken,
    pub stop_timeout_secs: u64,
}

impl<'m, M: BaseModel> Policy<M::Event, M::OutputTx> for WithModel<'m, M> {
    #[inline(always)]
    fn handle(
        &mut self,
        inp: Input<M::Event>,
        model_health: &HealthFlag,
        output_tx: &mut M::OutputTx,
    ) -> ControlFlow<ControllerResult> {
        match Controller::<M>::handle_with_model(inp, self, model_health, output_tx) {
            ControlFlow::Continue(()) => ControlFlow::Continue(()),
            ControlFlow::Break(r) => ControlFlow::Break(r),
        }
    }
}

pub struct NoModel<'m, M: BaseModel> {
    pub c: &'m mut M::Config,
}

impl<'m, M: BaseModel> Policy<M::Event, M::OutputTx> for NoModel<'m, M> {
    #[inline(always)]
    fn handle(
        &mut self,
        inp: Input<M::Event>,
        _model_health: &HealthFlag,
        output_tx: &mut M::OutputTx,
    ) -> ControlFlow<ControllerResult> {
        match Controller::<M>::handle_no_model(inp, self.c, output_tx) {
            ControlFlow::Continue(()) => ControlFlow::Continue(()),
            ControlFlow::Break(r) => ControlFlow::Break(r),
        }
    }
}
