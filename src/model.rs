use crate::io::base::BaseTx;
use crate::utils::CancelToken;
use anyhow::Result;
use serde::Deserialize;
use std::any::Any;
use std::fmt::Debug;

pub trait ModelContext: Send + 'static + Clone {}
pub trait ModelEvent: Send + 'static + Clone {}

#[derive(Clone, Debug)]
pub struct NullModelCtx;

impl ModelContext for NullModelCtx {}

#[derive(Clone, Debug)]
pub struct NullEvent;

impl ModelEvent for NullEvent {}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum StopKind {
    Stop,
    Shutdown,
    Restart,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ExecutionResult {
    Stop,
    Shutdown,
    Continue,
    Relax,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum StopState {
    InProgress,
    Done,
}

pub enum Output<T: Send + 'static = ()> {
    Custom(T),
    Err(anyhow::Error),
}

pub trait BaseModel: Sized {
    type Config: Send + Clone + for<'a> Deserialize<'a> + Any;
    type OutputTx: BaseTx + Clone;
    type Event: ModelEvent;
    type Ctx: ModelContext;

    fn initialize(
        ctx: Self::Ctx,
        config: Self::Config,
        reserved_core_id: Option<usize>,
        output_tx: Self::OutputTx,
        cancel_token: CancelToken,
    ) -> Result<Self>;
    fn execute(&mut self) -> ExecutionResult;
    fn on_event(&mut self, event: Self::Event);
    fn stop(&mut self, kind: StopKind) -> StopState;
    fn hot_reload(&mut self, config: &Self::Config) -> Result<()> {
        let _ = config;
        Ok(())
    }
    fn json_command(&mut self, value: serde_json::Value) -> Result<()> {
        let _ = value;
        Ok(())
    }
}
