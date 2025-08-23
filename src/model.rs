use crate::io::base::BaseTx;
use crate::utils::CancelToken;
use anyhow::Result;
use serde::Deserialize;
use std::any::Any;
use std::fmt::Debug;

/// Marker for a clonable, sendable model context.
pub trait ModelContext: Send + 'static + Clone {}

/// Marker for a clonable, sendable model event.
pub trait ModelEvent: Send + 'static + Clone {}

/// Empty model context.
#[derive(Clone, Debug)]
pub struct NullModelCtx;
impl ModelContext for NullModelCtx {}

/// Empty event type.
#[derive(Clone, Debug)]
pub struct NullEvent;
impl ModelEvent for NullEvent {}

/// Requested stop semantics for the model.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum StopKind {
    /// Graceful stop; runtime stays alive.
    Stop,
    /// Full shutdown; runtime will terminate.
    Shutdown,
    /// Stop and re-initialize (restart).
    Restart,
}

/// Hint to the runtime about the next scheduling step.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ExecutionResult {
    /// Stop the model; keep runtime alive.
    Stop,
    /// Shutdown the runtime.
    Shutdown,
    /// Continue hot loop without yielding.
    Continue,
    /// Yield/relax (spin/yield/sleep backoff).
    Relax,
}

/// Progress indicator for cooperative stopping.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum StopState {
    /// Still stopping; call `stop` again later.
    InProgress,
    /// Fully stopped.
    Done,
}

/// Optional side-channel output from the model.
pub enum Output<T: Send + 'static = ()> {
    /// User-defined payload.
    Custom(T),
    /// Error to report upstream.
    Err(anyhow::Error),
}

/// Contract for application logic driven by the runtime.
pub trait BaseModel: Sized {
    /// Configuration type (serde-deserializable).
    type Config: Send + Clone + for<'a> Deserialize<'a> + Any;
    /// Output transport used by the model.
    type OutputTx: BaseTx + Clone;
    /// Primary event type consumed by the model.
    type Event: ModelEvent;
    /// Context passed on initialization.
    type Ctx: ModelContext;

    /// Construct the model instance.
    ///
    /// `reserved_core_id` is provided for affinity-aware setups;
    /// `cancel_token` is a child of the runtime root token.
    fn initialize(
        ctx: Self::Ctx,
        config: Self::Config,
        reserved_core_id: Option<usize>,
        output_tx: Self::OutputTx,
        cancel_token: CancelToken,
    ) -> Result<Self>;

    /// One tick of the model’s hot loop.
    fn execute(&mut self) -> ExecutionResult;

    /// Handle a typed event delivered to the model from outside of the runtime.
    fn on_event(&mut self, event: Self::Event);

    /// Cooperatively stop the model; can be realized through multiple calls.
    fn stop(&mut self, kind: StopKind) -> StopState;

    /// Apply a new configuration at runtime (optional).
    fn hot_reload(&mut self, config: &Self::Config) -> Result<()> {
        let _ = config;
        Ok(())
    }
    
}
