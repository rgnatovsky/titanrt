use crate::control::inputs::{CommandInput, Input};
use crate::control::policy::{NoModel, Policy, WithModel};
use crate::error::TryRecvError;
use crate::io::base::BaseRx;
use crate::io::ringbuffer::RingReceiver;
use crate::model::{BaseModel, StopKind, StopState};
use crate::utils::{CancelToken, HealthFlag};
use serde_json::Value;
use std::ops::ControlFlow;
use std::thread;
use std::time::Duration;

/// Outcome of a single control-plane drain cycle.
#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy)]
pub enum ControllerResult {
    /// No inputs were available.
    Empty,
    /// Inputs were processed successfully.
    Processed,
    /// A new model should be initialized.
    InitModel,
    /// Control channel is disconnected or runtime should exit.
    Disconnected,
}

/// Control-plane driver that consumes [`Input`]s and applies them
/// to a model (if running) or a config (if not).
pub struct Controller<M: BaseModel> {
    control_rx: RingReceiver<Input<M::Event>>,
}

impl<M: BaseModel> Controller<M> {
    /// Create a new controller over a control channel receiver.
    pub fn new(control_rx: RingReceiver<Input<M::Event>>) -> Self {
        Self { control_rx }
    }

    /// Drain up to `max` inputs and apply them according to whether
    /// a model is present or not. Returns a [`ControllerResult`] hint.
    #[inline(always)]
    pub fn drain_inputs(
        &mut self,
        max: usize,
        maybe_model: Option<&mut M>,
        model_cfg: &mut M::Config,
        model_health: &HealthFlag,
        cancel: &CancelToken,
        stop_timeout_secs: u64,
    ) -> ControllerResult {
        let inp = match self.control_rx.try_recv() {
            Ok(inp) => inp,
            Err(TryRecvError::Empty) => return ControllerResult::Empty,
            Err(TryRecvError::Disconnected) => return ControllerResult::Disconnected,
        };

        match maybe_model {
            Some(model) => {
                let mut with = WithModel {
                    model,
                    cfg: model_cfg,
                    cancel,
                    stop_timeout_secs,
                };
                match Self::handle_with_model(inp, &mut with, model_health) {
                    ControlFlow::Continue(()) => (),
                    ControlFlow::Break(r) => return r,
                }
                self.drain_loop(max, with, model_health)
            }
            None => {
                match Self::handle_no_model(inp, model_cfg) {
                    ControlFlow::Continue(()) => (),
                    ControlFlow::Break(r) => return r,
                };
                self.drain_loop(max, NoModel::<M> { c: model_cfg }, model_health)
            }
        }
    }

    /// Internal loop to process additional inputs via a [`Policy`].
    #[inline(always)]
    fn drain_loop<P: Policy<M::Event>>(
        &mut self,
        max: usize,
        mut policy: P,
        model_health: &HealthFlag,
    ) -> ControllerResult {
        for _ in 1..max {
            match self.control_rx.try_recv() {
                Ok(inp) => {
                    if let ControlFlow::Break(r) = policy.handle(inp, model_health) {
                        return r;
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return ControllerResult::Disconnected,
            }
        }

        ControllerResult::Processed
    }

    /// Handle a control input when no model is running.
    #[inline(always)]
    pub(super) fn handle_no_model(
        input: Input<M::Event>,
        model_cfg: &mut M::Config,
    ) -> ControlFlow<ControllerResult> {
        match input {
            Input::Command(cmd) => {
                match cmd {
                    // With no model, Start/Restart means "init model".
                    CommandInput::Start | CommandInput::Restart => {
                        ControlFlow::Break(ControllerResult::InitModel)
                    }

                    // Hard signals — exit runtime.
                    CommandInput::Shutdown | CommandInput::Kill => {
                        ControlFlow::Break(ControllerResult::Disconnected)
                    }

                    // Stop/Json ignored if no model is running.
                    CommandInput::Stop => {
                        tracing::warn!(
                            "[TradingRuntime] ignoring command input without model: {:?}",
                            cmd
                        );
                        ControlFlow::Continue(())
                    }

                    // Apply config updates only to the cfg.
                    CommandInput::HotReload(v) => {
                        Self::hot_reload(None, model_cfg, v);
                        ControlFlow::Continue(())
                    }
                }
            }
            Input::Event(_) => ControlFlow::Continue(()),
        }
    }

    /// Handle a control input when a model is running.
    #[inline(always)]
    pub(super) fn handle_with_model(
        input: Input<M::Event>,
        with: &mut WithModel<M>,
        model_health: &HealthFlag,
    ) -> ControlFlow<ControllerResult> {
        match input {
            Input::Command(cmd) => match cmd {
                CommandInput::Start => {
                    tracing::info!(
                        "[TradingRuntime] ignoring start signal - model already running"
                    );
                    ControlFlow::Continue(())
                }
                CommandInput::Stop => {
                    tracing::info!("[TradingRuntime] stop signal received");
                    Self::stop_model(with.model, StopKind::Stop, with.stop_timeout_secs);
                    model_health.down();
                    ControlFlow::Continue(())
                }
                CommandInput::Restart => {
                    tracing::info!("[TradingRuntime] restart signal received");
                    Self::stop_model(with.model, StopKind::Restart, with.stop_timeout_secs);
                    model_health.down();
                    ControlFlow::Break(ControllerResult::InitModel)
                }
                CommandInput::Shutdown => {
                    tracing::info!("[TradingRuntime] shutdown signal received");
                    Self::stop_model(with.model, StopKind::Shutdown, with.stop_timeout_secs);
                    model_health.down();
                    ControlFlow::Break(ControllerResult::Disconnected)
                }
                CommandInput::Kill => {
                    tracing::info!("[TradingRuntime] kill signal received");
                    with.cancel.cancel();
                    model_health.down();
                    ControlFlow::Break(ControllerResult::Disconnected)
                }
                CommandInput::HotReload(v) => {
                    Self::hot_reload(Some(with.model), with.cfg, v);
                    ControlFlow::Continue(())
                }
            },
            Input::Event(e) => {
                with.model.on_event(e);
                ControlFlow::Continue(())
            }
        }
    }

    /// Apply config update, optionally calling the model’s `hot_reload`.
    pub(crate) fn hot_reload(model: Option<&mut M>, model_cfg: &mut M::Config, raw_value: Value) {
        match serde_json::from_value::<M::Config>(raw_value) {
            Ok(new_config) => {
                if let Some(model) = model {
                    match model.hot_reload(&new_config) {
                        Ok(()) => {
                            *model_cfg = new_config;
                        }
                        Err(e) => {
                            tracing::error!("[TradingRuntime] model hot reload error: {}", e);
                        }
                    };
                } else {
                    *model_cfg = new_config;
                }
            }
            Err(e) => {
                tracing::error!("[TradingRuntime] hot reload parsing error: {}", e);
            }
        }
    }

    /// Drive the model’s `stop` method until done or timeout.
    pub(crate) fn stop_model(model: &mut M, stop_kind: StopKind, timeout_sec: u64) {
        let start = std::time::Instant::now();
        let mut by_timeout = false;

        while model.stop(stop_kind) == StopState::InProgress {
            thread::sleep(Duration::from_millis(100));
            if start.elapsed().as_secs() > timeout_sec {
                by_timeout = true;
                break;
            }
        }

        tracing::info!(
            "[TradingRuntime] model stopped {}",
            if by_timeout {
                "by timeout"
            } else {
                "as normal"
            }
        );
    }
}
