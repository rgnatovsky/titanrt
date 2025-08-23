use crate::control::inputs::{CommandInput, Input, InputMeta, InputPayload};
use crate::control::policy::{NoModel, Policy, WithModel};
use crate::error::TryRecvError;
use crate::io::base::BaseRx;
use crate::io::ringbuffer::RingReceiver;
use crate::model::{BaseModel, Output, StopKind, StopState};
use crate::prelude::BaseTx;
use crate::utils::{CancelToken, HealthFlag};
use serde_json::Value;
use std::ops::ControlFlow;
use std::thread;
use std::time::Duration;
use uuid::Uuid;

/// Outcome of a single control-plane drain cycle.
#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy)]
pub enum ControllerResult {
    /// No inputs were available.
    Empty,
    /// Inputs were processed successfully.
    Processed,
    /// A new model should be initialized.
    InitModel(Option<Uuid>),
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
        output_tx: &mut M::OutputTx,
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
                match Self::handle_with_model(inp, &mut with, model_health, output_tx) {
                    ControlFlow::Continue(()) => (),
                    ControlFlow::Break(r) => return r,
                }
                self.drain_loop(max, with, model_health, output_tx)
            }
            None => {
                match Self::handle_no_model(inp, model_cfg, output_tx) {
                    ControlFlow::Continue(()) => (),
                    ControlFlow::Break(r) => return r,
                };
                self.drain_loop(max, NoModel::<M> { c: model_cfg }, model_health, output_tx)
            }
        }
    }

    /// Internal loop to process additional inputs via a [`Policy`].
    #[inline(always)]
    fn drain_loop<P: Policy<M::Event, M::OutputTx>>(
        &mut self,
        max: usize,
        mut policy: P,
        model_health: &HealthFlag,
        output_tx: &mut M::OutputTx,
    ) -> ControllerResult {
        for _ in 1..max {
            match self.control_rx.try_recv() {
                Ok(inp) => {
                    if let ControlFlow::Break(r) = policy.handle(inp, model_health, output_tx) {
                        return r;
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return ControllerResult::Disconnected,
            }
        }

        ControllerResult::Processed
    }

    /// Handle a control InputPayload when no model is running.
    #[inline(always)]
    pub(super) fn handle_no_model(
        input: Input<M::Event>,
        model_cfg: &mut M::Config,
        output_tx: &mut M::OutputTx,
    ) -> ControlFlow<ControllerResult> {
        match input.payload {
            InputPayload::Command(cmd) => {
                match cmd {
                    // With no model, Start/Restart means "init model".
                    CommandInput::Start | CommandInput::Restart => {
                        ControlFlow::Break(ControllerResult::InitModel(input.meta.map(|m| m.id)))
                    }

                    // Hard signals — exit runtime.
                    CommandInput::Shutdown | CommandInput::Kill => {
                        if let Some(meta) = input.meta.as_ref() {
                            output_tx
                                .try_send(Output::internal(meta.id, true, None))
                                .ok();
                        }
                        ControlFlow::Break(ControllerResult::Disconnected)
                    }

                    // Stop ignored if no model is running.
                    CommandInput::Stop => {
                        if let Some(meta) = input.meta.as_ref() {
                            output_tx
                                .try_send(Output::internal(
                                    meta.id,
                                    false,
                                    Some("model is not running".to_string()),
                                ))
                                .ok();
                        }
                        ControlFlow::Continue(())
                    }

                    // Apply config updates only to the cfg.
                    CommandInput::HotReload(v) => {
                        Self::hot_reload(None, model_cfg, v, output_tx, input.meta);
                        ControlFlow::Continue(())
                    }
                }
            }
            InputPayload::Event(_) => ControlFlow::Continue(()),
        }
    }

    /// Handle a control InputPayload when a model is running.
    #[inline(always)]
    pub(super) fn handle_with_model(
        input: Input<M::Event>,
        with: &mut WithModel<M>,
        model_health: &HealthFlag,
        output_tx: &mut M::OutputTx,
    ) -> ControlFlow<ControllerResult> {
        match input.payload {
            InputPayload::Command(cmd) => match cmd {
                CommandInput::Start => {
                    if let Some(meta) = input.meta.as_ref() {
                        output_tx
                            .try_send(Output::internal(
                                meta.id,
                                false,
                                Some("model already running".to_string()),
                            ))
                            .ok();
                    }

                    ControlFlow::Continue(())
                }
                CommandInput::Stop => {
                    tracing::info!("[TradingRuntime] stop signal received");
                    Self::stop_model(
                        with.model,
                        StopKind::Stop,
                        with.stop_timeout_secs,
                        output_tx,
                        input.meta,
                    );
                    model_health.down();

                    ControlFlow::Continue(())
                }
                CommandInput::Restart => {
                    tracing::info!("[TradingRuntime] restart signal received");
                    Self::stop_model(
                        with.model,
                        StopKind::Restart,
                        with.stop_timeout_secs,
                        output_tx,
                        None,
                    );
                    model_health.down();
                    ControlFlow::Break(ControllerResult::InitModel(input.meta.map(|m| m.id)))
                }
                CommandInput::Shutdown => {
                    tracing::info!("[TradingRuntime] shutdown signal received");
                    Self::stop_model(
                        with.model,
                        StopKind::Shutdown,
                        with.stop_timeout_secs,
                        output_tx,
                        input.meta,
                    );
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
                    Self::hot_reload(Some(with.model), with.cfg, v, output_tx, input.meta);
                    ControlFlow::Continue(())
                }
            },
            InputPayload::Event(e) => {
                with.model.on_event(e, input.meta);
                ControlFlow::Continue(())
            }
        }
    }

    /// Apply config update, optionally calling the model’s `hot_reload`.
    pub(crate) fn hot_reload(
        model: Option<&mut M>,
        model_cfg: &mut M::Config,
        raw_value: Value,
        output_tx: &mut M::OutputTx,
        meta: Option<InputMeta>,
    ) {
        match serde_json::from_value::<M::Config>(raw_value) {
            Ok(new_config) => {
                if let Some(model) = model {
                    match model.hot_reload(&new_config) {
                        Ok(()) => {
                            *model_cfg = new_config;
                            if let Some(meta) = meta {
                                output_tx
                                    .try_send(Output::internal(meta.id, true, None))
                                    .ok();
                            }
                        }
                        Err(e) => {
                            if let Some(meta) = meta {
                                output_tx
                                    .try_send(Output::internal(
                                        meta.id,
                                        false,
                                        Some(format!("config update failed: {}", e)),
                                    ))
                                    .ok();
                            }
                        }
                    }
                } else {
                    *model_cfg = new_config;
                    if let Some(meta) = meta {
                        output_tx
                            .try_send(Output::internal(meta.id, true, None))
                            .ok();
                    }
                }
            }
            Err(e) => {
                if let Some(meta) = meta {
                    output_tx
                        .try_send(Output::internal(
                            meta.id,
                            false,
                            Some(format!("config update failed: {}", e)),
                        ))
                        .ok();
                }
            }
        }
    }

    /// Drive the model’s `stop` method until done or timeout.
    pub(crate) fn stop_model(
        model: &mut M,
        stop_kind: StopKind,
        timeout_sec: u64,
        output_tx: &mut M::OutputTx,
        meta: Option<InputMeta>,
    ) {
        let start = std::time::Instant::now();
        let mut by_timeout = false;

        while model.stop(stop_kind) == StopState::InProgress {
            thread::sleep(Duration::from_millis(100));
            if start.elapsed().as_secs() > timeout_sec {
                by_timeout = true;
                break;
            }
        }

        if let Some(meta) = meta {
            output_tx
                .try_send(Output::internal(
                    meta.id,
                    true,
                    if by_timeout {
                        Some("timeout".to_string())
                    } else {
                        None
                    },
                ))
                .ok();
        }
    }
}
