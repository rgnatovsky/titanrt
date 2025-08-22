use crate::config::RuntimeConfig;
use crate::control::controller::{Controller, ControllerResult};
use crate::control::inputs::{CommandInput, Input};
use crate::io::base::BaseTx;
use crate::io::ringbuffer::{RingBuffer, RingSender};
use crate::model::{BaseModel, ExecutionResult, StopKind};
use crate::utils::try_pin_core;
use crate::utils::CancelToken;
use anyhow::{anyhow, Result};

use signal_hook::consts::TERM_SIGNALS;
use signal_hook::flag;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{hint::spin_loop, thread, time::Duration};

/// Runtime wraps a model in a dedicated control thread.
/// It manages lifecycle, control-plane inputs, cancellation, and OS termination signals.
pub struct Runtime<Model: BaseModel> {
    /// Sender for control-plane inputs (events/commands).
    control_tx: RingSender<Input<Model::Event>>,
    /// Handle of the spawned control thread.
    join: Option<thread::JoinHandle<()>>,
    _phantom_data: PhantomData<Model>,
}

impl<Model: BaseModel> Runtime<Model> {
    /// Returns a mutable reference to the control-plane sender.
    pub fn control_tx(&mut self) -> &mut RingSender<Input<Model::Event>> {
        &mut self.control_tx
    }

    /// Blocks until the runtime thread finishes.
    pub fn run_blocking(mut self) -> Result<()> {
        if let Some(trading_loop) = self.join.take() {
            let _ = trading_loop.join();
        } else {
            return Err(anyhow!("trading_loop is None"));
        }
        Ok(())
    }

    /// Requests shutdown and waits for the thread to end.
    pub fn shutdown(mut self) {
        if let Some(join) = self.join.take() {
            self.control_tx
                .try_send(Input::Command(CommandInput::Shutdown))
                .ok();
            let _ = join.join();
        }
    }

    /// Wraps the runtime into a guard that auto-shuts down on drop.
    pub fn into_guard(self) -> RuntimeGuard<Model> {
        RuntimeGuard(Some(self))
    }

    /// Spawns the runtime and blocks until it ends.
    pub fn spawn_blocking(
        cfg: RuntimeConfig,
        model_ctx: Model::Ctx,
        model_cfg: Model::Config,
        output_tx: Model::OutputTx,
    ) -> Result<()> {
        let mut rt = Self::spawn(cfg, model_ctx, model_cfg, output_tx)?;
        if let Some(join) = rt.join.take() {
            let _ = join.join();
        }
        Ok(())
    }

    /// Spawns the runtime thread that drives the model lifecycle.
    pub fn spawn(
        cfg: RuntimeConfig,
        ctx: Model::Ctx,
        mut model_cfg: Model::Config,
        output_tx: Model::OutputTx,
    ) -> Result<Self> {
        let max_inputs_pending = cfg.max_inputs_pending.unwrap_or(1024);
        let max_inputs_drain = cfg.max_inputs_drain.unwrap_or(max_inputs_pending);
        let stop_model_timeout = cfg.stop_model_timeout.unwrap_or(300);

        let (control_tx, control_rx) = RingBuffer::bounded(max_inputs_pending);

        let join = thread::spawn(move || {
            // Termination flag from OS signals
            let term_flag = Arc::new(AtomicBool::new(false));
            for sig in TERM_SIGNALS {
                let _ = flag::register(*sig, term_flag.clone());
            }

            // Pin to a specific core if requested
            let core_id = if let Some(core_id) = cfg.core_id {
                match try_pin_core(core_id) {
                    Ok(core_id) => {
                        tracing::info!("[TradingRuntime] pinned to core: {}", core_id);
                        Some(core_id)
                    }
                    Err(e) => {
                        tracing::error!("[TradingRuntime] cannot pin core: {}", e);
                        panic!("[TradingRuntime] cannot pin core: {e}");
                    }
                }
            } else {
                None
            };

            let mut controller = Controller::new(control_rx);
            let cancel_token = CancelToken::new_root();

            // Optionally initialize the model at start
            let mut maybe_model: Option<Model> = if cfg.init_model_on_start {
                let model_cfg_clone = model_cfg.clone();
                match Model::initialize(
                    ctx.clone(),
                    model_cfg_clone,
                    core_id,
                    output_tx.clone(),
                    cancel_token.new_child(),
                ) {
                    Ok(model) => Some(model),
                    Err(e) => {
                        tracing::error!("[TradingRuntime] model init error: {}", e);
                        return;
                    }
                }
            } else {
                None
            };

            let mut idle: u32 = 0;

            loop {
                // Handle termination signals
                if term_flag.load(Ordering::Relaxed) {
                    tracing::warn!("[TradingRuntime] termination signal received");
                    if let Some(ref mut model) = maybe_model {
                        Controller::stop_model(model, StopKind::Shutdown, stop_model_timeout);
                    }
                    cancel_token.cancel();
                    break;
                }

                // Drain control-plane inputs
                match controller.drain_inputs(
                    max_inputs_drain,
                    maybe_model.as_mut(),
                    &mut model_cfg,
                    &cancel_token,
                    stop_model_timeout,
                ) {
                    ControllerResult::Empty => {}
                    ControllerResult::Processed => idle = 0,
                    ControllerResult::Disconnected => {
                        tracing::error!("[TradingRuntime] control disconnected");
                        break;
                    }
                    ControllerResult::InitModel => {
                        tracing::info!("[TradingRuntime] model init");
                        maybe_model = match Model::initialize(
                            ctx.clone(),
                            model_cfg.clone(),
                            core_id,
                            output_tx.clone(),
                            cancel_token.new_child(),
                        ) {
                            Ok(model) => Some(model),
                            Err(e) => {
                                tracing::error!("[TradingRuntime] model init error: {}", e);
                                None
                            }
                        };
                        idle = 0;
                    }
                }

                // Drive the model if present
                match maybe_model {
                    None => thread::sleep(Duration::from_micros(100)),
                    Some(ref mut model) => match model.execute() {
                        ExecutionResult::Continue => idle = 0,
                        ExecutionResult::Relax => {
                            idle = idle.saturating_add(1);
                            if idle < 64 {
                                spin_loop();
                            } else if idle < 256 {
                                thread::yield_now();
                            } else {
                                thread::sleep(Duration::from_micros(2));
                            }
                        }
                        ExecutionResult::Stop => {
                            tracing::info!("[TradingRuntime] model.execute stopped by itself");
                            Controller::stop_model(model, StopKind::Stop, stop_model_timeout);
                            maybe_model = None;
                        }
                        ExecutionResult::Shutdown => {
                            tracing::info!("[TradingRuntime] model.execute shutdown by itself");
                            Controller::stop_model(model, StopKind::Shutdown, stop_model_timeout);
                            break;
                        }
                    },
                }
            }
        });

        Ok(Self {
            control_tx,
            join: Some(join),
            _phantom_data: PhantomData,
        })
    }
}

/// Guard that auto-shuts down the runtime when dropped.
pub struct RuntimeGuard<M: BaseModel>(Option<Runtime<M>>);

impl<M: BaseModel> Drop for RuntimeGuard<M> {
    fn drop(&mut self) {
        if let Some(mut rt) = self.0.take() {
            rt.control_tx
                .try_send(Input::Command(CommandInput::Shutdown))
                .ok();
        }
    }
}

impl<M: BaseModel> Drop for Runtime<M> {
    fn drop(&mut self) {
        self.control_tx
            .try_send(Input::Command(CommandInput::Shutdown))
            .ok();
    }
}
