use crate::config::RuntimeConfig;
use crate::control::controller::{Controller, ControllerResult};
use crate::control::inputs::{CommandInput, Input};
use crate::io::base::BaseTx;
use crate::io::ringbuffer::{RingBuffer, RingSender};
use crate::model::{BaseModel, ExecutionResult, Output, StopKind};
use crate::utils::CancelToken;
use crate::utils::{HealthFlag, try_pin_core};
use anyhow::{Result, anyhow};
use signal_hook::consts::TERM_SIGNALS;
use signal_hook::flag;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::{hint::spin_loop, thread, time::Duration};

/// Runtime wraps a model in a dedicated control thread.
/// It manages lifecycle, control-plane inputs, cancellation, and OS termination signals.
pub struct Runtime<Model: BaseModel> {
    /// Sender for control-plane inputs (events/commands).
    control_tx: RingSender<Input<Model::Event>>,
    /// Handle of the spawned control thread.
    join: Option<JoinHandle<()>>,
    /// Health flag for the model.
    model_health: HealthFlag,
    _phantom_data: PhantomData<Model>,
}

impl<Model: BaseModel> Runtime<Model> {
    /// Returns true if the model is running.
    pub fn is_model_running(&self) -> bool {
        self.model_health.get()
    }
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
                .try_send(Input::command(CommandInput::Shutdown))
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
        mut output_tx: Model::OutputTx,
    ) -> Result<Self> {
        let max_inputs_pending = cfg.max_inputs_pending.unwrap_or(1024);
        let max_inputs_drain = cfg.max_inputs_drain.unwrap_or(max_inputs_pending);
        let stop_model_timeout = cfg.stop_model_timeout.unwrap_or(300);
        let model_health = HealthFlag::new(false);
        let (control_tx, control_rx) =
            RingBuffer::bounded::<Input<Model::Event>>(max_inputs_pending);

        let join: JoinHandle<()>;

        {
            let model_health = model_health.clone();

            join = thread::spawn(move || {
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
                        &ctx,
                        model_cfg_clone,
                        core_id,
                        output_tx.clone(),
                        cancel_token.new_child(),
                    ) {
                        Ok(model) => {
                            model_health.up();
                            Some(model)
                        }
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
                            Controller::stop_model(
                                model,
                                StopKind::Shutdown,
                                stop_model_timeout,
                                &mut output_tx,
                                None,
                            );
                        }
                        cancel_token.cancel();
                        break;
                    }

                    // Drain control-plane inputs
                    match controller.drain_inputs(
                        max_inputs_drain,
                        maybe_model.as_mut(),
                        &mut model_cfg,
                        &model_health,
                        &mut output_tx,
                        &cancel_token,
                        stop_model_timeout,
                    ) {
                        ControllerResult::Empty => {}
                        ControllerResult::Processed => idle = 0,
                        ControllerResult::Disconnected => {
                            tracing::error!("[TradingRuntime] control disconnected");
                            break;
                        }
                        ControllerResult::InitModel(maybe_corr_id) => {
                            tracing::info!("[TradingRuntime] model init");
                            maybe_model = match Model::initialize(
                                &ctx,
                                model_cfg.clone(),
                                core_id,
                                output_tx.clone(),
                                cancel_token.new_child(),
                            ) {
                                Ok(model) => {
                                    model_health.up();
                                    if let Some(corr_id) = maybe_corr_id {
                                        output_tx
                                            .try_send(Output::internal(corr_id, true, None))
                                            .ok();
                                    }
                                    Some(model)
                                }
                                Err(e) => {
                                    if let Some(corr_id) = maybe_corr_id {
                                        output_tx
                                            .try_send(Output::internal(
                                                corr_id,
                                                false,
                                                Some(e.to_string()),
                                            ))
                                            .ok();
                                    }
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
                                Controller::stop_model(
                                    model,
                                    StopKind::Stop,
                                    stop_model_timeout,
                                    &mut output_tx,
                                    None,
                                );
                                model_health.down();
                                maybe_model = None;
                            }
                            ExecutionResult::Shutdown => {
                                tracing::info!("[TradingRuntime] model.execute shutdown by itself");
                                Controller::stop_model(
                                    model,
                                    StopKind::Shutdown,
                                    stop_model_timeout,
                                    &mut output_tx,
                                    None,
                                );
                                model_health.down();
                                break;
                            }
                        },
                    }
                }
            });
        }

        Ok(Self {
            control_tx,
            join: Some(join),
            model_health,
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
                .try_send(Input::command(CommandInput::Shutdown))
                .ok();
        }
    }
}

impl<M: BaseModel> Drop for Runtime<M> {
    fn drop(&mut self) {
        self.control_tx
            .try_send(Input::command(CommandInput::Shutdown))
            .ok();
    }
}
