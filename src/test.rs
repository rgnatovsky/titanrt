#[cfg(test)]
mod tests {
    use crate::config::RuntimeConfig;
    use crate::control::inputs::{CommandInput, Input};
    use crate::io::base::{BaseRx, BaseTx};
    use crate::io::mpmc::{MpmcChannel, MpmcReceiver, MpmcSender};
    use crate::model::{BaseModel, ExecutionResult, NullModelCtx, StopKind, StopState};
    use crate::runtime::Runtime;
    use crate::utils::*;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::thread;
    use std::time::{Duration, Instant};

    #[derive(Debug, Clone)]
    enum TestOut {
        Tick(usize),
        Done,
    }

    #[derive(Debug, Clone)]
    enum TestEvent {
        Ping(u32),
    }

    impl crate::model::ModelEvent for TestEvent {}

    // ---- Конфиг тестовой модели
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TickCfg {
        ticks: usize,       // сколько раз execute перед Stop
        relax_every: usize, // каждые k вызовов -> Relax
    }

    struct TickModel {
        remain: usize,
        relax_every: usize,
        out_tx: MpmcSender<TestOut>,
        _cancel: CancelToken,
    }

    impl BaseModel for TickModel {
        type Config = TickCfg;
        type OutputTx = MpmcSender<TestOut>;
        type Event = TestEvent;
        type Ctx = NullModelCtx;

        fn initialize(
            _ctx: Self::Ctx,
            cfg: Self::Config,
            _reserved_core_id: Option<usize>,
            output_tx: Self::OutputTx,
            cancel_token: CancelToken,
        ) -> anyhow::Result<Self> {
            Ok(Self {
                remain: cfg.ticks,
                relax_every: cfg.relax_every.max(1),
                out_tx: output_tx,
                _cancel: cancel_token,
            })
        }

        fn execute(&mut self) -> ExecutionResult {
            if self.remain == 0 {
                let _ = self.out_tx.try_send(TestOut::Done);
                return ExecutionResult::Shutdown;
            }

            let cur = self.remain;
            self.remain = self.remain.saturating_sub(1);
            let _ = self.out_tx.try_send(TestOut::Tick(cur));

            if cur % self.relax_every == 0 {
                ExecutionResult::Relax
            } else {
                ExecutionResult::Continue
            }
        }

        fn on_event(&mut self, event: Self::Event) {
            // Для теста просто сигнализируем о получении события
            match event {
                TestEvent::Ping(v) => {
                    let _ = self.out_tx.try_send(TestOut::Tick(900 + v as usize));
                }
            }
        }

        fn stop(&mut self, _kind: StopKind) -> StopState {
            // моментально останавливаемся
            StopState::Done
        }

        fn hot_reload(&mut self, config: &Self::Config) -> anyhow::Result<()> {
            // применяем новый конфиг на лету
            self.remain = config.ticks;
            self.relax_every = config.relax_every.max(1);
            // маркер, что hot_reload прошёл
            let _ = self.out_tx.try_send(TestOut::Tick(555));
            Ok(())
        }
    }

    // ---- helper: дождаться Done из out_rx без активного спина
    fn recv_done_within(rx: &mut MpmcReceiver<TestOut>, dur: Duration) -> bool {
        let start = Instant::now();
        loop {
            if start.elapsed() > dur {
                return false;
            }
            match rx.try_recv() {
                Ok(TestOut::Done) => return true,
                Ok(TestOut::Tick(_count)) => continue,
                Err(_) => thread::sleep(Duration::from_micros(100)),
            }
        }
    }

    fn recv_tick_within(rx: &mut MpmcReceiver<TestOut>, expect: usize, dur: Duration) -> bool {
        let start = Instant::now();
        loop {
            if start.elapsed() > dur {
                return false;
            }
            match rx.try_recv() {
                Ok(TestOut::Tick(v)) if v == expect => return true,
                Ok(_) => continue,
                Err(_) => thread::sleep(Duration::from_micros(100)),
            }
        }
    }

    #[test]
    fn runtime_autostarts_and_stops() {
        let (out_tx, mut out_rx) = MpmcChannel::bounded::<TestOut>(64);

        let cfg = RuntimeConfig {
            init_model_on_start: true,
            core_id: None,
            max_inputs_pending: Some(128),
            max_inputs_drain: None,
            stop_model_timeout: Some(5),
        };

        let model_cfg = TickCfg {
            ticks: 8,
            relax_every: 3,
        };

        Runtime::<TickModel>::spawn_blocking(cfg, NullModelCtx, model_cfg, out_tx)
            .expect("spawn_blocking failed");

        assert!(recv_done_within(&mut out_rx, Duration::from_secs(1)));
    }

    #[test]
    fn runtime_manual_init_then_stops() {
        let (out_tx, mut out_rx) = MpmcChannel::bounded::<TestOut>(64);

        let cfg = RuntimeConfig {
            init_model_on_start: false,
            core_id: None,
            max_inputs_pending: Some(128),
            max_inputs_drain: None,
            stop_model_timeout: Some(5),
        };

        let model_cfg = TickCfg {
            ticks: 5,
            relax_every: 2,
        };

        let mut rt = Runtime::<TickModel>::spawn(cfg, NullModelCtx, model_cfg, out_tx)
            .expect("spawn failed");

        // Триггерим инициализацию модели через контрол-команду
        rt.control_tx()
            .try_send(Input::Command(CommandInput::Start))
            .expect("control try_send failed");

        rt.run_blocking().expect("join failed");
        assert!(recv_done_within(&mut out_rx, Duration::from_secs(1)));
    }

    #[test]
    fn runtime_guard_sends_shutdown_on_drop() {
        let (out_tx, _out_rx) = MpmcChannel::bounded::<TestOut>(8);

        let cfg = RuntimeConfig {
            init_model_on_start: false,
            core_id: None,
            max_inputs_pending: Some(32),
            max_inputs_drain: None,
            stop_model_timeout: Some(5),
        };

        let model_cfg = TickCfg {
            ticks: 1,
            relax_every: 1,
        };
        let rt = Runtime::<TickModel>::spawn(cfg, NullModelCtx, model_cfg, out_tx)
            .expect("spawn failed");

        // Drop у guard должен отправить Shutdown и не паниковать
        let _guard = rt.into_guard();
    }

    #[test]
    fn runtime_hotreload_before_start_applies_to_config() {
        let (out_tx, mut out_rx) = MpmcChannel::bounded::<TestOut>(64);

        let cfg = RuntimeConfig {
            init_model_on_start: false,
            core_id: None,
            max_inputs_pending: Some(64),
            max_inputs_drain: None,
            stop_model_timeout: Some(5),
        };

        let model_cfg = TickCfg {
            ticks: 10,
            relax_every: 4,
        };
        let mut rt = Runtime::<TickModel>::spawn(cfg, NullModelCtx, model_cfg, out_tx)
            .expect("spawn failed");

        // Обновляем конфиг до очень маленького количества тиков ещё до старта
        rt.control_tx()
            .try_send(Input::Command(CommandInput::HotReload(
                json!({"ticks": 1, "relax_every": 1}),
            )))
            .expect("hotreload send failed");

        // Теперь запускаем
        rt.control_tx()
            .try_send(Input::Command(CommandInput::Start))
            .expect("start send failed");

        rt.run_blocking().expect("join failed");
        assert!(recv_done_within(&mut out_rx, Duration::from_secs(1)));
    }

    #[test]
    fn runtime_kill_exits_quickly() {
        let (out_tx, _out_rx) = MpmcChannel::bounded::<TestOut>(16);
        let cfg = RuntimeConfig {
            init_model_on_start: true,
            core_id: None,
            max_inputs_pending: Some(32),
            max_inputs_drain: None,
            stop_model_timeout: Some(5),
        };
        let model_cfg = TickCfg {
            ticks: 1000,
            relax_every: 3,
        };

        let mut rt = Runtime::<TickModel>::spawn(cfg, NullModelCtx, model_cfg, out_tx)
            .expect("spawn failed");
        rt.control_tx()
            .try_send(Input::Command(CommandInput::Kill))
            .expect("kill send failed");
        // Если kill обработан, поток рантайма завершится без паники
        rt.run_blocking().expect("join failed");
    }

    #[test]
    fn runtime_delivers_events_to_model() {
        let (out_tx, mut out_rx) = MpmcChannel::bounded::<TestOut>(64);
        let cfg = RuntimeConfig {
            init_model_on_start: true,
            core_id: None,
            max_inputs_pending: Some(64),
            max_inputs_drain: None,
            stop_model_timeout: Some(5),
        };
        let model_cfg = TickCfg {
            ticks: 3,
            relax_every: 10,
        };

        let mut rt = Runtime::<TickModel>::spawn(cfg, NullModelCtx, model_cfg, out_tx)
            .expect("spawn failed");
        // Отправим событие и ждём маркер 900 + 1 = 901
        rt.control_tx()
            .try_send(Input::Event(TestEvent::Ping(1)))
            .expect("event send failed");

        assert!(recv_tick_within(&mut out_rx, 901, Duration::from_secs(1)));
        rt.run_blocking().expect("join failed");
    }
}
