use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::hint::black_box;
use std::time::{Duration, Instant};

use reacton::io::mpmc::MpmcChannel;
use reacton::prelude::{
    BaseModel, BaseRx, BaseTx, ExecutionResult, NullEvent, NullModelCtx, Runtime, RuntimeConfig,
    StopKind, StopState,
};
use reacton::utils::CancelToken;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HotCfg {
    target_iters: u64,
}

#[derive(Debug, Clone)]
enum TestOut {
    Done,
}

struct HotModel {
    left: u64,
    done_tx: reacton::io::mpmc::MpmcSender<TestOut>,
    _cancel: CancelToken,
}

impl BaseModel for HotModel {
    type Config = HotCfg;
    type OutputTx = reacton::io::mpmc::MpmcSender<TestOut>;
    type Event = NullEvent;
    type Ctx = NullModelCtx;

    fn initialize(
        _ctx: Self::Ctx,
        cfg: Self::Config,
        _reserved_core_id: Option<usize>,
        outputs: Self::OutputTx,
        cancel_token: CancelToken,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            left: cfg.target_iters,
            done_tx: outputs,
            _cancel: cancel_token,
        })
    }

    #[inline(always)]
    fn execute(&mut self) -> ExecutionResult {
        if self.left == 0 {
            let _ = self.done_tx.try_send(TestOut::Done);
            return ExecutionResult::Stop;
        }
        self.left = black_box(self.left - 1);
        ExecutionResult::Continue
    }

    fn on_event(&mut self, _event: Self::Event) {}

    fn stop(&mut self, _kind: StopKind) -> StopState {
        StopState::Done
    }
}

fn run_hot_loop(total_iters: u64) -> Duration {
    let (out_tx, mut out_rx) = MpmcChannel::bounded::<TestOut>(8);

    let cfg = RuntimeConfig {
        init_model_on_start: true,
        core_id: Some(8),
        max_inputs_pending: Some(1024),
        max_inputs_drain: None,
        stop_model_timeout: Some(5),
    };
    let model_cfg = HotCfg {
        target_iters: total_iters,
    };

    let start = Instant::now();
    Runtime::<HotModel>::spawn_blocking(cfg, NullModelCtx, model_cfg, out_tx).unwrap();
    let elapsed = start.elapsed();

    let _ = out_rx.try_recv();

    elapsed
}

pub fn bench_hot_loop(c: &mut Criterion) {
    let mut group = c.benchmark_group("trading_runtime_hot_loop");

    for &iters in &[5_000_000_u64, 20_000_000_u64, 80_000_000_u64] {
        group.bench_function(BenchmarkId::from_parameter(iters), |b| {
            b.iter_custom(|n| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..n {
                    total += run_hot_loop(iters);
                }
                total
            });
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3))
        .sample_size(12);
    targets = bench_hot_loop
}
criterion_main!(benches);
