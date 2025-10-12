use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use titanrt::control::inputs::InputMeta;
use titanrt::io::mpmc::MpmcChannel;
use titanrt::model::Output;
use titanrt::prelude::{
    BaseModel, BaseRx, BaseTx, ExecutionResult, NullModelCtx, NullModelEvent, Runtime,
    RuntimeConfig, StopKind, StopState,
};
use titanrt::utils::CancelToken;

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
    done_tx: titanrt::io::mpmc::MpmcSender<Output<TestOut>>,
    _cancel: CancelToken,
}

impl BaseModel for HotModel {
    type Config = HotCfg;
    type OutputTx = titanrt::io::mpmc::MpmcSender<Output<TestOut>>;
    type OutputEvent = TestOut;
    type Event = NullModelEvent;
    type Ctx = NullModelCtx;

    fn initialize(
        cfg: Self::Config,
        _ctx: &Self::Ctx,
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
            let _ = self.done_tx.try_send(Output::generic(TestOut::Done));
            return ExecutionResult::Shutdown;
        }
        self.left = black_box(self.left - 1);
        ExecutionResult::Continue
    }

    fn on_event(&mut self, _event: Self::Event, _meta: Option<InputMeta>) {}

    fn stop(&mut self, _kind: StopKind) -> StopState {
        StopState::Done
    }

    fn hot_reload(&mut self, _config: &Self::Config) -> anyhow::Result<()> {
        Ok(())
    }
}

fn run_hot_loop(total_iters: u64) -> Duration {
    let (out_tx, mut out_rx) = MpmcChannel::bounded::<Output<TestOut>>(8);

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

    for &iters in &[5_000_000_u64, 20_000_000_u64, 100_000_000_u64] {
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
