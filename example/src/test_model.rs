use crate::connector::{
    CompositeConfig, CompositeConnector, CounterAction, CounterDescriptor, CounterEvent,
};
use anyhow::anyhow;
use std::sync::{Arc, Mutex};
use std::thread;
use titanrt::connector::{BaseConnector, HookArgs, Stream};
use titanrt::control::inputs::InputMeta;
use titanrt::io::ringbuffer::{RingReceiver, RingSender};
use titanrt::prelude::*;
use titanrt::utils::{CancelToken, CorePickPolicy, StateMarker};

#[derive(Default)]
pub struct CounterState {
    pub _calls: u64,
}

impl StateMarker for CounterState {}

pub struct TestModel {
    _config: String,
    _reserved_core_id: Option<usize>,
    _outputs: NullOutputTx,
    _cancel_token: CancelToken,
    last_seq: u64,
    stream: Stream<RingSender<CounterAction>, RingReceiver<u64>, CounterState>,
    count: u64,
    ctx: NullModelCtx,
}

#[derive(Default, Clone)]
pub struct TestModelContext {
    pub count: Arc<Mutex<u64>>,
}

impl ModelContext for TestModelContext {}

pub(crate) fn my_counter_hook(args: HookArgs<CounterEvent, RingSender<u64>, CounterState, CounterDescriptor>) -> u64 {

    let depth_parsed = args.raw;

   args.state.publish(crate::test_model::CounterState {
       _calls: depth_parsed.actions_sum,
   });

   args.event_tx.try_send(depth_parsed.actions_sum).ok();

    let tx = args.event_tx;
    tx.try_send(5).ok();
    1
}

impl BaseModel for TestModel {
    type Config = String;
    type OutputTx = NullOutputTx;
    type OutputEvent = ();
    type Event = NullEvent;
    type Ctx = NullModelCtx;

    fn initialize(
        ctx: Self::Ctx,
        config: Self::Config,
        pinned_core_id: Option<usize>,
        outputs: NullOutputTx,
        cancel_token: CancelToken,
    ) -> anyhow::Result<Self> {
        let mut connector = CompositeConnector::init(
            CompositeConfig {
                with_core_stats: false,
                default_max_cores: None,
                specific_cores: vec![],
            },
            cancel_token.new_child(),
            pinned_core_id.map(|c| vec![c]),
        )?;

        let desc = CounterDescriptor::new(30, 0, Some(CorePickPolicy::Specific(9)));

        let stream = connector.spawn_stream(desc, my_counter_hook)?;

        while !stream.is_healthy() {
            thread::yield_now();
        }

        Ok(TestModel {
            _config: config,

            _reserved_core_id: pinned_core_id,
            _outputs: outputs,
            _cancel_token: cancel_token,
            stream,
            last_seq: 0,
            count: 0,
            ctx: NullModelCtx,
        })
    }

    fn execute(&mut self) -> ExecutionResult {
        self.count = self.count.saturating_add(1);

        if !self.stream.is_healthy() {
            tracing::info!("test counter is not healthy");
            tracing::info!("ticks {}", self.count / 30);
            return ExecutionResult::Shutdown;
        }

        self.stream
            .state()
            .with_if_changed(&mut self.last_seq, |_s| {
                // tracing::info!("after change calls counts {:?}", s.calls);
            });

        let mut events_count = 0;

        while let Ok(_e) = self.stream.try_recv() {
            events_count += 1;
        }

        self.stream
            .try_send(CounterAction { val: events_count })
            .ok();

        ExecutionResult::Continue
    }

    fn on_event(&mut self, _event: Self::Event, _meta: Option<InputMeta>) {}

    fn stop(&mut self, _stop_kind: StopKind) -> StopState {
        // tracing::info!(
        //     "test model stopped, events count: {}",
        //     self.ctx.count.lock().unwrap().to_string()
        // );

        StopState::Done
    }

    fn hot_reload(&mut self, _config: &Self::Config) -> anyhow::Result<()> {
        Err(anyhow!("not implemented"))
    }
}
