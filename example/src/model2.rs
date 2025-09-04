use crate::connector::{CompositeConfig, CompositeConnector, CounterAction, CounterDescriptor};
use crate::test_model::CounterState;
use serde::Deserialize;
use titanrt::connector::{BaseConnector, Stream};
use titanrt::control::inputs::InputMeta;
use titanrt::io::mpmc::MpmcSender;
use titanrt::io::ringbuffer::{RingReceiver, RingSender};
use titanrt::model::{ExecutionResult, NullEvent, NullModelCtx, Output, StopKind, StopState};
use titanrt::prelude::{BaseModel, BaseTx};
use titanrt::utils::{CancelToken, CorePickPolicy};

#[derive(Clone)]
pub enum MyOutput {
    Grafana(u64),
}
#[derive(Default, Deserialize, Clone)]
pub struct TestModel2Config {}
pub struct TestModel2 {
    depth_stream: Stream<RingSender<CounterAction>, RingReceiver<u64>, CounterState>,
    last_depth_value: u64,
}

impl BaseModel for TestModel2 {
    type Config = TestModel2Config;
    type OutputTx = MpmcSender<Output<MyOutput>>;
    type OutputEvent = MyOutput;
    type Event = NullEvent;
    type Ctx = NullModelCtx;

    fn initialize(
        ctx: Self::Ctx,
        config: Self::Config,
        reserved_core_id: Option<usize>,
        output_tx: Self::OutputTx,
        cancel_token: CancelToken,
    ) -> anyhow::Result<Self> {
        let mut connector = CompositeConnector::init(
            CompositeConfig {
                with_core_stats: false,
                default_max_cores: None,
                specific_cores: vec![],
            },
            cancel_token.new_child(),
            None,
        )?;

        let desc = CounterDescriptor::new(30, 0, Some(CorePickPolicy::Specific(9)));

        let depth_stream = connector.spawn_stream(desc, crate::test_model::my_counter_hook)?;

        Ok(Self {
            depth_stream,
            last_depth_value: 0,
        })
    }

    fn execute(&mut self) -> ExecutionResult {
        self.depth_stream.try_send(CounterAction { val: 1 }).ok();
        self.depth_stream.cancel();

        while let Ok(h) = self.depth_stream.try_recv() {}

        let state_now = self.depth_stream.state().peek_if_changed(&mut self.last_depth_value);

        ExecutionResult::Continue
    }

    fn on_event(&mut self, _event: Self::Event, _meta: Option<InputMeta>) {}

    fn stop(&mut self, _kind: StopKind) -> StopState {
        StopState::InProgress
    }

    fn hot_reload(&mut self, config: &Self::Config) -> anyhow::Result<()> {
        Ok(())
    }
}
