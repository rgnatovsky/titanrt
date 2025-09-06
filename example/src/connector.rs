use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::sync::Arc;
use titanrt::connector::errors::StreamResult;
use titanrt::connector::{
    BaseConnector, Hook, HookArgs, IntoHook, Kind, RuntimeCtx, StreamDescriptor, StreamRunner,
    StreamSpawner, Venue,
};
use titanrt::io::ringbuffer::RingSender;
use titanrt::prelude::{BaseRx, BaseTx, TxPairExt};
use titanrt::utils::{CancelToken, CorePickPolicy, CoreStats, Reducer, StateMarker};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CompositeConfig {
    pub default_max_cores: Option<usize>,
    pub specific_cores: Vec<usize>,
    pub with_core_stats: bool,
}

pub struct CompositeConnector {
    pub(crate) config: CompositeConfig,
    pub(crate) cancel_token: CancelToken,
    pub(crate) core_stats: Option<Arc<CoreStats>>,
}

impl BaseConnector for CompositeConnector {
    type MainConfig = CompositeConfig;

    fn init(
        config: Self::MainConfig,
        cancel_token: CancelToken,
        reserved_core_ids: Option<Vec<usize>>,
    ) -> anyhow::Result<Self> {
        let core_stats = if config.with_core_stats {
            Some(CoreStats::new(
                config.default_max_cores,
                config.specific_cores.clone(),
                reserved_core_ids.unwrap_or_default(),
            )?)
        } else {
            None
        };

        Ok(Self {
            config,
            cancel_token,
            core_stats,
        })
    }

    fn name(&self) -> impl AsRef<str> + Display {
        "CompositeConnector"
    }

    fn config(&self) -> &Self::MainConfig {
        &self.config
    }

    fn cancel_token(&self) -> &CancelToken {
        &self.cancel_token
    }

    fn cores_stats(&self) -> Option<Arc<CoreStats>> {
        self.core_stats.clone()
    }
}

#[derive(Debug, Default, Clone)]
pub struct CounterEvent {
    pub actions_sum: u64,
}
#[derive(Clone)]
pub struct CounterAction {
    pub val: u64,
}

#[derive(Debug, Clone)]
pub struct CounterDescriptor {
    pub running_time_secs: u64,
    pub sleep_interval_sec: u64,
    pub core_pick_policy: Option<CorePickPolicy>,
}

impl CounterDescriptor {
    pub fn new(
        running_time_secs: u64,
        sleep_interval_sec: u64,
        core_pick_policy: Option<CorePickPolicy>,
    ) -> Self {
        Self {
            running_time_secs,
            sleep_interval_sec,
            core_pick_policy,
        }
    }
}

impl StreamDescriptor for CounterDescriptor {
    fn venue(&self) -> impl Venue {
        "test"
    }

    fn kind(&self) -> impl Kind {
        "counter"
    }

    fn max_pending_actions(&self) -> Option<usize> {
        Some(1024)
    }

    fn max_pending_events(&self) -> Option<usize> {
        Some(1024)
    }

    fn core_pick_policy(&self) -> Option<CorePickPolicy> {
        self.core_pick_policy
    }

    fn health_at_start(&self) -> bool {
        true
    }
}

impl<E, R, S> StreamSpawner<CounterDescriptor, E, R, S> for CompositeConnector
where
    S: StateMarker,
    E: BaseTx + TxPairExt,
    R: Reducer,
{
}

impl<E, R, S> StreamRunner<CounterDescriptor, E, R, S> for CompositeConnector
where
    E: BaseTx,
    S: StateMarker,
    R: Reducer,
{
    type Config = ();
    type ActionTx = RingSender<CounterAction>;
    type RawEvent = CounterEvent;
    type HookResult = u64;
    // type Hook = fn(&CounterEvent, &mut E, &StateCell<S>);

    fn build_config(&mut self, _desc: &CounterDescriptor) -> anyhow::Result<Self::Config> {
        Ok(())
    }

    fn run<H>(
        mut ctx: RuntimeCtx<Self::Config, CounterDescriptor, Self::ActionTx, E, R, S>,
        hook: H,
    ) -> StreamResult<()>
    where
        H: IntoHook<Self::RawEvent, E, R, S, CounterDescriptor, Self::HookResult>,
    {
        let mut hook = hook.into_hook();

        let start_at = std::time::Instant::now();
        let mut event = CounterEvent { actions_sum: 0 };
        let sleeping_interval = ctx.desc.sleep_interval_sec;

        loop {
            if ctx.cancel.is_cancelled() {
                break;
            }
            if start_at.elapsed().as_secs() > ctx.desc.running_time_secs {
                ctx.health.down();
                break;
            }

            let hook_args = HookArgs::new(
                event.clone(),
                &mut ctx.event_tx,
                &mut ctx.reducer,
                &ctx.state,
                &ctx.desc,
                &ctx.health,
            );

            hook.call(hook_args);

            while let Ok(action) = ctx.action_rx.try_recv() {
                event.actions_sum += action.val;
            }

            std::thread::sleep(std::time::Duration::from_secs(sleeping_interval));
        }

        Ok(())
    }
}
