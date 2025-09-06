use serde::Deserialize;
use std::thread::sleep;
use std::time::Instant;
use titanrt::connector::features::reqwest::connector::{ReqwestConnector, ReqwestConnectorConfig};
use titanrt::connector::features::reqwest::rate_limiter::{
    RateLimitConfig, RateLimitContext, RateLimitRule, RateLimitScope, RateLimitType,
};
use titanrt::connector::features::reqwest::stream::actions::{ReqwestAction, Url};
use titanrt::connector::features::reqwest::stream::descriptor::ReqwestStreamDescriptor;
use titanrt::connector::features::reqwest::stream::event::ReqwestEvent;
use titanrt::connector::{BaseConnector, HookArgs, Stream};
use titanrt::control::inputs::InputMeta;
use titanrt::io::ringbuffer::{RingReceiver, RingSender};
use titanrt::model::{ExecutionResult, NullEvent, NullModelCtx, NullOutputTx, StopKind, StopState};
use titanrt::prelude::BaseModel;
use titanrt::utils::time::timestamp::now_millis;
use titanrt::utils::time::{TimeUnit, Timeframe};
use titanrt::utils::{CancelToken, NullReducer, NullState};

#[derive(Default, Deserialize, Clone)]
pub struct TestModel2Config {}
pub struct TestModel2 {
    stream: Stream<RingSender<ReqwestAction>, RingReceiver<bool>, NullState>,
    now: Instant,
}

impl BaseModel for TestModel2 {
    type Config = TestModel2Config;
    type OutputTx = NullOutputTx;
    type OutputEvent = ();
    type Event = NullEvent;
    type Ctx = NullModelCtx;

    fn initialize(
        _ctx: Self::Ctx,
        _config: Self::Config,
        _reserved_core_id: Option<usize>,
        _output_tx: Self::OutputTx,
        _cancel_token: CancelToken,
    ) -> anyhow::Result<Self> {
        let conn_cfg = ReqwestConnectorConfig {
            default_max_cores: None,
            specific_core_ids: vec![],
            use_core_stats: false,
            local_ips: None,
            pool_idle_timeout_sec: None,
            pool_max_idle_per_host: None,
            request_timeout_ms: None,
        };

        let ct = CancelToken::new_root();

        let mut conn = ReqwestConnector::init(conn_cfg, ct, None)?;

        let mut desc = ReqwestStreamDescriptor::low_latency();
        desc.add_rate_limit(RateLimitConfig {
            venue: "bybit".to_string(),
            rules: Default::default(),
            default_rules: vec![RateLimitRule {
                limit_type: RateLimitType::Ip,
                scope: RateLimitScope::Global,
                timeframe: Timeframe::new(1, TimeUnit::Second),
                limit: 5,
                weight: 1,
                can_override_weight: false,
                key: Default::default(),
            }],
        });

        let stream = conn.spawn_stream(desc, hook)?;

        while !stream.is_healthy() {
            sleep(std::time::Duration::from_secs(1));
        }

        Ok(Self {
            stream,
            now: Instant::now(),
        })
    }

    fn execute(&mut self) -> ExecutionResult {
        if self.now.elapsed().as_secs() > 10 {
            return ExecutionResult::Shutdown;
        }

        if self.stream.is_healthy() {
            let action = ReqwestAction::get(
                Url::parse(
                    "https://api.bybit.com/v5/market/orderbook?category=spot&symbol=BTCUSDT",
                )
                .unwrap(),
            )
            .rl_ctx(RateLimitContext {
                venue: "bybit",
                endpoint: None,
                ip: None,
                account_id: None,
            })
            .build();

            self.stream.try_send(action).ok();
        }

        ExecutionResult::Continue
    }

    fn on_event(&mut self, _event: Self::Event, _meta: Option<InputMeta>) {}

    fn stop(&mut self, _kind: StopKind) -> StopState {
        self.stream.cancel();
        StopState::Done
    }

    fn hot_reload(&mut self, _config: &Self::Config) -> anyhow::Result<()> {
        Ok(())
    }
}

fn hook(args: HookArgs<ReqwestEvent, RingSender<bool>, NullReducer, NullState, ReqwestStreamDescriptor>) {
    if args.raw.is_success() {
        println!("Received event at hook. Now {}", now_millis())
    } else {
        println!("{:?}", args.raw.maybe_error_msg())
    }
}
