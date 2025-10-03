use std::fmt::Debug;

use crate::connector::features::shared::rate_limiter::RateLimitConfig;
use crate::connector::{Kind, StreamDescriptor, Venue};
use crate::utils::CorePickPolicy;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
/// Descriptor configuring the reqwest-based stream runner.
///
/// # What this stream does
/// The runner consumes HTTP *actions* from an internal action queue, executes them
/// concurrently with `reqwest::Client`, and emits `HttpResponse` events into a user
/// hook. It is optimized for a high-throughput **single-threaded actor**: a Tokio
/// current-thread runtime plus a `LocalSet` drive many small per-request tasks
/// spawned with `spawn_local`.
///
/// ## Lifecycle
/// - A current-thread Tokio runtime (`enable_io`, `enable_time`) and a `LocalSet` are created.
/// - Main loop:
///   1. Non-blocking drain of pending actions. For each action:
///      - Select a `Client` by `ip_id` (fallback to `DEFAULT_IP_ID`).
///      - Build a `reqwest::Request`; build errors are converted via `HttpResponse::from_error`.
///      - Spawn a lightweight local task that:
///         - Resolves applicable rate-limit rules and **waits outside any lock**,
///         - Executes `client.execute(request).await`,
///         - Maps the result to `HttpResponse` and sends it to an internal result queue.
///   2. Pull up to `max_hook_calls_at_once` responses and invoke the user hook (`HookArgs`).
///   3. Yield or sleep for `wait_async_tasks_us` to cooperate with other tasks.
/// - On cancellation: health is lowered and the loop exits gracefully.
///
/// ## Rate limiting
/// A `RateLimitManager` enforces one or more rules per request context using GCRA buckets
/// (generic cell rate algorithm). Resolution/creation of buckets happens under a short
/// `Mutex` section; **actual waits occur without holding the lock**, avoiding global
/// serialization. Multiple rules are awaited **sequentially** to honor all constraints.
///
/// ## Ordering & concurrency
/// Requests run concurrently; responses may arrive **out of order**. The hook sees at most
/// `max_hook_calls_at_once` events per tick. Backpressure is provided by bounded action/event
/// buffers (see fields below).
///
/// ## Tuning guide
/// - `max_hook_calls_at_once`: raise for higher throughput if the hook is cheap; reduce if the hook is heavy.
/// - `wait_async_tasks_us`: `0` yields only; a small sleep (e.g. 50–500µs) can reduce CPU on hot loops.
/// - `max_pending_actions` / `max_pending_events`: cap queue sizes to protect memory and apply backpressure.
/// - `rate_limits`: venue-specific rules compiled and applied by the manager.
///
/// ## Failure semantics
/// - Build/execute errors are wrapped in `HttpResponse::from_error` and still delivered to the hook.
/// - Limiter/actor shutdown does not deadlock callers; waits resolve early.
///
/// This type is the public, documented entry point; the execution loop lives in the `StreamRunner` impl.
pub struct ReqwestStreamDescriptor<T> {
    /// Maximum number of hook invocations to process in a single loop tick.
    /// Works as a soft batch size to prevent the hook from monopolizing the loop.
    pub max_hook_calls_at_once: usize,

    /// Cooperative pause between loop ticks, in microseconds.
    /// If 0, the runner only yields to the scheduler; if >0, it sleeps that long on the LocalSet.
    pub wait_async_tasks_us: u64,

    /// Upper bound for queued actions awaiting execution.
    /// `None` uses the unbounded queue.
    pub max_pending_actions: Option<usize>,

    /// Upper bound for queued outgoing events pending hook delivery.
    /// `None` uses the unbounded queue.
    pub max_pending_events: Option<usize>,

    /// Optional policy for selecting/pinning the runtime core/worker.
    /// If `None`, the runtime's default core-picking policy is used.
    pub core_pick_policy: Option<CorePickPolicy>,

    /// Rate-limit configuration set applied by the RateLimitManager (GCRA buckets).
    /// Empty means "no limits".
    pub rate_limits: Vec<RateLimitConfig>,

    /// User-defined context passed to the hook.
    pub ctx: Option<T>,
}

impl<T> ReqwestStreamDescriptor<T> {
    pub fn new(
        max_hook_calls_at_once: Option<usize>,
        wait_async_tasks_us: Option<u64>,
        max_pending_actions: Option<usize>,
        max_pending_events: Option<usize>,
        core_pick_policy: Option<CorePickPolicy>,
        rate_limits: Option<Vec<RateLimitConfig>>,
        ctx: Option<T>,
    ) -> Self {
        let max_hook_calls_at_once = max_hook_calls_at_once.filter(|&x| x > 0).unwrap_or(10);
        let wait_async_tasks_us = wait_async_tasks_us.unwrap_or(100);

        Self {
            max_hook_calls_at_once,
            max_pending_actions,
            max_pending_events,
            wait_async_tasks_us,
            core_pick_policy,
            rate_limits: rate_limits.unwrap_or_default(),
            ctx,
        }
    }

    /// Low-latency preset: fewer hooks per tick, tiny sleep.
    pub fn low_latency() -> Self {
        Self {
            max_hook_calls_at_once: 4,
            wait_async_tasks_us: 0, // rely on yield
            ..Default::default()
        }
    }

    /// High-throughput preset: more hooks per tick, modest sleep.
    pub fn high_throughput() -> Self {
        Self {
            max_hook_calls_at_once: 64,
            wait_async_tasks_us: 200,
            ..Default::default()
        }
    }

    /// Add a rate limit config to the stream descriptor
    pub fn add_rate_limit(&mut self, rate_limit: RateLimitConfig) {
        self.rate_limits.push(rate_limit);
    }
}

impl<T> Default for ReqwestStreamDescriptor<T> {
    fn default() -> Self {
        Self {
            max_hook_calls_at_once: 10,
            wait_async_tasks_us: 100,
            max_pending_actions: None,
            max_pending_events: None,
            core_pick_policy: None,
            rate_limits: vec![],
            ctx: None,
        }
    }
}

impl<T: Debug + Clone + Send + 'static> StreamDescriptor<T> for ReqwestStreamDescriptor<T> {
    fn venue(&self) -> impl Venue {
        "any"
    }

    fn kind(&self) -> impl Kind {
        "http"
    }

    fn max_pending_actions(&self) -> Option<usize> {
        self.max_pending_actions
    }

    fn max_pending_events(&self) -> Option<usize> {
        self.max_pending_events
    }

    fn core_pick_policy(&self) -> Option<CorePickPolicy> {
        self.core_pick_policy
    }

    fn health_at_start(&self) -> bool {
        false
    }

    fn context(&self) -> Option<&T> {
        self.ctx.as_ref()
    }
}
