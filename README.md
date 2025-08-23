# TitanRt

Model-first, **typed reactive runtime** for real-time systems. The runtime focuses on **models that own their connectors/streams** and publish/translate cheap, lock-free state snapshots (`StateCell<T>`). The control thread handles lifecycle (start/restart/hot-reload/shutdown/kill), back-pressure and cooperative cancellation; your model pulls typed events from its streams and pushes typed actions back.

* Predictable back-pressure via unified TX/RX traits.
* Typed boundaries between model, I/O adapters and state.
* Graceful, hierarchical cancellation and optional core pinning.
* Hot-reload of configuration.
* Health flags and lock-free state snapshots.
* When using the built-in connector, a model-side hook selects which events are emitted.
* The state payload is **user-defined**: you choose `T` for `StateCell<T>`.

## Install

Add with Cargo (no version pinning; you always pull the latest):

```bash
cargo add titanrt
# or, if you plan to use stream/connector APIs:
cargo add titanrt --features connector
```

## Quick start: a minimal model

`BaseModel` declares your config, output transport, event, and context types. The runtime drives
`initialize → execute → stop`, and you may handle external events via `on_event` and apply config updates via
`hot_reload`.

```rust
use anyhow::Result;
use titanrt::prelude::*;            // config, model, runtime, io::base re-exports
use titanrt::io::base::NullTx;      // no-op output
// Events: use the provided unit marker
// use titanrt::model::NullEvent;   // already in prelude via model::*

#[derive(Clone)]
struct AppCtx;
impl ModelContext for AppCtx {}

struct MyModel {
   _cancel: CancelToken,
}

impl BaseModel for MyModel {
   type Config = String;       // any serde-deserializable type
   type OutputTx = NullTx;       // output channel (no-op here)
   type Event = NullEvent;    // typed external events
   type Ctx = AppCtx;

   fn initialize(
      _ctx: AppCtx,
      _cfg: String,
      _reserved_core: Option<usize>,
      _out: NullTx,
      cancel: CancelToken,
   ) -> Result<Self> {
      Ok(Self { _cancel: cancel })
   }

   fn execute(&mut self) -> ExecutionResult {
      // single hot-loop tick; do useful work or drain streams here
      ExecutionResult::Relax    // yield (spin/yield/sleep backoff)
   }

   fn on_event(&mut self, _e: Self::Event) { /* handle external events */ }

   fn stop(&mut self, _kind: StopKind) -> StopState {
      // cancel streams, join workers, flush state, etc.
      StopState::Done
   }
}

fn main() -> Result<()> {
   let cfg = RuntimeConfig {
      init_model_on_start: true,
      core_id: None,                    // optionally pin runtime thread
      max_inputs_pending: Some(1024),   // control-plane ring capacity
      max_inputs_drain: Some(64),      // max inputs per drain pass
      stop_model_timeout: Some(5),      // seconds
   };

   let ctx = AppCtx;
   let model_cfg = "hello".to_string();
   let outputs = NullTx;

   Runtime::<MyModel>::spawn(cfg, ctx, model_cfg, outputs)?
      .run_blocking()
}
```

## Driving the runtime (control-plane)

Use the control sender to post **typed events** or **lifecycle/config commands**:

```rust
use titanrt::control::inputs::{Input, CommandInput};
use serde_json::json;

let mut rt = Runtime::<MyModel>::spawn(cfg, ctx, model_cfg, outputs) ?;

// Start (if not auto-started)
rt.control_tx().try_send(Input::Command(CommandInput::Start)).ok();

// Send a typed event to your model
rt.control_tx().try_send(Input::Event(NullEvent)).ok();

// Hot-reload configuration (your `hot_reload` receives the deserialized value)
rt.control_tx().try_send(Input::Command(
CommandInput::HotReload(json!({"foo": 42})),
)).ok();

// Graceful stop or full shutdown
rt.control_tx().try_send(Input::Command(CommandInput::Stop)).ok();
rt.control_tx().try_send(Input::Command(CommandInput::Shutdown)).ok();

// Block until the runtime thread finishes (or drop a guard for auto-shutdown)
rt.run_blocking() ?;
```

The control thread also listens to OS termination signals (via `signal-hook`) and will request a cooperative shutdown.

## Streams & connectors (optional feature)

Enable the `connector` feature to build typed **streams** (worker threads) managed by your model. The crate exposes:

* `connector::StreamDescriptor` — describes a stream (venue/kind, channel bounds, core policy, initial health).
* `connector::StreamRunner`/`StreamSpawner` — spawn a worker with typed action/event channels, `StateCell<S>`,
  `HealthFlag` and a child `CancelToken`.
* `connector::Stream` — handle owned by the model: `id`, `health`, `state`, `action_tx`, `event_rx`, etc.

You write your own connector facade (implementing `BaseConnector`) that holds shared resources and spawns streams via
`StreamSpawner`. Your model keeps the `Stream` handle(s), drains events each `execute()` tick, and publishes actions as
needed.

> Tip: use `CoreStats` and `CorePickPolicy` to pick CPU cores (round-robin, minimum threads, specific cores) for each
> stream worker.

## Channels & back-pressure

A small, unified transport layer:

* **Traits:** `BaseTx` / `BaseRx` (+ `TxPairExt` / `RxPairExt` helpers) give `try_send/try_recv`, blocking `send/recv`
  with `CancelToken` + optional timeouts, and draining (`drain`, `drain_max`).
* **Implementations:**

    * `io::ringbuffer::RingBuffer` — lock-free ring buffer (bounded).
    * `io::mpmc::MpmcChannel` — Crossbeam MPMC (bounded).
    * `io::base::NullTx`/`NullRx` — no-op ends for unit/empty flows.

Choose the channel per stream; the model only sees the `BaseTx/BaseRx` abstractions.

## State & health

* `utils::StateCell<S>` — lock-free snapshot cell with versioning (`publish`, `load`, `seq`), where `S: StateMarker` (
  usually a small “view”).
* `utils::HealthFlag` — cheap `AtomicBool` wrapped to avoid false sharing (`up`, `down`, `get`).
* `utils::CancelToken` — hierarchical cooperative cancellation (`child()`, `cancel()`, `is_cancelled()`).

## Configuration & hot-reload

* `BaseModel::Config` must be `Clone + Send + serde::Deserialize`.
* The control plane accepts `CommandInput::HotReload(serde_json::Value)`; your model implements `hot_reload(&Config)` (
  default is a no-op).

## Pinning & per-core stats

`utils::CoreStats` tracks per-core thread counts; `CorePickPolicy` lets you request **minimum-threads**, **round-robin
**, or **specific core(s)**. The runtime can also pin its own control thread (`RuntimeConfig::core_id`).

## Examples

A complete runnable example is included under `example/` in the repository: a toy model + connector/stream demonstrating
actions, events, state snapshots, core policies and cancellation.

## Feature flags

* `connector` — enables connector/stream APIs (descriptors, spawners, runners, stream handle).

## Documentation

API reference on **docs.rs**:

* [https://docs.rs/titanrt](https://docs.rs/titanrt)

## License

Dual-licensed under **MIT** or **Apache-2.0**.

---
