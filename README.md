Below is a cleaned-up, **English-only**, *model-first* README where **the model owns and controls connectors/streams**. Events flow from streams into the model; the runtime control-plane is reserved for lifecycle commands (Start/Restart/Hot-reload/Shutdown).

---

# Reacton

Typed reactive runtime for real-time systems.

**Reacton** is a *model-first* reactive runtime. Your **model** owns and controls **connectors** and their **streams** (workers). Streams produce strongly-typed events; the model pulls and handles them inside its execution loop. The runtime provides lifecycle orchestration, back-pressure, cooperative cancellation, and optional CPU core pinning. The **control plane** is for commands; the **event plane** lives inside the model via its embedded connectors.

* Crate: `reacton`
* License: MIT OR Apache-2.0
* Docs: `https://docs.rs/reacton`

---

## Highlights

* **Model-first design:** the model *spawns and manages* connectors/streams and *owns* the connector/stream handles.
* **Typed boundaries:** descriptors, actions, events, state cells, and hooks are all statically typed.
* **Deterministic back-pressure:** bounded/unbounded in-process transports, configurable draining.
* **Cooperative cancellation:** hierarchical `CancelToken` for graceful stops.
* **Deterministic performance:** optional CPU core pinning and per-stream core selection policy.
* **Small surface area:** minimal API and dependencies.

---

## Architecture (at a glance)

```
                   ┌───────────────────────────────────────────┐
                   │                 Runtime                   │
                   │   control plane: Start/Restart/Reload...  │
                   └───────────▲───────────────────┬───────────┘
                               │                   │
                        commands│                   │ model runs
                               │                   │ execute(), stop()
                     ┌─────────┴─────────┐         │
                     │       Model        │────────┘
                     │  owns connectors   │
                     └─────────┬─────────┘
                               │
                   ┌───────────▼───────────┐
                   │      Connector(s)     │
                   │    spawn Stream(s)    │
                   └───────────┬───────────┘
                               │
                    Actions ───►│   │◄─── Events
                               ▼
                            Stream
                     (worker thread/loop)
```

* **Streams** are spawned from inside the model and communicate via typed action/event channels.
* The **model** pulls events from stream receivers in `execute()` and pushes actions to streams when needed.
* The **runtime** only handles lifecycle commands, back-pressure on the control queue, and cancellation.

---

## Core concepts

### BaseModel (model-first)

Your application logic implements `BaseModel` and **owns** the connector/stream handles:

* `initialize(ctx, config, reserved_core_id, output_tx, cancel_token)`
  Create connectors, spawn streams, keep their handles.
* `execute()`
  Hot loop. Drain event receivers from owned streams, translate/publish state, emit outputs, push actions if needed.
* `stop(kind)`
  Cooperatively stop streams (cancel + join) and clean up.
* Optional: `hot_reload(&Config)`, `json_command(Value)`.
* Associated types: `Config`, `OutputTx` (if you publish downstream), `Event` (if you still forward some events via control plane—usually *not* needed in connector-first flow), `Ctx`.

### Connector / Stream layer

* **StreamDescriptor** — declares venue/kind, bounds for actions/events channels, core pick policy, initial health.
* **StreamRunner\<D, E, S>** — defines the worker loop and a *hook* translating raw messages to typed events.
* **StreamSpawner\<D, E, S>** — helper that allocates channels/StateCell, applies core policy, and spawns the worker.
* **BaseConnector** — a thin facade used *inside the model* to spawn multiple streams.

### Transports

* `io::ringbuffer` — low-overhead bounded SPSC-like transport.
* `io::mpmc` (crossbeam) — bounded/unbounded MPMC.
  All transports implement `BaseTx`/`BaseRx` with `try_send/send(timeout, cancel)` and `try_recv/recv(timeout, cancel)`.

### Cancellation & pinning

* **`CancelToken`** supports hierarchical cancellation; the model can create per-stream child tokens.
* **Core pinning** via `core_affinity`; descriptors expose a core pick policy (round-robin, specific core, etc.).

---

## Quick start (model owns the connector)

Below is a minimal sketch showing the **model spawning and controlling** a simple echo stream.
The stream forwards every “action” as a typed event; the model drains those events in `execute()`.

```rust
use anyhow::Result;
use serde::Deserialize;

// --- Reacton imports (names may be re-exported in your crate layout)
use reacton::adapter::*;
use reacton::io::mpmc::{MpmcSender, MpmcReceiver};
use reacton::io::base::{BaseTx, TxPairExt};
use reacton::model::{BaseModel, ExecutionResult, StopKind, StopState};
use reacton::runtime::{Runtime, RuntimeConfig};
use reacton::utils::CancelToken;

// 1) A small descriptor for an "echo" stream
#[derive(Debug, Clone)]
struct EchoDesc { cap: usize }
impl StreamDescriptor for EchoDesc {
    fn venue(&self) -> impl Venue { "local" }
    fn kind(&self)  -> impl Kind  { "echo" }
    fn max_pending_actions(&self) -> Option<usize> { Some(self.cap) }
    fn max_pending_events(&self)  -> Option<usize> { Some(self.cap) }
    fn core_pick_policy(&self) -> Option<CorePickPolicy> { None }
    fn health_at_start(&self) -> bool { true }
}

// 2) A connector that can run the Echo stream
struct EchoConnector { cancel: CancelToken }
impl StreamRunner<EchoDesc, MpmcSender<String>, ()> for EchoConnector {
    type Config   = ();
    type ActionTx = MpmcSender<String>;
    type RawEvent = String;
    type Hook     = fn(&Self::RawEvent, &mut MpmcSender<String>, &StateCell<()>);

    fn build_config(&mut self, _d: &EchoDesc) -> Result<Self::Config> { Ok(()) }

    fn run(mut ctx: RuntimeCtx<EchoDesc, Self, MpmcSender<String>, ()>, hook: Self::Hook)
        -> adapter::errors::StreamResult<()>
    {
        loop {
            if ctx.cancel.is_cancelled() { break; }
            match ctx.action_rx.try_recv() {
                Ok(msg) => (hook)(&msg, &mut ctx.event_tx, &ctx.state),
                Err(_)  => {} // empty/disconnected; consider sleep/spin policy as needed
            }
        }
        Ok(())
    }
}
impl StreamSpawner<EchoDesc, MpmcSender<String>, ()> for EchoConnector {}
impl BaseConnector for EchoConnector {
    type Config = ();
    fn init(_cfg: (), cancel: CancelToken, _reserved: Option<Vec<usize>>) -> Result<Self> {
        Ok(Self { cancel })
    }
    fn cancel_token(&self) -> &CancelToken { &self.cancel }
}

// 3) The model OWNS and CONTROLS its connector/stream
#[derive(Clone, Debug, Deserialize)]
struct MyCfg { greeting: String }

struct MyModel {
    cfg: MyCfg,
    connector: EchoConnector,
    echo: Option<
        // The returned Stream type from spawn_stream; use concrete type in your codebase.
        // For README, we keep it inferred via `let echo = ...?; self.echo = Some(echo);`
        // so the example stays transport-agnostic.
        // e.g.: reacton::adapter::Stream<EchoDesc, MpmcSender<String>, ()>
        // (type depends on your crate’s exact definitions)
        Box<dyn std::any::Any> // placeholder for README; use concrete type in real code
    >,
    echo_tx: Option<MpmcSender<String>>,
    echo_rx: Option<MpmcReceiver<String>>,
}

impl BaseModel for MyModel {
    type Config   = MyCfg;
    type OutputTx = ();         // not used here
    type Event    = ();         // we don’t inject events via control-plane
    type Ctx      = ();

    fn initialize(
        _ctx: Self::Ctx,
        cfg: Self::Config,
        _reserved_core_id: Option<usize>,
        _output_tx: Self::OutputTx,
        cancel: CancelToken,
    ) -> Result<Self> {
        // The model owns a root token; create a child for connector if desired.
        let connector = EchoConnector::init((), cancel.new_child(), None)?;

        // Spawn a stream *from inside the model*:
        let hook: <EchoConnector as StreamRunner<EchoDesc, MpmcSender<String>, ()>>::Hook =
            |raw, out, _state| { let _ = out.try_send(raw.clone()); };

        // Build the stream; keep both action TX and event RX in the model:
        let mut stream = connector
            .spawn_stream::<EchoDesc, MpmcSender<String>, ()>(EchoDesc { cap: 1024 }, hook)?;

        // Pull halves (API depends on your Stream facade; using common methods here)
        let tx = stream.action_tx().clone();        // typed actions → worker
        let rx = stream.event_rx().clone();         // typed events ← worker

        Ok(Self {
            cfg,
            connector,
            echo: Some(Box::new(stream)),
            echo_tx: Some(tx),
            echo_rx: Some(rx),
        })
    }

    fn execute(&mut self) -> ExecutionResult {
        // 1) Periodic work: push an action into the stream
        if let Some(tx) = &mut self.echo_tx {
            let _ = tx.try_send(format!("{}!", self.cfg.greeting));
        }

        // 2) Drain events coming from the stream
        if let Some(rx) = &mut self.echo_rx {
            // Non-blocking drain; apply your own drain limit/backoff policy
            for _ in 0..64 {
                match rx.try_recv() {
                    Ok(ev) => {
                        // Handle the typed event inside the model
                        // e.g. update StateCell, forward to outputs, etc.
                        let _ = ev; // placeholder
                    }
                    Err(_) => break,
                }
            }
        }

        // Choose your pacing: Continue (tight loop), Relax (yield/sleep), Stop, Shutdown
        ExecutionResult::Relax
    }

    fn on_event(&mut self, _e: Self::Event) {
        // Unused: events come from embedded streams, not control-plane.
    }

    fn stop(&mut self, _kind: StopKind) -> StopState {
        // Graceful stop: cancel & join owned stream(s)
        // (Your Stream type should expose cancel() / stop() / wait_to_end())
        // Example (pseudo-API):
        // if let Some(stream) = &mut self.echo { let _ = stream.stop(); }

        StopState::Done
    }
}

// 4) Wiring: run the runtime; the model controls its connector internally
fn main() -> Result<()> {
    let cfg = RuntimeConfig {
        init_model_on_start: true,
        core_id: None,
        max_inputs_pending: Some(1024),
        max_inputs_drain: Some(64),
        stop_model_timeout: Some(5),
    };

    let mut rt = Runtime::<MyModel>::spawn(
        cfg,
        (),                               // Ctx
        MyCfg { greeting: "hello".into() },
        (),                               // OutputTx
    )?;

    // Control-plane is for lifecycle commands only:
    use reacton::control::inputs::{Input, CommandInput};
    let _ = rt.control_tx().try_send(Input::Command(CommandInput::Shutdown));

    rt.run_blocking()?;
    Ok(())
}
```

> Notes
>
> * In your project, replace placeholder types (like the `Box<dyn Any>`) with your concrete `Stream<...>` type.
> * Many users prefer to store just `action_tx`/`event_rx` plus a small `StreamHandle` (with `cancel()/stop()/is_healthy()`) instead of the full stream object.

---

## Recommended model pattern

* **Own the connector(s):** create in `initialize()`, hold in the model struct.
* **Spawn streams from the model:** one or many per connector; keep `action_tx`, `event_rx`, and a handle.
* **Drive the system in `execute()`:**

  * Drain event receivers (non-blocking), apply limits per tick.
  * Send actions into streams based on your logic/timers/state.
  * Publish snapshots via `StateCell<S>` if needed.
* **Graceful stop:** cancel and join streams in `stop()`.
* **Hot-reload:** reconfigure model, rebuild or restart streams as needed.

---

## Control plane (commands only)

* `Start`, `Stop`, `Restart`, `HotReload(Value)`, `Json(Value)`, `Shutdown`, `Kill`
  Avoid using `Input::Event` for regular data flow; keep event ingress inside the model via streams.

---

## Transports & back-pressure

* Prefer `io::ringbuffer` for hot paths and predictable latency.
* Use `io::mpmc` for flexible topologies (multiple producers/consumers).
* Bound and tune queues per stream (descriptor) and for control plane (`max_inputs_*`).

---

## License

Licensed under either of

* Apache License, Version 2.0, or
* MIT license

at your option.

