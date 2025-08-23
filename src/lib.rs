//! # TitanRt
//!
//! Model‑first, typed reactive runtime for real‑time systems.
//!
//! This crate focuses on **models that own connectors and streams**. The runtime
//! only orchestrates lifecycle (start/restart/hot‑reload/shutdown) and back‑pressure,
//! while your model pulls typed events from its streams and pushes typed actions back.
//!
//! **Read the full guide & architecture in the project README.**
//! – <https://crates.io/crates/titanrt>
//!
//! ## When to use
//! - You need a compact event loop with predictable back‑pressure.
//! - You want typed boundaries between model, I/O adapters and state.
//! - You care about graceful, hierarchical cancellation and optional core pinning.
//!
//! ## Core types (jump in)
//! - [`runtime::Runtime`] — control thread that feeds commands and drives the model.
//! - [`model::BaseModel`] — your app logic (owns connectors/streams).
//! - [`connector::BaseConnector`] — spawns typed worker **streams**.
//! - [`connector::Stream`] — handle with action TX, event RX, health, state and cancel.
//! - [`utils::CancelToken`] — hierarchical cooperative cancellation.
//!
//! ## Minimal model loop (owning a stream)
//! ```rust,ignore
//! use titanrt::model::{BaseModel, ExecutionResult, StopKind, StopState};
//! use titanrt::runtime::{Runtime, RuntimeConfig};
//! use titanrt::utils::CancelToken;
//! use titanrt::io::base::{NullTx};
//! use anyhow::Result;
//!
//! // Your connector implements StreamRunner + StreamSpawner; omitted here.
//! // struct EchoConnector { /* ... */ }
//! // impl BaseConnector for EchoConnector { /* ... */ }
//!
//! struct MyModel {
//!     // connector: EchoConnector,
//!     // echo: Stream<...>, echo_tx: ..., echo_rx: ...
//! }
//!
//! impl BaseModel for MyModel {
//!     type Config   = ();
//!     type OutputTx = NullTx;         // or your own transport
//!     type Event    = ();             // events normally come from streams, not control plane
//!     type Ctx      = ();
//!
//!     fn initialize(_: (), _: (), _: Option<usize>, _: NullTx, _cancel: CancelToken) -> Result<Self> {
//!         // 1) Build connector(s) and spawn stream(s) here.
//!         // 2) Keep action_tx, event_rx and state handles in the model.
//!         Ok(Self{})
//!     }
//!
//!     fn execute(&mut self) -> ExecutionResult {
//!         // Drain events from stream(s), push actions if needed, publish state snapshots.
//!         // self.echo.try_recv().ok();
//!         ExecutionResult::Relax
//!     }
//!
//!     fn on_event(&mut self, _e: Self::Event) {}
//!
//!     fn stop(&mut self, _kind: StopKind) -> StopState {
//!         // Cancel and join streams here.
//!         StopState::Done
//!     }
//! }
//!
//! fn main() -> Result<()> {
//!     let cfg = RuntimeConfig {
//!         init_model_on_start: true,
//!         core_id: None,
//!         max_inputs_pending: Some(1024),
//!         max_inputs_drain: Some(64),
//!         stop_model_timeout: Some(5),
//!     };
//!     let rt = Runtime::<MyModel>::spawn(cfg, (), (), NullTx)?;
//!     rt.run_blocking()?;
//!     Ok(())
//! }
//! ```
//!
//! ## Notes
//! - Prefer delivering data via **streams** (event plane inside the model).
//!   Use the control plane for **commands** only.
//! - Pin to specific cores through stream descriptors and [`utils::CorePickPolicy`].
//! - See README for a full, compiling connector/stream example and real transports.
//!
//! ---
//! Re‑exports live under modules like `runtime`, `model`, `connector`, `io`, and `utils`.

pub mod config;
pub mod control;
pub mod error;
pub mod io;
pub mod model;
pub mod runtime;
mod test;
pub mod utils;

#[cfg(feature = "connector")]
pub mod connector;
pub mod prelude;
