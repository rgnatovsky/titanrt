use crate::adapter::{Stream, StreamDescriptor, StreamSpawner};
use crate::io::base::{BaseTx, TxPairExt};
use crate::utils::*;
use serde::Deserialize;
use std::fmt::Display;
use std::sync::Arc;

/// Connector facade owned by the model.
///
/// A `BaseConnector` is a lightweight handle that can spawn typed worker
/// streams. It keeps shared resources (e.g., network clients, core stats,
/// root cancel token) and delegates per-stream spawning to its
/// `StreamSpawner` implementation.
///
/// Notes:
/// - `Self::ActionTx` and `Self::Hook` are *inherited* from your
///   `impl StreamSpawner<...> for Self`; they are not declared here.
/// - `spawn_stream` passes a **child** `CancelToken` and optional `CoreStats`
///   into the spawner so each stream can be cancelled/pinned independently.
pub trait BaseConnector: Sized + Send + 'static {
    /// Connector configuration type (serde-deserializable).
    type Config: Clone + Send + for<'a> Deserialize<'a> + 'static;

    /// Construct the connector with a root cancel token and optional reserved cores.
    fn init(
        config: Self::Config,
        cancel_token: CancelToken,
        reserved_core_ids: Option<Vec<usize>>,
    ) -> anyhow::Result<Self>;

    /// Human-readable connector name (for logs/metrics).
    fn name(&self) -> impl AsRef<str> + Display;

    /// Access the immutable configuration.
    fn config(&self) -> &Self::Config;

    /// Root cancel token; streams will receive a child token.
    fn cancel_token(&self) -> &CancelToken;

    /// Optional per-core statistics used for pinning policies.
    fn cores_stats(&self) -> Option<Arc<CoreStats>>;

    /// Spawn a typed stream described by `desc`, using the connector’s spawner.
    ///
    /// The `hook` and `ActionTx` associated types come from the corresponding
    /// `StreamSpawner<D, E, S>` implementation for `Self`.
    ///
    /// - `desc`: stream descriptor (venue/kind/bounds/core policy).
    /// - `hook`: translation callback from raw messages to typed events.
    /// - Returns a `Stream` exposing action TX, event RX, health, cancel, and join.
    fn spawn_stream<D, E, S>(
        &mut self,
        desc: D,
        hook: Self::Hook,
    ) -> anyhow::Result<Stream<Self::ActionTx, E::RxHalf, S>>
    where
        Self: StreamSpawner<D, E, S>,
        D: StreamDescriptor,
        S: StateMarker,
        E: BaseTx + TxPairExt,
    {
        <Self as StreamSpawner<D, E, S>>::spawn(
            self,
            desc,
            hook,
            self.cancel_token().new_child(),
            self.cores_stats(),
        )
    }
}
