use crate::connector::hook::IntoHook;
use crate::connector::{Stream, StreamDescriptor, StreamSpawner};
use crate::io::base::{BaseTx, TxPairExt};
use crate::utils::*;
use serde::Deserialize;
use std::fmt::Display;
use std::sync::Arc;

/// Connector facade owned by the model.
///
/// A `BaseConnector` is a lightweight handle that can spawn typed stream
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
    type MainConfig: Clone + Send + for<'a> Deserialize<'a> + 'static;

    /// Construct the connector with a root cancel token and optional reserved cores.
    fn init(
        config: Self::MainConfig,
        cancel_token: CancelToken,
        reserved_core_ids: Option<Vec<usize>>,
    ) -> anyhow::Result<Self>;

    /// Human-readable connector name (for logs/metrics).
    fn name(&self) -> impl AsRef<str> + Display;

    /// Access the immutable configuration.
    fn config(&self) -> &Self::MainConfig;

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
    fn spawn_stream<D, E, R, S, H>(
        &mut self,
        desc: D,
        hook: H,
    ) -> anyhow::Result<Stream<Self::ActionTx, E::RxHalf, S>>
    where
        Self: StreamSpawner<D, E, R, S>,
        D: StreamDescriptor,
        S: StateMarker,
        E: BaseTx + TxPairExt,
        H: IntoHook<Self::RawEvent, E, R, S, D, Self::HookResult>,
        R: Reducer,
    {
        <Self as StreamSpawner<D, E, R, S>>::spawn(
            self,
            desc,
            hook,
            self.cancel_token().new_child(),
            self.cores_stats(),
        )
    }
}

pub struct AnyConnector<T>(pub T)
where
    T: BaseConnector;

impl<T> std::ops::Deref for AnyConnector<T>
where
    T: BaseConnector,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<T> std::ops::DerefMut for AnyConnector<T>
where
    T: BaseConnector,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> AnyConnector<T>
where
    T: BaseConnector,
{
    pub fn new(inner: T) -> Self {
        Self(inner)
    }

    pub fn init(
        config: T::MainConfig,
        cancel_token: CancelToken,
        reserved_core_ids: Option<Vec<usize>>,
    ) -> anyhow::Result<Self> {
        T::init(config, cancel_token, reserved_core_ids).map(Self)
    }

    pub fn into_inner(self) -> T {
        self.0
    }
    pub fn inner(&self) -> &T {
        &self.0
    }
    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.0
    }

    pub fn name(&self) -> impl AsRef<str> + Display {
        self.0.name()
    }

    pub fn config(&self) -> &T::MainConfig {
        self.0.config()
    }
}
