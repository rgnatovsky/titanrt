use crate::io::base::{BaseTx, TxPairExt};
use crate::utils::*;
use serde::Deserialize;
use std::fmt::Display;
use std::sync::Arc;
use crate::adapter::{Stream, StreamDescriptor, StreamSpawner};

pub trait BaseConnector: Sized + Send + 'static {
    type Config: Clone + Send + for<'a> Deserialize<'a> + 'static;

    fn init(
        config: Self::Config,
        cancel_token: CancelToken,
        reserved_core_ids: Option<Vec<usize>>,
    ) -> anyhow::Result<Self>;

    fn name(&self) -> impl AsRef<str> + Display;

    fn config(&self) -> &Self::Config;

    fn cancel_token(&self) -> &CancelToken;

    fn cores_stats(&self) -> Option<Arc<CoreStats>>;

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
