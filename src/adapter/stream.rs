use crate::adapter::errors::{StreamError, StreamResult};
use crate::error::{RecvError, SendError, TryRecvError};
use crate::io::base::{BaseRx, BaseTx, RxMarker, TxMarker};
use crate::utils::*;
use std::fmt::{Debug, Display};
use std::{fmt, sync::Arc, thread::JoinHandle, time::Duration};
use uuid::fmt::Simple;
use uuid::Uuid;

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct StreamId {
    raw: Simple,
}

impl Default for StreamId {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamId {
    #[inline]
    pub fn raw(&self) -> Simple {
        self.raw
    }

    pub fn new() -> StreamId {
        Self {
            raw: Uuid::new_v4().simple(),
        }
    }
}

impl From<Simple> for StreamId {
    fn from(raw: Simple) -> Self {
        Self { raw }
    }
}

impl Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.raw)
    }
}

pub struct Stream<A: BaseTx, E: BaseRx, S: StateMarker> {
    id: StreamId,
    cancel: CancelToken,
    handle: Option<JoinHandle<StreamResult<()>>>,
    health: HealthFlag,
    action_tx: A,
    event_rx: E,
    state: Arc<StateCell<S>>,
}

impl<A: BaseTx, E: BaseRx, S: StateMarker> Stream<A, E, S> {
    pub fn new(
        id: Simple,
        handle: JoinHandle<StreamResult<()>>,
        health: HealthFlag,
        action_tx: A,
        event_rx: E,
        state: Arc<StateCell<S>>,
        cancel: CancelToken,
    ) -> Self {
        Self {
            id: StreamId::from(id),
            cancel,
            health,
            handle: Some(handle),
            action_tx,
            event_rx,
            state,
        }
    }

    #[inline]
    pub fn id(&self) -> &StreamId {
        &self.id
    }

    #[inline]
    pub fn id_raw(&self) -> Simple {
        self.id.raw()
    }

    #[inline]
    pub fn token(&self) -> &CancelToken {
        &self.cancel
    }

    #[inline(always)]
    pub fn is_healthy(&self) -> bool {
        self.health.get()
    }

    // ---- управление жизненным циклом ----
    #[inline]
    pub fn cancel(&self) {
        self.health.down();
        self.cancel.cancel();
    }

    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    pub fn stop(self) -> StreamResult<()> {
        self.cancel();
        self.wait_to_end()
    }

    pub fn wait_to_end(mut self) -> StreamResult<()> {
        self.health.down();
        if let Some(join) = self.handle.take() {
            match join.join() {
                Ok(r) => r,
                Err(p) => Err(StreamError::JoinPanic(
                    p.downcast_ref::<&str>()
                        .map(|s| s.to_string())
                        .or_else(|| p.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "panic (unknown type)".into()),
                )),
            }
        } else {
            Ok(())
        }
    }

    #[inline]
    pub fn state(&self) -> &Arc<StateCell<S>> {
        &self.state
    }
}

impl<A, E, S> Stream<A, E, S>
where
    A: BaseTx + TxMarker,
    E: BaseRx,
    S: StateMarker,
{
    #[inline]
    pub fn try_send(
        &mut self,
        action: <A as BaseTx>::EventType,
    ) -> Result<(), SendError<<A as BaseTx>::EventType>> {
        self.action_tx.try_send(action)
    }

    #[inline]
    pub fn send(
        &mut self,
        action: <A as BaseTx>::EventType,
        timeout: Option<Duration>,
    ) -> Result<(), SendError<<A as BaseTx>::EventType>> {
        self.action_tx.send(action, &self.cancel, timeout)
    }

    #[inline]
    pub fn action_tx(&self) -> &A {
        &self.action_tx
    }
}
impl<A, E, S> Stream<A, E, S>
where
    A: BaseTx,
    E: BaseRx + RxMarker,
    S: StateMarker,
{
    // ---- отправка команд ----
    #[inline]
    pub fn try_recv(&mut self) -> Result<E::EventType, TryRecvError> {
        self.event_rx.try_recv()
    }

    #[inline]
    pub fn recv(&mut self, timeout: Option<Duration>) -> Result<E::EventType, RecvError> {
        self.event_rx.recv(&self.cancel, timeout)
    }

    #[inline]
    pub fn drain(&mut self, max: usize) -> Vec<E::EventType> {
        self.event_rx.drain(max)
    }

    #[inline]
    pub fn drain_max(&mut self) -> Vec<E::EventType> {
        self.event_rx.drain_max()
    }

    #[inline]
    pub fn event_rx(&self) -> &E {
        &self.event_rx
    }
}

impl<A, E, S> Drop for Stream<A, E, S>
where
    A: BaseTx,
    E: BaseRx,
    S: StateMarker,
{
    #[inline]
    fn drop(&mut self) {
        if !self.cancel.is_cancelled() {
            self.cancel.cancel();
        }
    }
}

impl<A, E, S> Debug for Stream<A, E, S>
where
    A: BaseTx,
    E: BaseRx,
    S: StateMarker,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Stream")
            .field("id", &self.id)
            .field("is_cancelled", &self.cancel.is_cancelled())
            .finish()
    }
}
