use crate::connector::errors::{StreamError, StreamResult};
use crate::error::{RecvError, SendError, TryRecvError};
use crate::io::base::{BaseRx, BaseTx, RxMarker, TxMarker};
use crate::utils::*;
use std::fmt::{Debug, Display};
use std::{fmt, sync::Arc, thread::JoinHandle, time::Duration};
use uuid::fmt::Simple;
use uuid::Uuid;

/// Unique identifier for a spawned stream.
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
    /// Raw UUID (compact format).
    #[inline]
    pub fn raw(&self) -> Simple {
        self.raw
    }

    /// Generate a new random id.
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

/// Handle to a running worker stream.
///
/// Wraps thread join handle, cancel token, health flag,
/// action/event channels, and shared state.
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
    /// Construct a new stream handle from parts.
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

    /// Get typed stream id.
    #[inline]
    pub fn id(&self) -> &StreamId {
        &self.id
    }

    /// Get raw UUID value.
    #[inline]
    pub fn id_raw(&self) -> Simple {
        self.id.raw()
    }

    /// Cancellation token of the stream.
    #[inline]
    pub fn token(&self) -> &CancelToken {
        &self.cancel
    }

    /// Check current health flag.
    #[inline(always)]
    pub fn is_healthy(&self) -> bool {
        self.health.get()
    }

    // ---- lifecycle management ----

    /// Request cancellation (sets health down and cancels token).
    #[inline]
    pub fn cancel(&self) {
        self.health.down();
        self.cancel.cancel();
    }

    /// Check if the cancel token is tripped.
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Cancel and wait for the thread to end.
    pub fn stop(self) -> StreamResult<()> {
        self.cancel();
        self.wait_to_end()
    }

    /// Wait for the thread to end, returning its result.
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

    /// Access shared state snapshot cell.
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
    /// Non-blocking send of an action into the worker.
    #[inline]
    pub fn try_send(
        &mut self,
        action: <A as BaseTx>::EventType,
    ) -> Result<(), SendError<<A as BaseTx>::EventType>> {
        self.action_tx.try_send(action)
    }

    /// Send an action with optional timeout and cancellation.
    #[inline]
    pub fn send(
        &mut self,
        action: <A as BaseTx>::EventType,
        timeout: Option<Duration>,
    ) -> Result<(), SendError<<A as BaseTx>::EventType>> {
        self.action_tx.send(action, &self.cancel, timeout)
    }

    /// Borrow the action TX half.
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
    // ---- event consumption ----

    /// Non-blocking receive from the worker.
    #[inline]
    pub fn try_recv(&mut self) -> Result<E::EventType, TryRecvError> {
        self.event_rx.try_recv()
    }

    /// Blocking/cooperative receive with optional timeout.
    #[inline]
    pub fn recv(&mut self, timeout: Option<Duration>) -> Result<E::EventType, RecvError> {
        self.event_rx.recv(&self.cancel, timeout)
    }

    /// Drain up to `max` events.
    #[inline]
    pub fn drain(&mut self, max: usize) -> Vec<E::EventType> {
        self.event_rx.drain(max)
    }

    /// Drain all currently available events.
    #[inline]
    pub fn drain_max(&mut self) -> Vec<E::EventType> {
        self.event_rx.drain_max()
    }

    /// Borrow the event RX half.
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
