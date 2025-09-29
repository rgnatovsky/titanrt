use crate::error::{RecvError, SendError, TryRecvError};
use crate::utils::CancelToken;
use std::time::Duration;

/// Marker trait for transport TX halves.
pub trait TxMarker {}

/// Marker trait for transport RX halves.
pub trait RxMarker {}

/// Base trait for sending typed events.
///
/// Implemented by channel senders (TX half).
pub trait BaseTx: Send + 'static {
    /// Event type carried by this transport.
    type EventType: Send + 'static;

    /// Non-blocking send. Returns `Err` if the channel is full or disconnected.
    fn try_send(&mut self, a: Self::EventType) -> Result<(), SendError<Self::EventType>>;

    /// Blocking/cooperative send with optional timeout and cancellation.
    fn send(
        &mut self,
        a: Self::EventType,
        cancel: &CancelToken,
        timeout: Option<Duration>,
    ) -> Result<(), SendError<Self::EventType>>;
}

/// Base trait for receiving typed events.
///
/// Implemented by channel receivers (RX half).
pub trait BaseRx: Send + 'static {
    /// Event type carried by this transport.
    type EventType: Send + 'static;

    /// Non-blocking receive. Returns `Empty` if no data, `Disconnected` if channel closed.
    fn try_recv(&mut self) -> Result<Self::EventType, TryRecvError>;

    /// Blocking/cooperative receive with optional timeout and cancellation.
    fn recv(
        &mut self,
        cancel: &CancelToken,
        timeout: Option<Duration>,
    ) -> Result<Self::EventType, RecvError>;

    /// Drain up to `max` events from the channel (default cap 64).
    fn drain(&mut self, max: usize) -> Vec<Self::EventType> {
        let max = max.min(64);
        let mut out = Vec::with_capacity(max);

        for _ in 0..max {
            match self.try_recv() {
                Ok(a) => out.push(a),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            }
        }
        out
    }

    /// Drain all currently available events.
    fn drain_max(&mut self) -> Vec<Self::EventType> {
        self.drain(usize::MAX)
    }
}

/// Extension for RX halves that can create paired TX halves.
pub trait RxPairExt: BaseRx + Sized {
    type TxHalf: BaseTx<EventType = Self::EventType>;

    /// Create a bounded channel with given capacity.
    fn bound(cap: usize) -> (Self::TxHalf, Self);

    /// Create an unbounded channel.
    fn unbound() -> (Self::TxHalf, Self);
}

/// Extension for TX halves that can create paired RX halves.
pub trait TxPairExt: BaseTx + Sized {
    type RxHalf: BaseRx<EventType = Self::EventType>;

    /// Create a bounded channel with given capacity.
    fn bound(cap: usize) -> (Self, Self::RxHalf);

    /// Create an unbounded channel.
    fn unbound() -> (Self, Self::RxHalf);
}

/// No-op sender for unit type.
#[derive(Clone, Debug, Default)]
pub struct NullTx;

/// No-op receiver for unit type.
#[derive(Clone, Debug, Default)]
pub struct NullRx;

impl BaseTx for NullTx {
    type EventType = ();

    fn try_send(&mut self, _a: Self::EventType) -> Result<(), SendError<Self::EventType>> {
        Ok(())
    }

    fn send(
        &mut self,
        _a: Self::EventType,
        _token: &CancelToken,
        _timeout: Option<Duration>,
    ) -> Result<(), SendError<Self::EventType>> {
        Ok(())
    }
}

impl BaseRx for NullRx {
    type EventType = ();

    fn try_recv(&mut self) -> Result<Self::EventType, TryRecvError> {
        Ok(())
    }

    fn recv(
        &mut self,
        _token: &CancelToken,
        _timeout: Option<Duration>,
    ) -> Result<Self::EventType, RecvError> {
        Ok(())
    }

    fn drain(&mut self, _max: usize) -> Vec<Self::EventType> {
        Vec::new()
    }

    fn drain_max(&mut self) -> Vec<Self::EventType> {
        Vec::new()
    }
}

impl RxPairExt for NullRx {
    type TxHalf = NullTx;

    fn bound(_cap: usize) -> (Self::TxHalf, Self) {
        (NullTx, NullRx)
    }

    fn unbound() -> (Self::TxHalf, Self) {
        (NullTx, NullRx)
    }
}

impl TxPairExt for NullTx {
    type RxHalf = NullRx;

    fn bound(_cap: usize) -> (Self, Self::RxHalf) {
        (NullTx, NullRx)
    }

    fn unbound() -> (Self, Self::RxHalf) {
        (NullTx, NullRx)
    }
}

impl<E> BaseRx for Option<E>
where
    E: BaseRx,
{
    type EventType = <E as BaseRx>::EventType;

    fn try_recv(&mut self) -> Result<Self::EventType, TryRecvError> {
        match self {
            Some(rx) => rx.try_recv(),
            None => Err(TryRecvError::Disconnected),
        }
    }

    fn recv(
        &mut self,
        cancel: &CancelToken,
        timeout: Option<Duration>,
    ) -> Result<Self::EventType, RecvError> {
        match self {
            Some(rx) => rx.recv(cancel, timeout),
            None => Err(RecvError::Absent),
        }
    }

    fn drain(&mut self, max: usize) -> Vec<Self::EventType> {
        match self {
            Some(rx) => rx.drain(max),
            None => Vec::new(),
        }
    }

    fn drain_max(&mut self) -> Vec<Self::EventType> {
        match self {
            Some(rx) => rx.drain_max(),
            None => Vec::new(),
        }
    }
}

impl<E> RxMarker for Option<E> where E: BaseRx + RxMarker {}
