use crate::error::{RecvError, SendError, TryRecvError};
use crate::utils::CancelToken;
use std::time::Duration;

pub trait TxMarker {}
pub trait RxMarker {}

pub trait BaseTx: Send + 'static {
    type EventType: Send + 'static;

    fn try_send(&mut self, a: Self::EventType) -> Result<(), SendError<Self::EventType>>;

    fn send(
        &mut self,
        a: Self::EventType,
        cancel: &CancelToken,
        timeout: Option<Duration>,
    ) -> Result<(), SendError<Self::EventType>>;
}

pub trait BaseRx: Send + 'static {
    type EventType: Send + 'static;

    fn try_recv(&mut self) -> Result<Self::EventType, TryRecvError>;

    fn recv(
        &mut self,
        cancel: &CancelToken,
        timeout: Option<Duration>,
    ) -> Result<Self::EventType, RecvError>;

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

    fn drain_max(&mut self) -> Vec<Self::EventType> {
        self.drain(usize::MAX)
    }
}

pub trait RxPairExt: BaseRx + Sized {
    type TxHalf: BaseTx<EventType = Self::EventType>;
    fn bound(cap: usize) -> (Self::TxHalf, Self);

    fn unbound() -> (Self::TxHalf, Self);
}

pub trait TxPairExt: BaseTx + Sized {
    type RxHalf: BaseRx<EventType = Self::EventType>;
    fn bound(cap: usize) -> (Self, Self::RxHalf);
    fn unbound() -> (Self, Self::RxHalf);
}

#[derive(Clone, Debug, Default)]
pub struct NullTx;

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
