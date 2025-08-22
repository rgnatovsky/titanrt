use crate::error::{RecvError, SendError, TryRecvError};
use crate::io::base::{BaseRx, BaseTx, RxMarker, RxPairExt, TxMarker, TxPairExt};
use crate::utils::CancelToken;
use crossbeam::channel as cbchan;
use crossbeam::utils::Backoff;
use std::thread;
use std::time::{Duration, Instant};

pub struct MpmcChannel;

impl MpmcChannel {
    #[inline]
    pub fn bounded<T: Send + 'static>(capacity: usize) -> (MpmcSender<T>, MpmcReceiver<T>) {
        let (tx, rx) = cbchan::bounded::<T>(capacity);
        (MpmcSender { tx }, MpmcReceiver { rx })
    }

    #[inline]
    pub fn unbounded<T: Send + 'static>() -> (MpmcSender<T>, MpmcReceiver<T>) {
        let (tx, rx) = cbchan::unbounded::<T>();
        (MpmcSender { tx }, MpmcReceiver { rx })
    }
}

#[derive(Clone)]
pub struct MpmcSender<E> {
    tx: cbchan::Sender<E>,
}

impl<E: Send + 'static> BaseTx for MpmcSender<E> {
    type EventType = E;

    #[inline]
    fn try_send(&mut self, a: E) -> Result<(), SendError<E>> {
        match self.tx.try_send(a) {
            Ok(()) => Ok(()),
            Err(cbchan::TrySendError::Full(v)) => Err(SendError::full(Some(v))),
            Err(cbchan::TrySendError::Disconnected(v)) => Err(SendError::closed(Some(v))),
        }
    }

    fn send(
        &mut self,
        mut a: E,
        cancel: &CancelToken,
        timeout: Option<Duration>,
    ) -> Result<(), SendError<E>> {
        let start = Instant::now();
        let backoff = Backoff::new();
        let mut spins: u32 = 0;

        loop {
            if cancel.is_cancelled() {
                return Err(SendError::cancelled(Some(a)));
            }
            if let Some(t) = timeout
                && start.elapsed() >= t
            {
                return Err(SendError::timeout(Some(a)));
            }

            match self.tx.try_send(a) {
                Ok(()) => return Ok(()),
                Err(cbchan::TrySendError::Full(v)) => {
                    a = v;
                    spins = spins.saturating_add(1);
                    if spins < 64 {
                        backoff.spin();
                    } else if spins < 256 {
                        backoff.snooze();
                    } else {
                        thread::sleep(Duration::from_micros(2));
                    }
                }
                Err(cbchan::TrySendError::Disconnected(a)) => {
                    // Канал закрыт — считаем это фатальной ошибкой транспорта
                    return Err(SendError::closed(Some(a)));
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct MpmcReceiver<E> {
    rx: cbchan::Receiver<E>,
}

impl<E: Send + 'static> BaseRx for MpmcReceiver<E> {
    type EventType = E;

    #[inline]
    fn try_recv(&mut self) -> Result<E, TryRecvError> {
        match self.rx.try_recv() {
            Ok(v) => Ok(v),
            Err(cbchan::TryRecvError::Empty) => Err(TryRecvError::Empty),
            Err(cbchan::TryRecvError::Disconnected) => Err(TryRecvError::Disconnected),
        }
    }

    fn recv(&mut self, cancel: &CancelToken, timeout: Option<Duration>) -> Result<E, RecvError> {
        let start = Instant::now();
        let backoff = Backoff::new();
        let mut spins: u32 = 0;

        loop {
            if cancel.is_cancelled() {
                return Err(RecvError::Cancelled);
            }
            if let Some(t) = timeout
                && start.elapsed() >= t
            {
                return Err(RecvError::Timeout);
            }

            match self.rx.try_recv() {
                Ok(v) => return Ok(v),
                Err(cbchan::TryRecvError::Empty) => {
                    spins = spins.saturating_add(1);
                    if spins < 64 {
                        backoff.spin();
                    } else if spins < 256 {
                        backoff.snooze();
                    } else {
                        thread::sleep(Duration::from_micros(2));
                    }
                }
                Err(cbchan::TryRecvError::Disconnected) => {
                    return Err(RecvError::Disconnected);
                }
            }
        }
    }
}

impl<E: Send + 'static> RxPairExt for MpmcReceiver<E> {
    type TxHalf = MpmcSender<E>;

    fn bound(cap: usize) -> (Self::TxHalf, Self) {
        MpmcChannel::bounded(cap)
    }
    fn unbound() -> (Self::TxHalf, Self) {
        MpmcChannel::unbounded()
    }
}

impl<E: Send + 'static> TxPairExt for MpmcSender<E> {
    type RxHalf = MpmcReceiver<E>;

    fn bound(cap: usize) -> (Self, Self::RxHalf) {
        MpmcChannel::bounded(cap)
    }
    fn unbound() -> (Self, Self::RxHalf) {
        MpmcChannel::unbounded()
    }
}

impl<E: Send + 'static> TxMarker for MpmcSender<E> {}

impl<E: Send + 'static> RxMarker for MpmcReceiver<E> {}
