use crate::error::{RecvError, SendError, TryRecvError};
use crate::io::base::{BaseRx, BaseTx, RxMarker, RxPairExt, TxMarker, TxPairExt};
use crate::utils::CancelToken;
use crossbeam::utils::Backoff;
use ringbuf::consumer::Consumer;
use ringbuf::producer::Producer;
use ringbuf::traits::Split;
use ringbuf::{HeapCons, HeapProd, HeapRb};
use std::thread;
use std::time::{Duration, Instant};

pub struct RingBuffer;

impl RingBuffer {
    pub fn bounded<T>(capacity: usize) -> (RingSender<T>, RingReceiver<T>) {
        // HeapRb — потокобезопасное хранилище; split() даёт прод/конс.
        let rb = HeapRb::<T>::new(capacity);
        let (prod, cons) = rb.split();

        (RingSender { prod }, RingReceiver { cons })
    }
}

pub struct RingSender<E> {
    prod: HeapProd<E>,
}

impl<E: Send + 'static + Clone> BaseTx for RingSender<E> {
    type EventType = E;

    #[inline]
    fn try_send(&mut self, a: E) -> Result<(), SendError<E>> {
        // У продюсера неблокирующий try_push: Ok(()) или Err(a) если full. :contentReference[oaicite:2]{index=2}
        self.prod.try_push(a).map_err(|v| SendError::full(Some(v)))
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

            match self.prod.try_push(a) {
                Ok(()) => return Ok(()),
                Err(aa) => {
                    a = aa; // буфер полон, владеем значением и продолжаем
                    spins = spins.saturating_add(1);
                    if spins < 64 {
                        backoff.spin();
                    } else if spins < 256 {
                        backoff.snooze(); // примерно yield
                    } else {
                        thread::sleep(Duration::from_micros(2));
                    }
                }
            }
        }
    }
}

pub struct RingReceiver<E> {
    cons: HeapCons<E>,
}

impl<E: Send + 'static> BaseRx for RingReceiver<E> {
    type EventType = E;

    #[inline]
    fn try_recv(&mut self) -> Result<E, TryRecvError> {
        self.cons.try_pop().ok_or(TryRecvError::Empty)
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

            match self.cons.try_pop() {
                Some(a) => return Ok(a),
                None => {
                    spins = spins.saturating_add(1);
                    if spins < 64 {
                        backoff.spin();
                    } else if spins < 256 {
                        backoff.snooze(); // примерно yield
                    } else {
                        thread::sleep(Duration::from_micros(2));
                    }
                }
            }
        }
    }
}

impl<E: Send + 'static + Clone> RxPairExt for RingReceiver<E> {
    type TxHalf = RingSender<E>;

    fn bound(cap: usize) -> (Self::TxHalf, Self) {
        RingBuffer::bounded(cap)
    }
    fn unbound() -> (Self::TxHalf, Self) {
        RingBuffer::bounded(99_999)
    }
}

impl<E: Send + 'static + Clone> TxPairExt for RingSender<E> {
    type RxHalf = RingReceiver<E>;

    fn bound(cap: usize) -> (Self, Self::RxHalf) {
        RingBuffer::bounded(cap)
    }
    fn unbound() -> (Self, Self::RxHalf) {
        RingBuffer::bounded(99_999)
    }
}

impl<E: Send + 'static> TxMarker for RingSender<E> {}

impl<E: Send + 'static> RxMarker for RingReceiver<E> {}
