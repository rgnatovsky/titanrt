// yellowstone_grpc
use arc_swap::ArcSwap;
use crossbeam::utils::CachePadded;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub trait StateMarker: Default + Sync + Send + 'static {}

#[derive(Default)]
pub struct NullState;

impl StateMarker for NullState {}

#[derive(Debug)]
pub struct StateCell<State: StateMarker> {
    snap: ArcSwap<State>,
    seq: CachePadded<AtomicU64>,
}

impl<S: StateMarker> StateCell<S> {
    pub fn new(init: S) -> Self {
        Self {
            snap: ArcSwap::from(Arc::new(init)),
            seq: CachePadded::new(AtomicU64::new(1)),
        }
    }

    pub fn new_arc(init: S) -> Arc<Self> {
        Arc::new(Self::new(init))
    }

    pub fn new_default() -> Arc<Self> {
        Arc::new(Self::new(S::default()))
    }

    #[inline]
    pub fn publish_arc(&self, next: Arc<S>) {
        self.snap.store(next);
        self.seq.fetch_add(1, Ordering::Release);
    }

    #[inline]
    pub fn publish(&self, next: S) {
        self.publish_arc(Arc::new(next));
    }

    // producer side
    pub fn update(&self, next: Arc<S>) {
        self.snap.store(next);
        self.seq.fetch_add(1, Ordering::Release);
    }
    #[inline]
    pub fn peek(&self) -> arc_swap::Guard<Arc<S>> {
        self.snap.load()
    }

    #[inline]
    pub fn peek_if_changed(&self, last_seq: &mut u64) -> Option<arc_swap::Guard<Arc<S>>> {
        let cur = self.seq.load(Ordering::Acquire);
        if cur == *last_seq {
            return None;
        }
        *last_seq = cur;
        Some(self.snap.load())
    }

    #[inline]
    pub fn load(&self) -> Arc<S> {
        self.snap.load_full()
    }

    #[inline]
    pub fn with<R>(&self, f: impl FnOnce(&S, u64) -> R) -> R {
        let g = self.snap.load();
        let cur = self.seq.load(Ordering::Acquire);
        f(&*g, cur)
    }

    #[inline]
    pub fn with_if_changed<R>(&self, last_seq: &mut u64, f: impl FnOnce(&S) -> R) -> Option<R> {
        let cur = self.seq.load(Ordering::Acquire);
        if cur == *last_seq {
            return None;
        }
        let g = self.snap.load(); // после Acquire — видим новые байты снапшота
        *last_seq = cur;
        Some(f(&*g))
    }

    #[inline]
    pub fn seq(&self) -> u64 {
        self.seq.load(Ordering::Acquire)
    }
    #[inline]
    pub fn changed_since(&self, last: u64) -> bool {
        self.seq() != last
    }
}
