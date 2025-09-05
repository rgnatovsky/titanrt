// yellowstone_grpc
use arc_swap::ArcSwap;
use crossbeam::utils::CachePadded;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Marker trait for state reducers.
/// Must be `Send + 'static + Default`.
pub trait Reducer: Send + 'static + Default {}

/// Empty reducer for cases where no internal state is needed.
#[derive(Default)]
pub struct NullReducer;
impl Reducer for NullReducer {}

/// Marker trait for state snapshots stored in [`StateCell`].
/// Must be `Send + Sync + Default + 'static`.
pub trait StateMarker: Default + Sync + Send + 'static {}

/// Empty state marker for cases where no state is needed.
#[derive(Default)]
pub struct NullState;
impl StateMarker for NullState {}

/// Lock-free snapshot cell with versioning.
///
/// Internally uses [`ArcSwap`] for atomic snapshot replacement
/// and an `AtomicU64` sequence counter for change detection.
///
/// Readers can cheaply peek or check if the snapshot changed
/// without blocking writers.
#[derive(Debug)]
pub struct StateCell<State: StateMarker> {
    snap: ArcSwap<State>,
    seq: CachePadded<AtomicU64>,
}

impl<S: StateMarker> StateCell<S> {
    /// Create a new cell with an initial state.
    pub fn new(init: S) -> Self {
        Self {
            snap: ArcSwap::from(Arc::new(init)),
            seq: CachePadded::new(AtomicU64::new(1)),
        }
    }

    /// Create a new cell wrapped in [`Arc`].
    pub fn new_arc(init: S) -> Arc<Self> {
        Arc::new(Self::new(init))
    }

    /// Create a new cell with the state's default value, wrapped in [`Arc`].
    pub fn new_default() -> Arc<Self> {
        Arc::new(Self::new(S::default()))
    }

    /// Publish a new snapshot (by Arc).
    #[inline]
    pub fn publish_arc(&self, next: Arc<S>) {
        self.snap.store(next);
        self.seq.fetch_add(1, Ordering::Release);
    }

    /// Publish a new snapshot (by value).
    #[inline]
    pub fn publish(&self, next: S) {
        self.publish_arc(Arc::new(next));
    }

    /// Alias for publishing; can be used by producers.
    pub fn update(&self, next: Arc<S>) {
        self.snap.store(next);
        self.seq.fetch_add(1, Ordering::Release);
    }

    /// Load a guard to the current snapshot (cheap, lock-free).
    #[inline]
    pub fn peek(&self) -> arc_swap::Guard<Arc<S>> {
        self.snap.load()
    }

    /// Load a snapshot only if sequence changed since `last_seq`.
    /// Updates `last_seq` if changed.
    #[inline]
    pub fn peek_if_changed(&self, last_seq: &mut u64) -> Option<arc_swap::Guard<Arc<S>>> {
        let cur = self.seq.load(Ordering::Acquire);
        if cur == *last_seq {
            return None;
        }
        *last_seq = cur;
        Some(self.snap.load())
    }

    /// Get the current snapshot as an owned [`Arc`].
    #[inline]
    pub fn load(&self) -> Arc<S> {
        self.snap.load_full()
    }

    /// Apply a closure to the current snapshot and sequence.
    #[inline]
    pub fn with<R>(&self, f: impl FnOnce(&S, u64) -> R) -> R {
        let g = self.snap.load();
        let cur = self.seq.load(Ordering::Acquire);
        f(&*g, cur)
    }

    /// Apply a closure if the sequence changed since `last_seq`.
    /// Updates `last_seq` if changed.
    #[inline]
    pub fn with_if_changed<R>(&self, last_seq: &mut u64, f: impl FnOnce(&S) -> R) -> Option<R> {
        let cur = self.seq.load(Ordering::Acquire);
        if cur == *last_seq {
            return None;
        }
        let g = self.snap.load(); // after Acquire we see new snapshot bytes
        *last_seq = cur;
        Some(f(&*g))
    }

    /// Current sequence number.
    #[inline]
    pub fn seq(&self) -> u64 {
        self.seq.load(Ordering::Acquire)
    }

    /// Check if sequence changed since `last`.
    #[inline]
    pub fn changed_since(&self, last: u64) -> bool {
        self.seq() != last
    }
}
