// yellowstone_grpc
use arc_swap::ArcSwap;
use crossbeam::utils::CachePadded;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Marker trait for state reducers.
/// Must be `Send + 'static + Default`.
pub trait Reducer: Send + 'static + Default {}

/// Empty reducer for cases where no internal state is needed.
#[derive(Default)]
pub struct NullReducer;
impl Reducer for NullReducer {}

/// Marker trait for state snapshots stored in [`StateCell`].
/// Must be `Send + Sync + Default + 'static`.
///
/// States can optionally support internal mutation without replacing
/// the entire snapshot by implementing mutation methods that work
/// with interior mutability (e.g., via `RwLock`, `Mutex`, atomics).
pub trait StateMarker: Default + Sync + Send + 'static {
    fn configure<D>(&mut self, _desc: D, _reducer: &mut impl Reducer) {}
}

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
///
/// # Internal Mutation
///
/// States can implement internal mutability (via `RwLock`, `Mutex`, atomics)
/// and use [`mutate_internal`](Self::mutate_internal) to modify data without
/// replacing the entire snapshot. The sequence counter still increments,
/// notifying observers of changes.
///
/// # Example: Full Snapshot Replacement
/// ```ignore
/// #[derive(Default)]
/// struct Config {
///     timeout: u64,
/// }
/// impl StateMarker for Config {}
///
/// let cell = StateCell::new(Config { timeout: 100 });
/// cell.publish(Config { timeout: 200 }); // Replace entire state
/// ```
///
/// # Example: Internal Mutation
/// ```ignore
/// use std::sync::{Arc, RwLock};
///
/// struct Metrics {
///     counters: Arc<RwLock<HashMap<String, u64>>>,
/// }
/// impl StateMarker for Metrics {}
///
/// let cell = StateCell::new(Metrics { ... });
///
/// // Modify without cloning the entire state
/// cell.mutate_internal(|state| {
///     let mut counters = state.counters.write().unwrap();
///     *counters.entry("requests".into()).or_insert(0) += 1;
/// });
///
/// // Observers detect the change via seq
/// if cell.changed_since(last_seq) {
///     println!("Metrics updated!");
/// }
/// ```
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

    /// Provides a temporary borrow of the object inside.
    ///
    /// This returns a proxy object allowing access to the thing held inside. However, there's
    /// only limited amount of possible cheap proxies in existence for each thread ‒ if more are
    /// created, it falls back to equivalent of [`load`](#method.load) internally.
    ///
    /// This is therefore a good choice to use for eg. searching a data structure or juggling the
    /// pointers around a bit, but not as something to store in larger amounts. The rule of thumb
    /// is this is suited for local variables on stack, but not in long-living data structures.
    ///
    /// # Consistency
    ///
    /// In case multiple related operations are to be done on the loaded value, it is generally
    /// recommended to call `peek` just once and keep the result over calling it multiple times.
    /// First, keeping it is usually faster. But more importantly, the value can change between the
    /// calls to peek, returning different objects, which could lead to logical inconsistency.
    /// Keeping the result makes sure the same object is used.
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

    /// Modify internal state without replacing the snapshot.
    ///
    /// This method allows calling a closure that can mutate the state's
    /// internal data (e.g., via `RwLock`, `Mutex`, or atomics) without
    /// creating a new Arc snapshot. The sequence counter is incremented
    /// to notify observers of the change.
    ///
    /// # Example
    /// ```ignore
    /// // State with internal mutability
    /// struct MyState {
    ///     data: Arc<RwLock<Vec<String>>>,
    /// }
    ///
    /// let cell = StateCell::new(MyState { ... });
    ///
    /// // Modify without replacing snapshot
    /// cell.mutate_internal(|state| {
    ///     let mut data = state.data.write().unwrap();
    ///     data.push("new item".to_string());
    /// });
    /// ```
    #[inline]
    pub fn mutate_internal<F>(&self, f: F)
    where
        F: FnOnce(&S),
    {
        let guard = self.snap.load();
        f(&*guard);
        self.seq.fetch_add(1, Ordering::Release);
    }

    /// Modify internal state and return a result.
    ///
    /// Similar to [`mutate_internal`](Self::mutate_internal), but allows
    /// the closure to return a value.
    ///
    /// # Example
    /// ```ignore
    /// let count = cell.mutate_internal_with(|state| {
    ///     let mut data = state.data.write().unwrap();
    ///     data.push("item".to_string());
    ///     data.len()
    /// });
    /// ```
    #[inline]
    pub fn mutate_internal_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&S) -> R,
    {
        let guard = self.snap.load();
        let result = f(&*guard);
        self.seq.fetch_add(1, Ordering::Release);
        result
    }
}
