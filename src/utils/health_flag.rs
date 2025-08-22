use crossbeam::utils::CachePadded;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Cheaply clonable flag to track worker or stream health.
/// Wraps an `AtomicBool` in `Arc<CachePadded<...>>` to avoid false sharing.
#[derive(Clone)]
#[repr(transparent)]
pub struct HealthFlag(Arc<CachePadded<AtomicBool>>);

impl HealthFlag {
    /// Create a new flag with the given initial value.
    #[inline]
    pub fn new(initial: bool) -> Self {
        Self(Arc::new(CachePadded::new(AtomicBool::new(initial))))
    }

    /// Wrap an existing `Arc` of the inner atomic.
    #[inline]
    pub fn from_arc(inner: Arc<CachePadded<AtomicBool>>) -> Self {
        Self(inner)
    }

    #[inline(always)]
    fn atomic(&self) -> &AtomicBool {
        &self.0
    }

    /// Get the current value (relaxed load).
    #[inline(always)]
    pub fn get(&self) -> bool {
        self.atomic().load(Ordering::Relaxed)
    }

    /// Set the flag (relaxed store).
    #[inline(always)]
    pub fn set(&self, v: bool) {
        self.atomic().store(v, Ordering::Relaxed)
    }

    /// Convenience: mark as healthy.
    #[inline(always)]
    pub fn up(&self) {
        self.set(true);
    }

    /// Convenience: mark as unhealthy.
    #[inline(always)]
    pub fn down(&self) {
        self.set(false);
    }

    /// Clone out the underlying `Arc`.
    #[inline]
    pub fn as_arc(&self) -> Arc<CachePadded<AtomicBool>> {
        self.0.clone()
    }

    /// Get with Acquire ordering (stronger than relaxed).
    #[inline]
    pub fn get_acquire(&self) -> bool {
        self.atomic().load(Ordering::Acquire)
    }

    /// Set with Release ordering (stronger than relaxed).
    #[inline]
    pub fn set_release(&self, v: bool) {
        self.atomic().store(v, Ordering::Release)
    }
}

impl fmt::Debug for HealthFlag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HealthFlag")
         .field("value", &self.get())
         .finish()
    }
}
