use crossbeam::utils::CachePadded;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone)]
#[repr(transparent)]
pub struct HealthFlag(Arc<CachePadded<AtomicBool>>);

impl HealthFlag {
    #[inline]
    pub fn new(initial: bool) -> Self {
        Self(Arc::new(CachePadded::new(AtomicBool::new(initial))))
    }

    #[inline]
    pub fn from_arc(inner: Arc<CachePadded<AtomicBool>>) -> Self {
        Self(inner)
    }

    #[inline(always)]
    fn atomic(&self) -> &AtomicBool {
        &self.0
    }

    #[inline(always)]
    pub fn get(&self) -> bool {
        self.atomic().load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn set(&self, v: bool) {
        self.atomic().store(v, Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn up(&self) {
        self.set(true);
    }
    #[inline(always)]
    pub fn down(&self) {
        self.set(false);
    }

    #[inline]
    pub fn as_arc(&self) -> Arc<CachePadded<AtomicBool>> {
        self.0.clone()
    }

    // при желании:
    #[inline]
    pub fn get_acquire(&self) -> bool {
        self.atomic().load(Ordering::Acquire)
    }
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
