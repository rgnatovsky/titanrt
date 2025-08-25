use std::fmt;
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Internal cancellation state, shared via [`Arc`].
/// Each state may optionally have a parent, so that
/// cancelling a parent cancels all of its descendants.
struct CancelState {
    cancelled: AtomicBool,
    parent: Option<Arc<CancelState>>,
}

impl CancelState {
    /// Create a root state (no parent).
    #[inline]
    fn new_root() -> Arc<Self> {
        Arc::new(Self {
            cancelled: AtomicBool::new(false),
            parent: None,
        })
    }

    /// Create a child state linked to a parent.
    #[inline]
    fn child_of(parent: Arc<CancelState>) -> Arc<Self> {
        Arc::new(Self {
            cancelled: AtomicBool::new(false),
            parent: Some(parent),
        })
    }

    /// Mark this state as cancelled.
    #[inline]
    fn cancel(&self) {
        // Relaxed is enough for visibility in this simple model;
        // use stronger ordering if you need cross-thread guarantees.
        self.cancelled.store(true, Ordering::Relaxed);
    }

    /// Check whether this or any ancestor has been cancelled.
    #[inline]
    fn is_cancelled(&self) -> bool {
        if self.cancelled.load(Ordering::Relaxed) {
            return true;
        }
        if let Some(ref p) = self.parent {
            return p.is_cancelled();
        }
        false
    }
}

/// Hierarchical cancellation token.
///
/// A `CancelToken` can be cloned cheaply and checked at any time.
/// Cancelling a parent token cancels all of its children.
#[derive(Clone)]
pub struct CancelToken {
    state: Arc<CancelState>,
}

impl Debug for CancelToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancelToken")
            .field("is_cancelled", &self.is_cancelled())
            .finish()
    }
}

impl CancelToken {
    /// Create a new root cancellation token.
    #[inline]
    pub fn new_root() -> Self {
        Self {
            state: CancelState::new_root(),
        }
    }

    /// Cancel this token (and propagate to all children).
    #[inline]
    pub fn cancel(&self) {
        self.state.cancel();
    }

    /// Check if this token (or any ancestor) has been cancelled.
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.state.is_cancelled()
    }

    /// Create a new child token linked to this one.
    #[inline]
    pub fn new_child(&self) -> Self {
        Self {
            state: CancelState::child_of(self.state.clone()),
        }
    }
    /// Sleep until the token is cancelled or the specified duration has elapsed.
    /// If the token is cancelled, return false, otherwise return true.
    #[inline]
    pub fn sleep_cancellable(&self, total: Duration) -> bool {

        let mut slept = Duration::ZERO;
        let tick = Duration::from_millis(50);
        while slept < total {
            if self.is_cancelled() {
                return false;
            }
            std::thread::sleep(tick.min(total - slept));
            slept += tick;
        }
        true
    }
}
