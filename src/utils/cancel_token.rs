use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

struct CancelState {
   cancelled: AtomicBool,
   parent: Option<Arc<CancelState>>,
}

impl CancelState {
   #[inline]
   fn new_root() -> Arc<Self> {
      Arc::new(Self {
         cancelled: AtomicBool::new(false),
         parent: None,
      })
   }

   #[inline]
   fn child_of(parent: Arc<CancelState>) -> Arc<Self> {
      Arc::new(Self {
         cancelled: AtomicBool::new(false),
         parent: Some(parent),
      })
   }

   #[inline]
   fn cancel(&self) {
      // SeqCst для наглядности; можно Relaxed/Release в ultra‑HFT, если согласуешь модель.
      self.cancelled.store(true, Ordering::Relaxed);
   }

   #[inline]
   fn is_cancelled(&self) -> bool {
      if self.cancelled.load(Ordering::Relaxed) {
         return true;
      }
      // Хвостовая рекурсия по родителям очень короткая в реальности.
      if let Some(ref p) = self.parent {
         return p.is_cancelled();
      }
      false
   }
}

#[derive(Clone)]
pub struct CancelToken {
   state: Arc<CancelState>,
}

impl Debug for CancelToken {
   fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
      f.debug_struct("StdCancelToken")
       .field("is_cancelled", &self.is_cancelled())
       .finish()
   }
}

impl CancelToken {
   #[inline]
   pub fn new_root() -> Self {
      Self {
         state: CancelState::new_root(),
      }
   }

   #[inline]
   pub fn cancel(&self) {
      self.state.cancel();
   }

   #[inline]
   pub fn is_cancelled(&self) -> bool {
      self.state.is_cancelled()
   }

   #[inline]
   pub fn new_child(&self) -> Self {
      Self {
         state: CancelState::child_of(self.state.clone()),
      }
   }
}
