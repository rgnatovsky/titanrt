use std::fmt::{Debug, Display};

use crate::utils::CorePickPolicy;
pub trait Venue: AsRef<str> + Display {}
pub trait Kind: AsRef<str> + Display {}

impl<T> Venue for T where T: AsRef<str> + Display {}
impl<T> Kind for T where T: AsRef<str> + Display {}

#[must_use]
pub trait StreamDescriptor: Debug + Clone + Send + 'static {
    fn venue(&self) -> impl Venue;
    fn kind(&self) -> impl Kind;
    fn max_pending_actions(&self) -> Option<usize>;
    fn max_pending_events(&self) -> Option<usize>;
    fn core_pick_policy(&self) -> Option<CorePickPolicy>;
    fn health_at_start(&self) -> bool;
}
