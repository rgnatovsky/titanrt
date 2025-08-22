use std::fmt::{Debug, Display};

use crate::utils::CorePickPolicy;

/// Marker for a venue identifier (e.g. exchange name).
pub trait Venue: AsRef<str> + Display {}

/// Marker for a stream kind identifier (e.g. "trades", "orderbook").
pub trait Kind: AsRef<str> + Display {}

impl<T> Venue for T where T: AsRef<str> + Display {}
impl<T> Kind for T where T: AsRef<str> + Display {}

/// Descriptor of a stream to be spawned by a connector.
///
/// A `StreamDescriptor` declares what the stream represents
/// (venue/kind), how its channels are bounded, its CPU core
/// affinity policy, and its initial health flag.
#[must_use]
pub trait StreamDescriptor: Debug + Clone + Send + 'static {
    /// Human-readable venue identifier (e.g. "binance").
    fn venue(&self) -> impl Venue;

    /// Human-readable kind identifier (e.g. "trades").
    fn kind(&self) -> impl Kind;

    /// Optional bound for the actions channel (model → worker).
    fn max_pending_actions(&self) -> Option<usize>;

    /// Optional bound for the events channel (worker → model).
    fn max_pending_events(&self) -> Option<usize>;

    /// Optional CPU core pick policy (round-robin, specific core, etc).
    fn core_pick_policy(&self) -> Option<CorePickPolicy>;

    /// Initial health state of the stream.
    fn health_at_start(&self) -> bool;
}
