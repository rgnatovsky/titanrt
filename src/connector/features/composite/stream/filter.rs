use ahash::AHashMap;
use titanrt::utils::SharedStr;

/// HFT-optimized stream filter for a single strategy.
///
/// Uses AHashMap for O(1) lookup with minimal overhead.
/// Empty filter means strategy accepts ALL events (backward compatibility).
#[derive(Debug, Default, Clone)]
pub struct StreamFilter {
    /// Maps stream name to whether the strategy wants events from it.
    /// Empty = accept all streams (no filtering)
    streams: AHashMap<SharedStr, bool>,
}

impl StreamFilter {
    #[inline]
    pub fn new() -> Self {
        Self {
            streams: AHashMap::new(),
        }
    }

    /// Register that this strategy wants events from the given stream
    #[inline]
    pub fn add_stream(&mut self, name: SharedStr) {
        self.streams.insert(name, true);
    }

    /// Check if strategy should process events from this stream.
    /// Returns true if:
    /// - Filter is empty (strategy accepts all streams), OR
    /// - Stream is explicitly registered in the filter
    #[inline]
    pub fn should_process(&self, stream_name: &SharedStr) -> bool {
        self.streams.is_empty() || self.streams.get(stream_name).copied().unwrap_or(false)
    }

    /// Returns true if this filter has no registered streams (accepts all)
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.streams.is_empty()
    }

    /// Number of streams this strategy is subscribed to
    #[inline]
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.streams.len()
    }
}
