use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

// use crate::connector::features::composite::stream::{CompositeAction, event::StreamEventRoute};

/// Base action trait - all commands must implement this
/// to be processed by the action pipeline
pub trait EncodableAction: Debug + Clone + 'static {
    /// Associated type for command context
    ///
    /// This provides type-safe context instead of using `HashMap<String, Value>`.
    /// Commands can define their own context types with specific fields.
    type Ctx: Send + Default + Debug + Clone + 'static;

    /// Get the encoder id for this command: e.g., "bybit"
    fn encoder_key(&self) -> impl AsRef<str>;
}

/// Action encoder trait - converts commands to venue-specific actions
pub trait ActionEncoder<A: EncodableAction, Encoded>: Sync + Debug + Send {
    /// Process a command into one or more venue-specific actions
    ///
    /// # Arguments
    ///
    /// * `cmd` - The command to encode
    /// * `ctx` - Type-safe context specific to this command type
    fn encode(&self, cmd: A, ctx: &A::Ctx) -> anyhow::Result<Vec<Encoded>>;

    /// Get the venue ID this processor handles
    fn key(&self) -> &str;

    /// Optional: validate if command is supported
    fn supports(&self, cmd: &A) -> bool;
}

/// Action pass trait - processes a command and returns a new command
pub trait ActionPass<A: EncodableAction>: Sync + Debug + Send {
    fn run(&self, cmd: A, ctx: &A::Ctx) -> anyhow::Result<A>;
}

/// High-performance action pipeline with venue-based routing
#[derive(Debug, Clone)]
pub struct ActionPipeline<Cmd: EncodableAction, Encoded> {
    /// Pipeline passes (for preprocessing commands, validating, etc.)
    passes: Vec<Arc<dyn ActionPass<Cmd>>>,
    /// Venue-specific encoders (keyed by venue ID)
    encoders: HashMap<String, Arc<dyn ActionEncoder<Cmd, Encoded>>>,
    /// Default context for preprocessing and encoding (type-safe!)
    default_ctx: Cmd::Ctx,
}

impl<Cmd: EncodableAction, Encoded> ActionPipeline<Cmd, Encoded>
where
    Cmd::Ctx: Default,
{
    pub fn new() -> Self {
        Self {
            passes: Vec::new(),
            encoders: HashMap::new(),
            default_ctx: Cmd::Ctx::default(),
        }
    }
}

impl<A: EncodableAction, Encoded> ActionPipeline<A, Encoded> {
    /// Create a new pipeline with a specific default context
    pub fn with_default_context(ctx: A::Ctx) -> Self {
        Self {
            passes: Vec::new(),
            encoders: HashMap::new(),
            default_ctx: ctx,
        }
    }

    /// Register a venue-specific processor
    pub fn register_pass(&mut self, processor: Arc<dyn ActionPass<A>>) {
        self.passes.push(processor);
    }

    /// Set the default context
    pub fn set_default_context(&mut self, ctx: A::Ctx) {
        self.default_ctx = ctx;
    }

    /// Register a venue-specific encoder
    ///
    /// This allows different encoding settings per venue (e.g., different timeouts)
    pub fn register_encoder(&mut self, encoder: Arc<dyn ActionEncoder<A, Encoded>>) {
        self.encoders.insert(encoder.key().to_string(), encoder);
    }

    /// Execute a command
    pub fn execute(&self, cmd: A) -> anyhow::Result<Vec<Encoded>> {
        self.execute_with_context(cmd, &self.default_ctx)
    }

    /// Execute a command with a specific context
    pub fn execute_with_context(&self, mut cmd: A, ctx: &A::Ctx) -> anyhow::Result<Vec<Encoded>> {
        for p in &self.passes {
            cmd = p.run(cmd, ctx)?;
        }

        let enc = self
            .encoders
            .get(cmd.encoder_key().as_ref())
            .ok_or_else(|| {
                anyhow::anyhow!("no encoder for venue={}", cmd.encoder_key().as_ref())
            })?;

        enc.encode(cmd, ctx)
    }

    /// Check if venue is supported
    pub fn supports_venue(&self, venue: impl AsRef<str>) -> bool {
        self.encoders.contains_key(venue.as_ref())
    }

    /// Get list of supported venues
    pub fn supported_venues(&self) -> Vec<&str> {
        self.encoders.values().map(|p| p.key()).collect()
    }

    pub fn supports_cmd(&self, cmd: &A) -> bool {
        self.encoders.values().any(|p| p.supports(cmd))
    }
    pub fn supports_venue_cmd(&self, venue: impl AsRef<str>, cmd: &A) -> bool {
        self.encoders
            .get(venue.as_ref())
            .map(|p| p.supports(cmd))
            .unwrap_or(false)
    }

    /// Remove all passes from the pipeline
    pub fn clear_passes(&mut self) {
        self.passes.clear();
    }

    /// Remove a pass at specific index, returns the removed pass if exists
    pub fn remove_pass(&mut self, index: usize) -> Option<Arc<dyn ActionPass<A>>> {
        if index < self.passes.len() {
            Some(self.passes.remove(index))
        } else {
            None
        }
    }

    /// Get number of registered passes
    pub fn pass_count(&self) -> usize {
        self.passes.len()
    }
    /// Remove all encoders from the pipeline
    pub fn clear_encoders(&mut self) {
        self.encoders.clear();
    }

    /// Remove encoder for specific venue, returns the removed encoder if exists
    pub fn remove_encoder(
        &mut self,
        venue: impl AsRef<str>,
    ) -> Option<Arc<dyn ActionEncoder<A, Encoded>>> {
        self.encoders.remove(venue.as_ref())
    }

    /// Get number of registered encoders
    pub fn encoder_count(&self) -> usize {
        self.encoders.len()
    }

    /// Reset pipeline to default state
    pub fn reset(&mut self)
    where
        A::Ctx: Default,
    {
        self.clear_passes();
        self.clear_encoders();
        self.default_ctx = A::Ctx::default();
    }
}

#[derive(Debug, Clone)]
pub struct ActionPipelineRegistry<A: EncodableAction, Encoded> {
    default: ActionPipeline<A, Encoded>,
    by_key: HashMap<String, ActionPipeline<A, Encoded>>,
}

impl<Cmd: EncodableAction, Encoded> ActionPipelineRegistry<Cmd, Encoded>
where
    Cmd::Ctx: Default,
{
    pub fn new(default: Option<ActionPipeline<Cmd, Encoded>>) -> Self {
        Self {
            default: default.unwrap_or_else(ActionPipeline::new),
            by_key: HashMap::new(),
        }
    }

    pub fn reset(&mut self) {
        self.clear();
        self.override_default(ActionPipeline::new());
    }
}

impl<A: EncodableAction, Encoded> ActionPipelineRegistry<A, Encoded> {
    pub fn override_default(&mut self, pipeline: ActionPipeline<A, Encoded>) {
        self.default = pipeline;
    }

    pub fn register_pipeline(&mut self, key: String, pipeline: ActionPipeline<A, Encoded>) {
        self.by_key.insert(key, pipeline);
    }

    pub fn get_default(&self) -> &ActionPipeline<A, Encoded> {
        &self.default
    }

    pub fn get(&self, key: &str) -> &ActionPipeline<A, Encoded> {
        self.by_key.get(key).unwrap_or(&self.default)
    }
    pub fn get_mut(&mut self, key: &str) -> &mut ActionPipeline<A, Encoded> {
        self.by_key.get_mut(key).unwrap_or(&mut self.default)
    }

    pub fn remove(&mut self, key: &str) -> Option<ActionPipeline<A, Encoded>> {
        self.by_key.remove(key)
    }

    pub fn clear(&mut self) {
        self.by_key.clear();
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.by_key.contains_key(key)
    }
}

/// Null implementation of UnifiedAction for bots that don't use action pipeline.
///
/// This allows existing code to work without changes by using `Bot<E, I, O, NullUnifiedAction>`.
#[derive(Debug, Clone, Default)]
pub struct NullEncodableAction;

impl EncodableAction for NullEncodableAction {
    type Ctx = ();

    fn encoder_key(&self) -> impl AsRef<str> {
        "null"
    }
}
