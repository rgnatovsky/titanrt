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
    /// Default implementation returns the type name
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }

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
    encoder: Arc<dyn ActionEncoder<Cmd, Encoded>>,
    /// Default context for preprocessing and encoding (type-safe!)
    default_ctx: Cmd::Ctx,
}

impl<A: EncodableAction, Encoded> ActionPipeline<A, Encoded> {
    pub fn new(encoder: Arc<dyn ActionEncoder<A, Encoded>>, ctx: Option<A::Ctx>) -> Self {
        Self {
            passes: Vec::new(),
            encoder,
            default_ctx: ctx.unwrap_or_default(),
        }
    }

    /// Create a new pipeline with a default no-op encoder
    pub fn with_default_encoder() -> Self
    where
        A::Ctx: Default,
    {
        Self {
            passes: Vec::new(),
            encoder: Arc::new(NoOpEncoder),
            default_ctx: A::Ctx::default(),
        }
    }

    pub fn set_encoder(&mut self, encoder: Arc<dyn ActionEncoder<A, Encoded>>) {
        self.encoder = encoder;
    }

    /// Register a venue-specific processor
    pub fn register_pass(&mut self, processor: Arc<dyn ActionPass<A>>) {
        self.passes.push(processor);
    }

    /// Set the default context
    pub fn set_default_context(&mut self, ctx: A::Ctx) {
        self.default_ctx = ctx;
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

        self.encoder.encode(cmd, ctx)
    }

    pub fn supports_cmd(&self, cmd: &A) -> bool {
        self.encoder.supports(cmd)
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

    /// Reset pipeline to default state
    pub fn reset(&mut self)
    where
        A::Ctx: Default,
    {
        self.clear_passes();
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
            default: default.unwrap_or_else(ActionPipeline::with_default_encoder),
            by_key: HashMap::new(),
        }
    }

    pub fn reset(&mut self) {
        self.clear();
        self.override_default(ActionPipeline::with_default_encoder());
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

/// Default no-op encoder that does nothing
#[derive(Debug, Clone)]
pub struct NoOpEncoder;

impl<A: EncodableAction, Encoded> ActionEncoder<A, Encoded> for NoOpEncoder {
    fn encode(&self, _cmd: A, _ctx: &A::Ctx) -> anyhow::Result<Vec<Encoded>> {
        Ok(Vec::new())
    }

    fn supports(&self, _cmd: &A) -> bool {
        false
    }
}

/// Null implementation of UnifiedAction for bots that don't use action pipeline.
///
/// This allows existing code to work without changes by using `Bot<E, I, O, NullUnifiedAction>`.
#[derive(Debug, Clone, Default)]
pub struct NullEncodableAction;

impl EncodableAction for NullEncodableAction {
    type Ctx = ();
}
