use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Display;
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

/// Handle to access a pipeline in the registry
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PipelineHandle(usize);

impl Display for PipelineHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone)]
pub struct ActionPipelineRegistry<A: EncodableAction, Encoded> {
    default: ActionPipeline<A, Encoded>,
    slots: Vec<Option<ActionPipeline<A, Encoded>>>,
    by_key: HashMap<String, PipelineHandle>,
    next_id: usize,
}

impl<Cmd: EncodableAction, Encoded> ActionPipelineRegistry<Cmd, Encoded>
where
    Cmd::Ctx: Default,
{
    pub fn new(default: Option<ActionPipeline<Cmd, Encoded>>) -> Self {
        Self {
            default: default.unwrap_or_else(ActionPipeline::with_default_encoder),
            slots: Vec::new(),
            by_key: HashMap::new(),
            next_id: 0,
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

    /// Register a new pipeline and return a handle to access it
    pub fn register_pipeline(&mut self, pipeline: ActionPipeline<A, Encoded>) -> PipelineHandle {
        // Try to find an empty slot first
        for (idx, slot) in self.slots.iter_mut().enumerate() {
            if slot.is_none() {
                *slot = Some(pipeline);
                return PipelineHandle(idx);
            }
        }

        // No empty slot found, push to the end
        self.slots.push(Some(pipeline));
        PipelineHandle(self.slots.len() - 1)
    }

    /// Register a new pipeline with a string key and return a handle to access it
    pub fn register_pipeline_with_key(
        &mut self,
        key: String,
        pipeline: ActionPipeline<A, Encoded>,
    ) -> PipelineHandle {
        let handle = self.register_pipeline(pipeline);
        self.by_key.insert(key, handle);
        handle
    }

    pub fn get_default(&self) -> &ActionPipeline<A, Encoded> {
        &self.default
    }

    /// Get pipeline by handle
    pub fn get(&self, handle: PipelineHandle) -> Option<&ActionPipeline<A, Encoded>> {
        self.slots.get(handle.0).and_then(|slot| slot.as_ref())
    }

    /// Get pipeline by string key, falls back to default if not found
    pub fn get_by_key(&self, key: &str) -> Option<&ActionPipeline<A, Encoded>> {
        self.by_key.get(key).and_then(|handle| self.get(*handle))
    }

    /// Get mutable pipeline by handle
    pub fn get_mut(&mut self, handle: PipelineHandle) -> Option<&mut ActionPipeline<A, Encoded>> {
        self.slots.get_mut(handle.0).and_then(|slot| slot.as_mut())
    }

    /// Get mutable pipeline by string key, falls back to default if not found
    pub fn get_mut_by_key(&mut self, key: &str) -> &mut ActionPipeline<A, Encoded> {
        let handle = self.by_key.get(key).copied();

        match handle {
            Some(h) => self
                .slots
                .get_mut(h.0)
                .and_then(|slot| slot.as_mut())
                .unwrap_or(&mut self.default),
            None => &mut self.default,
        }
    }

    /// Remove pipeline by handle
    pub fn remove(&mut self, handle: PipelineHandle) -> Option<ActionPipeline<A, Encoded>> {
        // Remove from by_key map
        self.by_key.retain(|_, h| *h != handle);
        // Remove from slots
        self.slots.get_mut(handle.0).and_then(|slot| slot.take())
    }

    /// Remove pipeline by string key
    pub fn remove_by_key(&mut self, key: &str) -> Option<ActionPipeline<A, Encoded>> {
        if let Some(handle) = self.by_key.remove(key) {
            self.remove(handle)
        } else {
            None
        }
    }

    pub fn clear(&mut self) {
        self.slots.clear();
        self.by_key.clear();
        self.next_id = 0;
    }

    pub fn contains(&self, handle: PipelineHandle) -> bool {
        self.slots
            .get(handle.0)
            .map_or(false, |slot| slot.is_some())
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.by_key.contains_key(key)
    }

    /// Get total number of registered pipelines
    pub fn len(&self) -> usize {
        self.slots.iter().filter(|slot| slot.is_some()).count()
    }

    /// Check if registry has no registered pipelines
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
