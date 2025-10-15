use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::sync::Arc;

// use crate::connector::features::composite::stream::{CompositeAction, event::StreamEventRoute};

/// Base request trait - all commands/actions must implement this
/// to be processed by the encoder pipeline
pub trait EncodableRequest: Debug + Clone + 'static {
    /// Associated type for command context
    ///
    /// This provides type-safe context instead of using `HashMap<String, Value>`.
    /// Commands can define their own context types with specific fields.
    type Ctx: Send + Default + Debug + Clone + 'static;
}

/// Request encoder trait - converts commands to venue-specific actions
pub trait Encoder<Req: EncodableRequest, Encoded>: Sync + Debug + Send {
    /// Process a command into one or more venue-specific actions
    ///
    /// # Arguments
    ///
    /// * `req` - The command to encode
    /// * `ctx` - Type-safe context specific to this command type
    fn encode(&self, req: Req, ctx: &Req::Ctx) -> anyhow::Result<Vec<Encoded>>;

    /// Get the venue ID this processor handles
    /// Default implementation returns the type name
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }

    /// Optional: validate if command is supported
    fn supports(&self, req: &Req) -> bool;
}

/// Encoder pass trait - processes a command and returns a new command
pub trait EncoderPass<Req: EncodableRequest>: Sync + Debug + Send {
    fn run(&self, req: Req, ctx: &Req::Ctx) -> anyhow::Result<Req>;
}

/// High-performance action pipeline with venue-based routing
#[derive(Debug, Clone)]
pub struct EncoderPipeline<Req: EncodableRequest, Encoded> {
    /// Pipeline passes (for preprocessing commands, validating, etc.)
    passes: Vec<Arc<dyn EncoderPass<Req>>>,
    /// Venue-specific encoders (keyed by venue ID)
    encoder: Arc<dyn Encoder<Req, Encoded>>,
    /// Default context for preprocessing and encoding (type-safe!)
    default_ctx: Req::Ctx,
}

impl<Req: EncodableRequest, Encoded> EncoderPipeline<Req, Encoded> {
    pub fn new(encoder: Arc<dyn Encoder<Req, Encoded>>, ctx: Option<Req::Ctx>) -> Self {
        Self {
            passes: Vec::new(),
            encoder,
            default_ctx: ctx.unwrap_or_default(),
        }
    }

    /// Create a new pipeline with a default no-op encoder
    pub fn with_default_encoder() -> Self
    where
        Req::Ctx: Default,
    {
        Self {
            passes: Vec::new(),
            encoder: Arc::new(NoOpEncoder),
            default_ctx: Req::Ctx::default(),
        }
    }

    pub fn set_encoder(&mut self, encoder: Arc<dyn Encoder<Req, Encoded>>) {
        self.encoder = encoder;
    }

    /// Register a venue-specific processor
    pub fn register_pass(&mut self, processor: Arc<dyn EncoderPass<Req>>) {
        self.passes.push(processor);
    }

    /// Set the default context
    pub fn set_default_context(&mut self, ctx: Req::Ctx) {
        self.default_ctx = ctx;
    }

    /// Execute a command
    pub fn execute(&self, req: Req) -> anyhow::Result<Vec<Encoded>> {
        self.execute_with_context(req, &self.default_ctx)
    }

    /// Execute a command with a specific context
    pub fn execute_with_context(
        &self,
        mut req: Req,
        ctx: &Req::Ctx,
    ) -> anyhow::Result<Vec<Encoded>> {
        for p in &self.passes {
            req = p.run(req, ctx)?;
        }

        self.encoder.encode(req, ctx)
    }

    pub fn supports_cmd(&self, req: &Req) -> bool {
        self.encoder.supports(req)
    }

    /// Remove all passes from the pipeline
    pub fn clear_passes(&mut self) {
        self.passes.clear();
    }

    /// Remove a pass at specific index, returns the removed pass if exists
    pub fn remove_pass(&mut self, index: usize) -> Option<Arc<dyn EncoderPass<Req>>> {
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
        Req::Ctx: Default,
    {
        self.clear_passes();
        self.default_ctx = Req::Ctx::default();
    }
}

/// id to access a pipeline in the registry
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EncoderId(usize);

impl Display for EncoderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone)]
pub struct EncoderRegistry<Req: EncodableRequest, Encoded> {
    default: EncoderPipeline<Req, Encoded>,
    slots: Vec<Option<EncoderPipeline<Req, Encoded>>>,
    by_key: HashMap<String, EncoderId>,
    next_id: usize,
}

impl<Req: EncodableRequest, Encoded> EncoderRegistry<Req, Encoded>
where
    Req::Ctx: Default,
{
    pub fn new(default: Option<EncoderPipeline<Req, Encoded>>) -> Self {
        Self {
            default: default.unwrap_or_else(EncoderPipeline::with_default_encoder),
            slots: Vec::new(),
            by_key: HashMap::new(),
            next_id: 0,
        }
    }

    pub fn reset(&mut self) {
        self.clear();
        self.override_default(EncoderPipeline::with_default_encoder());
    }
}

impl<A: EncodableRequest, Encoded> EncoderRegistry<A, Encoded> {
    pub fn override_default(&mut self, pipeline: EncoderPipeline<A, Encoded>) {
        self.default = pipeline;
    }

    /// Register a new pipeline and return a id to access it
    pub fn register_pipeline(&mut self, pipeline: EncoderPipeline<A, Encoded>) -> EncoderId {
        // Try to find an empty slot first
        for (idx, slot) in self.slots.iter_mut().enumerate() {
            if slot.is_none() {
                *slot = Some(pipeline);
                return EncoderId(idx);
            }
        }

        // No empty slot found, push to the end
        self.slots.push(Some(pipeline));
        EncoderId(self.slots.len() - 1)
    }

    /// Register a new pipeline with a string key and return a id to access it
    pub fn register_pipeline_with_key(
        &mut self,
        key: String,
        pipeline: EncoderPipeline<A, Encoded>,
    ) -> EncoderId {
        let id = self.register_pipeline(pipeline);
        self.by_key.insert(key, id);
        id
    }

    pub fn get_default(&self) -> &EncoderPipeline<A, Encoded> {
        &self.default
    }

    /// Get pipeline by id
    pub fn get(&self, id: EncoderId) -> Option<&EncoderPipeline<A, Encoded>> {
        self.slots.get(id.0).and_then(|slot| slot.as_ref())
    }

    /// Get pipeline by string key, falls back to default if not found
    pub fn get_by_key(&self, key: &str) -> Option<&EncoderPipeline<A, Encoded>> {
        self.by_key.get(key).and_then(|id| self.get(*id))
    }

    /// Get mutable pipeline by id
    pub fn get_mut(&mut self, id: EncoderId) -> Option<&mut EncoderPipeline<A, Encoded>> {
        self.slots.get_mut(id.0).and_then(|slot| slot.as_mut())
    }

    /// Get mutable pipeline by string key, falls back to default if not found
    pub fn get_mut_by_key(&mut self, key: &str) -> &mut EncoderPipeline<A, Encoded> {
        let id = self.by_key.get(key).copied();

        match id {
            Some(h) => self
                .slots
                .get_mut(h.0)
                .and_then(|slot| slot.as_mut())
                .unwrap_or(&mut self.default),
            None => &mut self.default,
        }
    }

    /// Remove pipeline by id
    pub fn remove(&mut self, id: EncoderId) -> Option<EncoderPipeline<A, Encoded>> {
        // Remove from by_key map
        self.by_key.retain(|_, h| *h != id);
        // Remove from slots
        self.slots.get_mut(id.0).and_then(|slot| slot.take())
    }

    /// Remove pipeline by string key
    pub fn remove_by_key(&mut self, key: &str) -> Option<EncoderPipeline<A, Encoded>> {
        if let Some(id) = self.by_key.remove(key) {
            self.remove(id)
        } else {
            None
        }
    }

    pub fn clear(&mut self) {
        self.slots.clear();
        self.by_key.clear();
        self.next_id = 0;
    }

    pub fn contains(&self, id: EncoderId) -> bool {
        self.slots.get(id.0).map_or(false, |slot| slot.is_some())
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

impl<Req: EncodableRequest, Encoded> Encoder<Req, Encoded> for NoOpEncoder {
    fn encode(&self, _cmd: Req, _ctx: &Req::Ctx) -> anyhow::Result<Vec<Encoded>> {
        Ok(Vec::new())
    }

    fn supports(&self, _cmd: &Req) -> bool {
        false
    }
}

/// Null implementation of UnifiedAction for bots that don't use action pipeline.
///
/// This allows existing code to work without changes by using `Bot<E, I, O, NullUnifiedAction>`.
#[derive(Debug, Clone, Default)]
pub struct NullEncodableAction;

impl EncodableRequest for NullEncodableAction {
    type Ctx = ();
}
