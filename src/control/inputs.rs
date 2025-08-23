use crate::model::ModelEvent;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// Control-plane input for the runtime: either a typed event
/// or a command that drives the model lifecycle.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", content = "payload")]
pub enum InputPayload<E: ModelEvent> {
    /// User/application event forwarded to the model.
    Event(E),
    /// Lifecycle or config command.
    Command(CommandInput),
}

/// Commands accepted by the runtime control plane.
///
/// These drive model lifecycle, reconfiguration, and termination.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum CommandInput {
    // TODO: config getter
    /// Initialize the model if not running.
    Start,
    /// Request cooperative stop of the model (runtime stays alive).
    Stop,
    /// Stop and re-initialize the model.
    Restart,
    /// Apply new config via `BaseModel::hot_reload`.
    HotReload(Value),
    /// Stop the model and end the runtime thread.
    Shutdown,
    /// Immediate termination intent (cancel everything).
    Kill,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InputMeta {
    pub id: Uuid,
    pub source: Option<String>,
}

impl Default for InputMeta {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4(),
            source: None,
        }
    }
}

/// Generic input for the runtime/model with optional metadata.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Input<E: ModelEvent> {
    #[serde(flatten)]
    pub payload: InputPayload<E>,
    pub meta: Option<InputMeta>,
}

impl<E: ModelEvent> Input<E> {
    /// Create from an existing payload without metadata.
    pub fn new(payload: InputPayload<E>) -> Self {
        Self {
            payload,
            meta: None,
        }
    }

    /// Convenience constructor for an event.
    pub fn event(ev: E) -> Self {
        Self::new(InputPayload::Event(ev))
    }

    /// Convenience constructor for a command.
    pub fn command(cmd: CommandInput) -> Self {
        Self::new(InputPayload::Command(cmd))
    }

    /// Set metadata entirely.
    pub fn with_meta(mut self, meta: InputMeta) -> Self {
        self.meta = Some(meta);
        self
    }

    /// Set or replace the id.
    pub fn with_id(mut self, id: Uuid) -> Self {
        Self::ensure_meta(&mut self.meta);
        self.meta.as_mut().unwrap().id = id;
        self
    }

    /// Set the source.
    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        Self::ensure_meta(&mut self.meta);
        self.meta.as_mut().unwrap().source = Some(source.into());
        self
    }

    /// Получить id, если есть мета.
    pub fn id(&self) -> Option<Uuid> {
        self.meta.as_ref().map(|m| m.id)
    }

    /// Источник как &str, если есть.
    pub fn source(&self) -> Option<&str> {
        self.meta.as_ref().and_then(|m| m.source.as_deref())
    }

    /// Быстрая проверка типа payload.
    pub fn is_event(&self) -> bool {
        matches!(self.payload, InputPayload::Event(_))
    }

    pub fn is_command(&self) -> bool {
        matches!(self.payload, InputPayload::Command(_))
    }

    /// Маппинг типа события без аллокации/копии команды.
    pub fn map_event<T: ModelEvent>(self, f: impl FnOnce(E) -> T) -> Input<T> {
        let payload = match self.payload {
            InputPayload::Event(ev) => InputPayload::Event(f(ev)),
            InputPayload::Command(c) => InputPayload::Command(c),
        };
        Input {
            payload,
            meta: self.meta,
        }
    }

    /// Обеспечить наличие метаданных; вернёт &mut для точечной правки.
    pub fn meta_mut(&mut self) -> &mut InputMeta {
        Self::ensure_meta(&mut self.meta);
        self.meta.as_mut().unwrap()
    }

    fn ensure_meta(meta: &mut Option<InputMeta>) {
        if meta.is_none() {
            *meta = Some(InputMeta::default());
        }
    }
}

// Утилиты и преобразования

impl<E: ModelEvent> From<E> for Input<E> {
    fn from(ev: E) -> Self {
        Input::event(ev)
    }
}

impl<E: ModelEvent> From<CommandInput> for Input<E> {
    fn from(cmd: CommandInput) -> Self {
        Input::command(cmd)
    }
}
