use crate::model::ModelEvent;
use serde_json::Value;

#[derive(Debug, Clone)]
pub enum Input<E: ModelEvent> {
    Event(E),
    Command(CommandInput),
}

#[derive(Debug, Clone)]
pub enum CommandInput {
    // TODO: config getter
    Start,
    Stop,
    Restart,
    HotReload(Value),
    Json(Value),
    Shutdown,
    Kill,
}
