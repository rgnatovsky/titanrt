use crate::model::ModelEvent;
use serde_json::Value;

/// Control-plane input for the runtime: either a typed event
/// or a command that drives the model lifecycle.
#[derive(Debug, Clone)]
pub enum Input<E: ModelEvent> {
    /// User/application event forwarded to the model.
    Event(E),
    /// Lifecycle or config command.
    Command(CommandInput),
}

/// Commands accepted by the runtime control plane.
///
/// These drive model lifecycle, reconfiguration, and termination.
#[derive(Debug, Clone)]
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
    /// Forward raw JSON to `BaseModel::json_command`.
    Json(Value),
    /// Stop the model and end the runtime thread.
    Shutdown,
    /// Immediate termination intent (cancel everything).
    Kill,
}
