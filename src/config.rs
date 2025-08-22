use serde::{Deserialize, Serialize};

/// Runtime configuration for the model control thread.
/// Keeps lifecycle and back-pressure knobs small and explicit.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RuntimeConfig {
    /// Initialize the model immediately on runtime start.
    /// If `false`, you must send a `Start` command later.
    pub init_model_on_start: bool,

    /// Logical CPU core to pin the runtime thread to (`None` = no pinning).
    pub core_id: Option<usize>,

    /// Max number of pending control-plane inputs (`None` = 1024).
    pub max_inputs_pending: Option<usize>,

    /// Max inputs drained per iteration (`None` = max_inputs_pending).
    pub max_inputs_drain: Option<usize>,

    /// Cooperative stop timeout **in seconds** (`None` = 300 seconds).
    pub stop_model_timeout: Option<u64>,
}