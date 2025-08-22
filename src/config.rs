use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RuntimeConfig {
    pub init_model_on_start: bool,
    pub core_id: Option<usize>,
    pub max_inputs_pending: Option<usize>,
    pub max_inputs_drain: Option<usize>,
    pub stop_model_timeout: Option<u64>,
}
