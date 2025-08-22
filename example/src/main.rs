mod test_model;
mod connector;

use crate::test_model::{TestModel, TestModelContext};
use crossbeam::channel::bounded;
use std::sync::{Arc, Mutex};
use titanrt::config::RuntimeConfig;
use titanrt::prelude::*;
use tracing::Level;

/// Initialize and configure tracing_tools.
pub fn setup_simple_tracing(level: &str) {
    let level = match level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    tracing_subscriber::fmt().with_max_level(level).init();
}

pub fn main() {
    setup_simple_tracing("info");

    let cfg = RuntimeConfig {
        init_model_on_start: true,
        core_id: Some(8),
        max_inputs_pending: Some(128),
        max_inputs_drain: None,
        stop_model_timeout: Some(5),
    };

    let (_outputs, _) = bounded::<Output<NullEvent>>(100);

    let model_ctx = TestModelContext {
        count: Arc::new(Mutex::new(0)),
    };

    let rt =
        Runtime::<TestModel>::spawn(cfg, model_ctx, "test_model_cfg_string".to_string(), NullTx);

    rt.unwrap().run_blocking().unwrap()
}
