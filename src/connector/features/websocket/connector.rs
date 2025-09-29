use crate::connector::BaseConnector;
use crate::utils::{CancelToken, CoreStats};
use serde::{Deserialize, Serialize};
use std::{fmt::Display, sync::Arc};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WsConnectorConfig {
    pub default_max_cores: Option<usize>,
    pub specific_core_ids: Vec<usize>,
    pub use_core_stats: bool,
}

pub struct WsConnector {
    config: WsConnectorConfig,
    cancel_token: CancelToken,
    core_stats: Option<Arc<CoreStats>>,
}

impl BaseConnector for WsConnector {
    type MainConfig = WsConnectorConfig;

    fn init(
        config: Self::MainConfig,
        cancel_token: CancelToken,
        reserved_core_ids: Option<Vec<usize>>,
    ) -> anyhow::Result<Self> {
        let core_stats = if config.use_core_stats {
            Some(CoreStats::new(
                config.default_max_cores,
                config.specific_core_ids.clone(),
                reserved_core_ids.unwrap_or_default(),
            )?)
        } else {
            None
        };
        Ok(Self {
            config,
            cancel_token,
            core_stats,
        })
    }

    fn name(&self) -> impl AsRef<str> + Display {
        "websocket"
    }
    fn config(&self) -> &Self::MainConfig {
        &self.config
    }
    fn cancel_token(&self) -> &CancelToken {
        &self.cancel_token
    }
    fn cores_stats(&self) -> Option<Arc<CoreStats>> {
        self.core_stats.clone()
    }
}
