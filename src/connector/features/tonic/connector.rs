use crate::connector::BaseConnector;
use crate::connector::features::shared::clients_map::{ClientConfig, ClientsMap};
use crate::connector::features::tonic::client::{TonicChannelSpec, TonicClient};
use crate::utils::{CancelToken, CoreStats};

use serde::{Deserialize, Serialize};
use std::{fmt::Display, sync::Arc};

pub const DEFAULT_CONN_ID: u16 = u16::MAX;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TonicConnectorConfig {
    pub default_max_cores: Option<usize>,
    pub specific_core_ids: Vec<usize>,
    pub use_core_stats: bool,
    pub client: ClientConfig<TonicChannelSpec>,
}

pub struct TonicConnector {
    config: TonicConnectorConfig,
    clients_map: ClientsMap<TonicClient, TonicChannelSpec>,
    cancel_token: CancelToken,
    core_stats: Option<Arc<CoreStats>>,
}

impl TonicConnector {
    pub fn clients_map(&self) -> ClientsMap<TonicClient, TonicChannelSpec> {
        self.clients_map.clone()
    }
}

impl BaseConnector for TonicConnector {
    type MainConfig = TonicConnectorConfig;
    ///  создаются gRPC-каналы (tonic::transport::Channel) через create_channel().
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

        let clients_map = ClientsMap::new(&config.client)?;

        Ok(Self {
            config,
            clients_map,
            cancel_token,
            core_stats,
        })
    }

    fn name(&self) -> impl AsRef<str> + Display {
        "tonic"
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

impl Display for TonicConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TonicConnector")
    }
}
