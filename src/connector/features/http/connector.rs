use crate::connector::BaseConnector;
use crate::connector::features::http::client::{ReqwestClient, ReqwestClientSpec};
use crate::connector::features::http::config::HttpConnectorConfig;
use crate::connector::features::shared::clients_map::{ClientsMap, SpecificClient};
use crate::utils::{CancelToken, CoreStats};
use std::fmt::Display;

use std::sync::Arc;

pub struct HttpConnector {
    config: HttpConnectorConfig,
    clients_map: ClientsMap<ReqwestClient, ReqwestClientSpec>,
    cancel_token: CancelToken,
    core_stats: Option<Arc<CoreStats>>,
}

impl HttpConnector {
    pub fn clients_map(&self) -> ClientsMap<ReqwestClient, ReqwestClientSpec> {
        self.clients_map.clone()
    }

    pub fn upsert_client(
        &self,
        client: SpecificClient<ReqwestClientSpec>,
    ) -> anyhow::Result<Arc<ReqwestClient>> {
        self.clients_map.upsert(client)
    }

    pub fn remove_client(&self, id: usize) -> Option<Arc<ReqwestClient>> {
        self.clients_map.remove(id)
    }

    pub fn next_client_id(&self) -> usize {
        self.clients_map.next_vacant_id()
    }
}

impl BaseConnector for HttpConnector {
    type MainConfig = HttpConnectorConfig;

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
        let clients_map = ClientsMap::new(&config.client, None)?;

        Ok(Self {
            config,
            clients_map,
            cancel_token,
            core_stats,
        })
    }

    fn name(&self) -> impl AsRef<str> + Display {
        "reqwest"
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

impl Display for HttpConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BinanceConnector")
    }
}
