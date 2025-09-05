use crate::connector::errors::StreamError;
use crate::connector::BaseConnector;
use crate::utils::{CancelToken, CoreStats};
use ahash::AHashMap;
use anyhow::anyhow;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

pub const DEFAULT_IP_ID: u16 = u16::MAX;

#[derive(Clone, Deserialize, Serialize)]
pub struct ReqwestConnectorConfig {
    pub default_max_cores: Option<usize>,
    pub specific_core_ids: Vec<usize>,
    pub use_core_stats: bool,
    pub local_ips: Option<Vec<(u16, String)>>,
    pub pool_idle_timeout_sec: Option<u64>,
    pub pool_max_idle_per_host: Option<usize>,
    pub request_timeout_ms: Option<u64>,
}

pub struct ReqwestConnector {
    config: ReqwestConnectorConfig,
    clients_map: AHashMap<u16, Client>,
    cancel_token: CancelToken,
    core_stats: Option<Arc<CoreStats>>,
}

impl ReqwestConnector {
    pub fn clients_map(&self) -> &AHashMap<u16, Client> {
        &self.clients_map
    }
}

impl BaseConnector for ReqwestConnector {
    type MainConfig = ReqwestConnectorConfig;

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

        let local_ips = config.local_ips.clone().unwrap_or_default();
        let pool_idle_timeout_sec = config.pool_idle_timeout_sec.unwrap_or(90);
        let pool_max_idle_per_host = config.pool_max_idle_per_host.unwrap_or(8);
        let mut clients_map = AHashMap::new();

        for (id, ip) in local_ips.iter() {
            let client = create_client(
                pool_max_idle_per_host,
                pool_idle_timeout_sec,
                config.request_timeout_ms,
                Some(ip.as_str()),
            )?;
            clients_map.insert(*id, client);
        }

        let default_client = create_client(
            pool_max_idle_per_host,
            pool_idle_timeout_sec,
            config.request_timeout_ms,
            None,
        )?;

        clients_map.insert(DEFAULT_IP_ID, default_client);

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

impl Display for ReqwestConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BinanceConnector")
    }
}

fn create_client(
    pool_max_idle_per_host: usize,
    pool_idle_timeout_sec: u64,
    request_timeout_ms: Option<u64>,
    ip: Option<&str>,
) -> anyhow::Result<Client, StreamError> {
    let mut builder = reqwest::ClientBuilder::new()
        .pool_max_idle_per_host(pool_max_idle_per_host)
        .pool_idle_timeout(Duration::from_secs(pool_idle_timeout_sec))
        .tcp_nodelay(true);

    if let Some(t) = request_timeout_ms {
        builder = builder.timeout(Duration::from_millis(t));
    }

    if let Some(ip) = ip {
        let ip = IpAddr::from_str(ip)
            .map_err(|e| StreamError::Unknown(anyhow!("ip parse error: {e}")))?;

        builder = builder.local_address(ip)
    }

    builder
        .build()
        .map_err(|e| StreamError::Unknown(anyhow!("reqwest client build error: {e}")))
}
