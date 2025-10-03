use std::{net::IpAddr, str::FromStr, sync::Arc, time::Duration};

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

use crate::connector::{
    errors::StreamError,
    features::shared::clients_map::{ClientInitializer, SpecificClient},
};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReqwestClientSpec {
    pub pool_max_idle_per_host: usize,
    pub pool_idle_timeout_sec: u64,
    pub request_timeout_ms: Option<u64>,
}

#[derive(Clone)]
pub struct ReqwestClient(pub Client);

impl ClientInitializer<ReqwestClientSpec> for ReqwestClient {
    fn init(
        cfg: &SpecificClient<ReqwestClientSpec>,
        _rt: Option<Arc<Runtime>>,
    ) -> anyhow::Result<Self> {
        let mut builder = reqwest::ClientBuilder::new()
            .pool_max_idle_per_host(cfg.spec.pool_max_idle_per_host)
            .pool_idle_timeout(Duration::from_secs(cfg.spec.pool_idle_timeout_sec))
            .tcp_nodelay(true);

        if let Some(t) = cfg.spec.request_timeout_ms {
            builder = builder.timeout(Duration::from_millis(t));
        }

        if let Some(ip) = cfg.ip.as_deref() {
            let ip = IpAddr::from_str(ip).map_err(|e| {
                StreamError::Unknown(anyhow::anyhow!("[ReqwestClient] IP parse error: {e}"))
            })?;

            builder = builder.local_address(ip)
        }

        let client = builder.build().map_err(|e| {
            StreamError::Unknown(anyhow::anyhow!("[ReqwestClient] Build error: {e}"))
        })?;

        let conn = ReqwestClient(client);
        Ok(conn)
    }
}
