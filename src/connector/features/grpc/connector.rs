use crate::connector::BaseConnector;
use crate::utils::{CancelToken, CoreStats};

use ahash::AHashMap;
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use tokio::runtime::{Builder, Handle};
use std::{fmt::Display, sync::Arc, time::Duration};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

pub const DEFAULT_CONN_ID: u16 = u16::MAX;

#[derive(Clone, Deserialize, Serialize)]
pub struct GrpcConnectorConfig {
    pub default_max_cores: Option<usize>,
    pub specific_core_ids: Vec<usize>,
    pub use_core_stats: bool,

    /// id -> URL (например "https://host:443")
    pub endpoints: Option<Vec<(u16, String)>>,

    pub connect_timeout_ms: Option<u64>, // таймауты, keepalive;
    pub http2_keepalive_interval_ms: Option<u64>,
    pub http2_keepalive_timeout_ms: Option<u64>,
    pub request_timeout_ms: Option<u64>,
}

#[derive(Clone)]
pub struct GrpcClients {
    pub channel: Channel,
}

pub struct GrpcConnector {
    config: GrpcConnectorConfig,
    clients_map: AHashMap<u16, GrpcClients>,
    cancel_token: CancelToken,
    core_stats: Option<Arc<CoreStats>>,
}

impl GrpcConnector {
    pub fn clients_map(&self) -> &AHashMap<u16, GrpcClients> { &self.clients_map }
}

impl BaseConnector for GrpcConnector {
    type MainConfig = GrpcConnectorConfig;
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
        } else { None };

        let connect_timeout = Duration::from_millis(config.connect_timeout_ms.unwrap_or(10_000));
        let req_timeout     = config.request_timeout_ms.map(Duration::from_millis);
        let ka_interval     = config.http2_keepalive_interval_ms.map(Duration::from_millis);
        let ka_timeout      = config.http2_keepalive_timeout_ms.map(Duration::from_millis);

        let endpoints = config.endpoints.clone().unwrap_or_default();
        let mut clients_map = AHashMap::new();

        for (id, url) in endpoints {
            let channel = create_channel(&url, connect_timeout, req_timeout, ka_interval, ka_timeout)?;
            clients_map.insert(id, GrpcClients { channel });
        }

        Ok(Self { config, clients_map, cancel_token, core_stats })
    }

    fn name(&self) -> impl AsRef<str> + Display { "grpc" }
    fn config(&self) -> &Self::MainConfig { &self.config }
    fn cancel_token(&self) -> &CancelToken { &self.cancel_token }
    fn cores_stats(&self) -> Option<Arc<CoreStats>> { self.core_stats.clone() }
}

impl Display for GrpcConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "GrpcConnector") }
}

fn create_channel(
    url: &str,
    connect_timeout: Duration,
    req_timeout: Option<Duration>,
    http2_keepalive_interval: Option<Duration>,
    http2_keepalive_timeout: Option<Duration>,
) -> anyhow::Result<Channel> {
    let mut ep = Endpoint::from_shared(url.to_string())
        .map_err(|e| anyhow!("invalid gRPC url: {e}"))?
        .connect_timeout(connect_timeout);

    if url.starts_with("https://") {
        ep = ep
            .tls_config(ClientTlsConfig::new())
            .map_err(|e| anyhow!("tls config error: {e}"))?;
    }
    if let Some(t) = req_timeout {
        ep = ep.timeout(t);
    }
    if let Some(iv) = http2_keepalive_interval {
        ep = ep.http2_keep_alive_interval(iv).keep_alive_while_idle(true);
    }
    if let Some(to) = http2_keepalive_timeout {
        ep = ep.keep_alive_timeout(to);
    }

    // ВАЖНО: не блокируемся внутри уже идущего Tokio
    if Handle::try_current().is_ok() {
        Ok(ep.connect_lazy())
    } else {
        let rt = Builder::new_current_thread().enable_io().enable_time().build()?;
        Ok(rt.block_on(ep.connect())?)
    }
}