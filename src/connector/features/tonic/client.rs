use std::str::FromStr;
use std::time::Duration;
use std::{net::IpAddr, sync::Arc};

use anyhow::Result;
use tokio::runtime::Runtime;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint, Uri};

use serde::{Deserialize, Serialize};

use crate::connector::features::shared::clients_map::{ClientInitializer, SpecificClient};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TonicChannelSpec {
    pub uri: String,
    pub connect_timeout_ms: Option<u64>,
    pub request_timeout_ms: Option<u64>,
    pub tcp_nodelay: Option<bool>,
    pub http2_keepalive_interval_ms: Option<u64>,
    pub http2_keepalive_timeout_ms: Option<u64>,
    pub tls: bool,
}

#[derive(Clone)]
pub struct TonicClient(Channel);

impl TonicClient {
    pub fn channel(&self) -> Channel {
        self.0.clone()
    }
}

impl ClientInitializer<TonicChannelSpec> for TonicClient {
    fn init(cfg: &SpecificClient<TonicChannelSpec>, rt: Option<Arc<Runtime>>) -> Result<Self> {
        let rt = rt.ok_or_else(|| anyhow::anyhow!("[TonicClient] Tokio Runtime is required"))?;

        let mut endpoint = Endpoint::from_shared(cfg.spec.uri.clone())
            .map_err(|e| anyhow::anyhow!("[TonicClient] invalid uri '{}': {e}", cfg.spec.uri))?;

        if cfg.spec.tls {
            let server_name = {
                let uri = Uri::try_from(&cfg.spec.uri).map_err(|_| {
                    anyhow::anyhow!("[TonicClient] invalid https uri: {}", cfg.spec.uri)
                })?;
                let host = uri.host().ok_or_else(|| {
                    anyhow::anyhow!(
                        "[TonicClient] invalid https uri (no host): {}",
                        cfg.spec.uri
                    )
                })?;

                let is_ip = host.parse::<IpAddr>().is_ok();

                if is_ip {
                    tracing::warn!(
                        "[TonicClient] URI host is an IP address: '{host}'. TLS certificate validation may fail.",
                    );
                }

                host.to_string()
            };

            let tls = ClientTlsConfig::new()
                .with_native_roots()
                .domain_name(server_name);

            endpoint = endpoint
                .tls_config(tls)
                .map_err(|e| anyhow::anyhow!("[TonicClient] tls config error: {e}"))?;
        }

        if let Some(ip_str) = cfg.ip.as_deref() {
            let ip = IpAddr::from_str(ip_str)
                .map_err(|e| anyhow::anyhow!("[TonicClient] IP parse error: {e}"))?;
            endpoint = endpoint.local_address(Some(ip));
        }

        endpoint = endpoint.connect_timeout(Duration::from_millis(
            cfg.spec.connect_timeout_ms.unwrap_or(5000),
        ));

        endpoint = endpoint.http2_keep_alive_interval(Duration::from_millis(
            cfg.spec.http2_keepalive_interval_ms.unwrap_or(30000),
        ));

        endpoint = endpoint.keep_alive_timeout(Duration::from_millis(
            cfg.spec.http2_keepalive_timeout_ms.unwrap_or(20000),
        ));

        endpoint = endpoint.timeout(Duration::from_millis(
            cfg.spec.request_timeout_ms.unwrap_or(10000),
        ));
        endpoint = endpoint.tcp_nodelay(cfg.spec.tcp_nodelay.unwrap_or(true));

        let client = rt
            .block_on(async { endpoint.connect().await })
            .map_err(|e| anyhow::anyhow!("[TonicClient] connect error: {e:#}"))?;

        Ok(TonicClient(client))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tokio::runtime::Builder;

    use crate::connector::features::{
        shared::clients_map::ClientInitializer, tonic::client::TonicClient,
    };

    #[test]
    fn tonicclient_init_panics_without_runtime() {
        use crate::connector::features::shared::clients_map::SpecificClient;

        let rt_tokio = Arc::new(
            Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
                .map_err(|e| anyhow::anyhow!("Tokio Runtime error: {e}"))
                .unwrap(),
        );

        let cfg = SpecificClient {
            spec: super::TonicChannelSpec {
                uri: "http://127.0.0.1:50051".to_string(),
                connect_timeout_ms: None,
                request_timeout_ms: None,
                tcp_nodelay: None,
                http2_keepalive_interval_ms: None,
                http2_keepalive_timeout_ms: None,
                tls: false,
            },
            ip: None,
            id: 1,
        };

        TonicClient::init(&cfg, Some(rt_tokio.clone())).unwrap();
    }
}
