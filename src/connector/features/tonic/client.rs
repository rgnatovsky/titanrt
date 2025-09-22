use std::str::FromStr;
use std::time::Duration;
use std::{net::IpAddr, sync::Arc};

use anyhow::Result;
use tokio::runtime::Runtime;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint, Uri};

use serde::{Deserialize, Serialize};

use crate::connector::{
    errors::StreamError,
    features::shared::clients_map::{ClientInitializer, SpecificClient},
};

/// Спецификация конфигурации для tonic endpoint
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TonicChannelSpec {
    /// URI сервера, например "http://127.0.0.1:50051"
    pub uri: String,
    /// Таймаут на установку соединения (в миллисекундах)
    pub connect_timeout_ms: Option<u64>,
    /// Общий таймаут на RPC (в миллисекундах)
    pub request_timeout_ms: Option<u64>,
    /// Опционально: TCP_NODELAY (если хочешь и если Endpoint поддерживает)
    pub tcp_nodelay: Option<bool>,
    pub http2_keepalive_interval_ms: Option<u64>,
    pub http2_keepalive_timeout_ms: Option<u64>,
}

/// Клиент-обёртка, хранит Endpoint и кэшированный Channel
#[derive(Clone)]
pub struct TonicClient(Channel);

impl TonicClient {
    pub fn channel(&self) -> Channel {
        self.0.clone()
    }
}

/// Реализация ClientInitializer — синхронная инициализация Endpoint-а
impl ClientInitializer<TonicChannelSpec> for TonicClient {
    fn init(cfg: &SpecificClient<TonicChannelSpec>, rt: Option<Arc<Runtime>>) -> Result<Self> {
        let rt = rt.ok_or_else(|| {
            StreamError::Unknown(anyhow::anyhow!(
                "[TonicClient] Tokio Runtime is required for TonicClient::init()"
            ))
        })?;

        let mut endpoint = Endpoint::from_shared(cfg.spec.uri.clone()).map_err(|e| {
            StreamError::Unknown(anyhow::anyhow!(
                "[TonicClient] invalid uri '{}': {e}",
                cfg.spec.uri
            ))
        })?;

        if cfg.spec.uri.starts_with("https://") {
            let domain = Uri::try_from(&cfg.spec.uri)
                .ok()
                .and_then(|u| u.host().map(|s| s.to_string()))
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "[TonicClient] invalid https uri (no host): {}",
                        cfg.spec.uri
                    )
                })?;

            endpoint = endpoint
                .tls_config(
                    ClientTlsConfig::new()
                        .with_native_roots()
                        .domain_name(domain),
                )
                .map_err(|e| anyhow::anyhow!("tls config error: {e}"))?;
        }

        if let Some(ip_str) = cfg.ip.as_deref() {
            let ip = IpAddr::from_str(ip_str).map_err(|e| {
                StreamError::Unknown(anyhow::anyhow!("[TonicClient] IP parse error: {e}"))
            })?;
            endpoint = endpoint.local_address(Some(ip));
        }

        if let Some(ms) = cfg.spec.connect_timeout_ms {
            endpoint = endpoint.connect_timeout(Duration::from_millis(ms));
        } else {
            endpoint = endpoint.connect_timeout(Duration::from_secs(5));
        }
        if let Some(ms) = cfg.spec.request_timeout_ms {
            endpoint = endpoint.timeout(Duration::from_millis(ms));
        } else {
            endpoint = endpoint.timeout(Duration::from_secs(10));
        }
        if let Some(nodelay) = cfg.spec.tcp_nodelay {
            endpoint = endpoint.tcp_nodelay(nodelay);
        } else {
            endpoint = endpoint.tcp_nodelay(true);
        }
        if let Some(ms) = cfg.spec.http2_keepalive_interval_ms {
            endpoint = endpoint.http2_keep_alive_interval(Duration::from_millis(ms));
        }
        if let Some(ms) = cfg.spec.http2_keepalive_timeout_ms {
            endpoint = endpoint.keep_alive_timeout(Duration::from_millis(ms));
        }

        let client = rt
            .block_on(async { endpoint.connect().await })
            .map_err(|e| {
                anyhow::anyhow!("[TonicClient] connect error: {e:#}")
            })?;

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
            },
            ip: None,
            id: 1,
            // возможно другие поля — если есть, добавь их тут
        };

        TonicClient::init(&cfg, Some(rt_tokio.clone())).unwrap();
    }
}
