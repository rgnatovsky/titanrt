use std::{net::IpAddr, str::FromStr, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use url::Url;

use crate::connector::features::shared::clients_map::{ClientInitializer, SpecificClient};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WebSocketClientSpec {
    pub endpoint: String,
    #[serde(default)]
    pub protocols: Vec<String>,
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    #[serde(default)]
    pub connect_timeout_ms: Option<u64>,
    #[serde(default)]
    pub max_message_size: Option<usize>,
    #[serde(default)]
    pub tcp_nodelay: Option<bool>,
}

#[derive(Clone)]
pub struct WebSocketClient {
    pub url: Url,
    pub protocols: Vec<String>,
    pub headers: Vec<(HeaderName, HeaderValue)>,
    pub connect_timeout: Option<Duration>,
    pub max_message_size: Option<usize>,
    pub tcp_nodelay: bool,
    pub local_ip: Option<IpAddr>,
}

impl ClientInitializer<WebSocketClientSpec> for WebSocketClient {
    fn init(cfg: &SpecificClient<WebSocketClientSpec>, _rt: Option<Arc<Runtime>>) -> Result<Self> {
        let url = Url::parse(&cfg.spec.endpoint)
            .with_context(|| format!("invalid websocket endpoint: {}", cfg.spec.endpoint))?;

        let mut headers = Vec::with_capacity(cfg.spec.headers.len());
        for (name, value) in &cfg.spec.headers {
            let header_name = HeaderName::from_str(name)
                .with_context(|| format!("invalid header name: {name}"))?;
            let header_value = HeaderValue::from_str(value)
                .with_context(|| format!("invalid header value for {name}"))?;
            headers.push((header_name, header_value));
        }

        let local_ip = match cfg.ip.as_deref() {
            Some(raw) => Some(
                IpAddr::from_str(raw)
                    .with_context(|| format!("invalid local ip address: {raw}"))?,
            ),
            None => None,
        };

        Ok(Self {
            url,
            protocols: cfg.spec.protocols.clone(),
            headers,
            connect_timeout: cfg
                .spec
                .connect_timeout_ms
                .map(|ms| Duration::from_millis(ms)),
            max_message_size: cfg.spec.max_message_size,
            tcp_nodelay: cfg.spec.tcp_nodelay.unwrap_or(true),
            local_ip,
        })
    }
}
