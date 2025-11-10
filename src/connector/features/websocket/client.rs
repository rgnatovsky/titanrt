use std::{net::IpAddr, str::FromStr, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tungstenite::protocol::WebSocketConfig;
use url::Url;

use crate::connector::features::shared::clients_map::{ClientInitializer, SpecificClient};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WebSocketConfigSpec {
    #[serde(default)]
    pub write_buffer_size: Option<usize>,
    #[serde(default)]
    pub max_write_buffer_size: Option<usize>,
    #[serde(default)]
    pub max_message_size: Option<usize>,
    #[serde(default)]
    pub max_frame_size: Option<usize>,
    #[serde(default)]
    pub accept_unmasked_frames: Option<bool>,
}

impl WebSocketConfigSpec {
    fn into_config(self) -> WebSocketConfig {
        let default = WebSocketConfig::default();
        WebSocketConfig {
            #[allow(deprecated)]
            max_send_queue: None,
            write_buffer_size: self.write_buffer_size.unwrap_or(default.write_buffer_size),
            max_write_buffer_size: self
                .max_write_buffer_size
                .unwrap_or(default.max_write_buffer_size),
            max_message_size: self.max_message_size.or(default.max_message_size),
            max_frame_size: self.max_frame_size.or(default.max_frame_size),
            accept_unmasked_frames: self
                .accept_unmasked_frames
                .unwrap_or(default.accept_unmasked_frames),
        }
    }
}

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
    pub tcp_nodelay: Option<bool>,
    #[serde(default)]
    pub use_tls: Option<bool>,
    #[serde(default)]
    pub ws_config: Option<WebSocketConfigSpec>,
}

#[derive(Clone)]
pub struct WebSocketClient {
    pub url: Url,
    pub protocols: Vec<String>,
    pub headers: Vec<(HeaderName, HeaderValue)>,
    pub connect_timeout: Option<Duration>,
    pub tcp_nodelay: bool,
    pub local_ip: Option<IpAddr>,
    pub use_tls: bool,
    pub ws_config: WebSocketConfig,
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

        let ws_config = if let Some(ws_config_spec) = cfg.spec.ws_config.clone() {
            let config = ws_config_spec.into_config();

            config
        } else {
            WebSocketConfig::default()
        };

        Ok(Self {
            url,
            protocols: cfg.spec.protocols.clone(),
            headers,
            connect_timeout: cfg
                .spec
                .connect_timeout_ms
                .map(|ms| Duration::from_millis(ms)),
            tcp_nodelay: cfg.spec.tcp_nodelay.unwrap_or(true),
            local_ip,
            use_tls: cfg.spec.use_tls.unwrap_or(true),
            ws_config,
        })
    }
}
