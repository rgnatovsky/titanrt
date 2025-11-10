use crate::connector::BaseConnector;
use crate::connector::features::shared::clients_map::{ClientConfig, ClientsMap, SpecificClient};
use crate::connector::features::websocket::client::{WebSocketClient, WebSocketClientSpec};
use crate::utils::{CancelToken, CoreStats};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::sync::Arc;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WebSocketConnectorConfig {
    pub default_max_cores: Option<usize>,
    pub specific_core_ids: Vec<usize>,
    pub use_core_stats: bool,
    pub client: ClientConfig<WebSocketClientSpec>,
}

pub struct WebSocketConnector {
    config: WebSocketConnectorConfig,
    clients_map: ClientsMap<WebSocketClient, WebSocketClientSpec>,
    cancel_token: CancelToken,
    core_stats: Option<Arc<CoreStats>>,
}

impl WebSocketConnector {
    pub fn clients_map(&self) -> ClientsMap<WebSocketClient, WebSocketClientSpec> {
        self.clients_map.clone()
    }

    pub fn upsert_client(
        &self,
        client: SpecificClient<WebSocketClientSpec>,
    ) -> anyhow::Result<Arc<WebSocketClient>> {
        self.clients_map.upsert(client)
    }

    pub fn remove_client(&self, id: usize) -> Option<Arc<WebSocketClient>> {
        self.clients_map.remove(id)
    }

    pub fn next_client_id(&self) -> usize {
        self.clients_map.next_vacant_id()
    }
}

impl BaseConnector for WebSocketConnector {
    type MainConfig = WebSocketConnectorConfig;

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

impl Display for WebSocketConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WebSocketConnector")
    }
}

#[cfg(test)]
mod tests {
    use tracing_appender::non_blocking::WorkerGuard;

    use crate::{
        connector::{
            BaseConnector, EventTxType, HookArgs,
            features::{
                shared::{clients_map::SpecificClient, events::StreamEventRaw},
                websocket::{
                    client::WebSocketClientSpec,
                    connector::{WebSocketConnector, WebSocketConnectorConfig},
                    stream::{
                        WebSocketCommand, WebSocketEvent, WebSocketMessage,
                        WebSocketStreamDescriptor,
                    },
                },
            },
        },
        io::ringbuffer::RingSender,
        utils::{CancelToken, NullReducer, NullState, logger::LoggerConfig},
    };

    fn logging_init(level: &str) -> Option<WorkerGuard> {
        let mut logger = LoggerConfig::default();
        logger.level = level.to_string();
        logger.init().unwrap()
    }

    fn ws_conn_init(endpoint: &str, conn_id: usize) -> WebSocketConnector {
        let cancel = CancelToken::new_root();
        WebSocketConnector::init(
            WebSocketConnectorConfig {
                default_max_cores: Some(4),
                specific_core_ids: vec![],
                use_core_stats: false,
                client: crate::connector::features::shared::clients_map::ClientConfig {
                    default: None,
                    specific: vec![SpecificClient {
                        id: conn_id,
                        ip: None,
                        spec: WebSocketClientSpec {
                            endpoint: endpoint.to_string(),
                            protocols: vec![],
                            headers: vec![],
                            connect_timeout_ms: Some(10000),
                            tcp_nodelay: Some(true),
                            ws_config: None,
                            use_tls: Some(true),
                        },
                    }],
                    fail_on_empty: false,
                },
            },
            cancel,
            None,
        )
        .unwrap()
    }

    fn binance_hook(
        args: HookArgs<
            StreamEventRaw<WebSocketEvent>,
            RingSender<()>,
            NullReducer,
            NullState,
            WebSocketStreamDescriptor<()>,
            (),
        >,
    ) {
        match args.raw.inner() {
            WebSocketEvent::Connected { status, .. } => {
                tracing::info!("Binance WebSocket connected with status: {}", status);
            }
            WebSocketEvent::Message(msg) => match msg {
                WebSocketMessage::Text(text) => {
                    tracing::info!("Received Binance message: {}", text);
                }
                WebSocketMessage::Binary(_) => {
                    tracing::info!("Received Binance binary message");
                }
            },
            WebSocketEvent::Closed { code, reason } => {
                tracing::info!(
                    "Binance WebSocket closed: code={:?}, reason={:?}",
                    code,
                    reason
                );
            }
            WebSocketEvent::Error(err) => {
                tracing::error!("Binance WebSocket error: {}", err);
            }
            _ => {}
        }
    }

    #[test]
    fn test_binance_websocket() {
        let _logger = logging_init("debug");

        let conn_id = 0;
        let binance_url = "wss://stream.binance.com:9443/ws/btcusdt@ticker";

        let mut ws_conn = ws_conn_init(binance_url, conn_id);

        let descriptor = WebSocketStreamDescriptor::<()>::low_latency();

        let mut binance_stream = ws_conn
            .spawn_stream(descriptor, EventTxType::Own, binance_hook)
            .unwrap();

        // Подключаемся к Binance публичному стриму BTC/USDT ticker
        let connect_action = WebSocketCommand::connect()
            .url_str(binance_url)
            .unwrap()
            .to_builder()
            .conn_id(conn_id)
            .label("binance-btcusdt-ticker")
            .build();

        binance_stream.try_send(connect_action).unwrap();

        tracing::info!("Connected to Binance WebSocket, waiting for messages...");

        // Ждем несколько секунд для получения сообщений
        std::thread::sleep(std::time::Duration::from_secs(18));

        // Отключаемся
        let disconnect_action = WebSocketCommand::disconnect()
            .to_builder()
            .label("binance-btcusdt-ticker")
            .build();

        binance_stream.try_send(disconnect_action).unwrap();

        tracing::info!("Disconnected from Binance WebSocket");
    }
}
