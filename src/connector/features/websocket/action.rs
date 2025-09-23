use bytes::Bytes;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub enum WsSendPayload {
    Text(String),
    Binary(Bytes),
}

/// Информация для Connect
#[derive(Debug, Clone, Deserialize)]
pub struct WsConnectConfig {
    pub url: String,
    /// опционально: HTTP-заголовки (например, auth)
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    /// таймауты (мс) на чтение/запись
    #[serde(default)]
    pub read_timeout_ms: Option<u64>,
    #[serde(default)]
    pub write_timeout_ms: Option<u64>,
    /// авто-пинг раз в N сек (0/None — выкл)
    #[serde(default)]
    pub ping_every_secs: Option<u64>,
}

/// Управляющее действие WebSocket-стрима
#[derive(Debug, Clone, Deserialize)]
pub enum WsAction {
    /// Создать/пересоздать подключение (по `conn_id` из StreamAction)
    Connect(WsConnectConfig),
    /// Отправить текст/бинарь в уже открытое соединение
    Send(WsSendPayload),
    /// Локально закрыть соединение и убрать из пула
    Disconnect,
}