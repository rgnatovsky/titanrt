use bytes::Bytes;
use tokio_tungstenite::tungstenite::http::{HeaderMap, StatusCode};

use crate::connector::features::shared::events::StreamEventInner;

use super::message::WebSocketMessage;

#[derive(Debug, Clone)]
pub enum WebSocketEvent {
    Connected {
        status: StatusCode,
        status_code: u16,
        protocol: Option<String>,
        headers: HeaderMap,
    },
    Message(WebSocketMessage),
    Ping(Bytes),
    Pong(Bytes),
    Closed {
        code: Option<u16>,
        reason: Option<String>,
    },
    Error(String),
}

impl WebSocketEvent {
    pub fn connected(status: StatusCode, protocol: Option<String>, headers: HeaderMap) -> Self {
        Self::Connected {
            status_code: status.as_u16(),
            status,
            protocol,
            headers,
        }
    }

    pub fn closed(code: Option<u16>, reason: Option<String>) -> Self {
        Self::Closed { code, reason }
    }

    pub fn error<E: Into<String>>(err: E) -> Self {
        Self::Error(err.into())
    }

    pub fn protocol(&self) -> Option<&str> {
        match self {
            WebSocketEvent::Connected { protocol, .. } => protocol.as_deref(),
            _ => None,
        }
    }

    pub fn headers(&self) -> Option<&HeaderMap> {
        match self {
            WebSocketEvent::Connected { headers, .. } => Some(headers),
            _ => None,
        }
    }
}

impl StreamEventInner for WebSocketEvent {
    type Body = WebSocketMessage;
    type Err = String;
    type Code = u16;

    fn status(&self) -> Option<&Self::Code> {
        match self {
            WebSocketEvent::Connected { status_code, .. } => Some(status_code),
            WebSocketEvent::Closed {
                code: Some(code), ..
            } => Some(code),
            _ => None,
        }
    }

    fn is_ok(&self) -> bool {
        !matches!(self, WebSocketEvent::Error(_))
    }

    fn error(&self) -> Option<&Self::Err> {
        match self {
            WebSocketEvent::Error(err) => Some(err),
            _ => None,
        }
    }

    fn body(&self) -> Option<&Self::Body> {
        match self {
            WebSocketEvent::Message(msg) => Some(msg),
            _ => None,
        }
    }

    fn into_body(self) -> Option<Self::Body> {
        match self {
            WebSocketEvent::Message(msg) => Some(msg),
            _ => None,
        }
    }
}
