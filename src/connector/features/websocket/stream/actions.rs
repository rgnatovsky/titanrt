use std::str::FromStr;

use bytes::Bytes;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tracing::warn;
use url::Url;

use crate::connector::features::shared::actions::StreamActionBuilder;

use super::message::WebSocketMessage;

#[derive(Debug, Clone)]
pub enum WebSocketCommand {
    Connect(WebSocketConnect),
    Send(WebSocketMessage),
    Ping(Bytes),
    Pong(Bytes),
    Disconnect,
}

impl WebSocketCommand {
    pub fn connect() -> WebSocketConnectBuilder {
        WebSocketConnectBuilder::default()
    }

    pub fn send_text<T: Into<String>>(message: T) -> Self {
        Self::Send(WebSocketMessage::text(message))
    }

    pub fn send_binary<B: Into<Bytes>>(message: B) -> Self {
        Self::Send(WebSocketMessage::binary(message))
    }

    pub fn ping<B: Into<Bytes>>(payload: B) -> Self {
        Self::Ping(payload.into())
    }

    pub fn pong<B: Into<Bytes>>(payload: B) -> Self {
        Self::Pong(payload.into())
    }

    pub fn disconnect() -> Self {
        Self::Disconnect
    }

    pub fn to_builder(self) -> StreamActionBuilder<WebSocketCommand> {
        StreamActionBuilder::new(Some(self))
    }
}

#[derive(Debug, Clone, Default)]
pub struct WebSocketConnectBuilder {
    override_url: Option<Url>,
    protocols: Vec<String>,
    headers: Vec<(HeaderName, HeaderValue)>,
    initial_messages: Vec<WebSocketMessage>,
}

impl WebSocketConnectBuilder {
    pub fn url(mut self, url: Url) -> Self {
        self.override_url = Some(url);
        self
    }

    pub fn url_str(mut self, value: &str) -> anyhow::Result<Self> {
        let url = Url::parse(value)?;
        self.override_url = Some(url);
        Ok(self)
    }

    pub fn protocol<S: Into<String>>(mut self, protocol: S) -> Self {
        self.protocols.push(protocol.into());
        self
    }

    pub fn protocols<I, S>(mut self, protocols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.protocols.extend(protocols.into_iter().map(Into::into));
        self
    }

    pub fn header(mut self, name: &str, value: &str) -> Self {
        match (HeaderName::from_str(name), HeaderValue::from_str(value)) {
            (Ok(name), Ok(value)) => self.headers.push((name, value)),
            _ => warn!(%name, %value, "websocket connect header is invalid"),
        }
        self
    }

    pub fn initial_message(mut self, message: WebSocketMessage) -> Self {
        self.initial_messages.push(message);
        self
    }

    pub fn initial_text<T: Into<String>>(self, text: T) -> Self {
        self.initial_message(WebSocketMessage::text(text))
    }

    pub fn initial_binary<B: Into<Bytes>>(self, bytes: B) -> Self {
        self.initial_message(WebSocketMessage::binary(bytes))
    }

    pub fn build(self) -> WebSocketCommand {
        WebSocketCommand::Connect(WebSocketConnect {
            override_url: self.override_url,
            extra_protocols: self.protocols,
            extra_headers: self.headers,
            initial_messages: self.initial_messages,
        })
    }

    pub fn to_builder(self) -> StreamActionBuilder<WebSocketCommand> {
        self.build().to_builder()
    }
}

#[derive(Debug, Clone)]
pub struct WebSocketConnect {
    pub(crate) override_url: Option<Url>,
    pub(crate) extra_protocols: Vec<String>,
    pub(crate) extra_headers: Vec<(HeaderName, HeaderValue)>,
    pub(crate) initial_messages: Vec<WebSocketMessage>,
}
