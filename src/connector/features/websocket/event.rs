use crate::connector::features::shared::events::StreamEventInner;
use anyhow::anyhow;
use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct WsEvent {
    code: Option<u16>,
    err_msg: Option<String>,
    body: Option<Bytes>,
    is_message: bool,
}

impl WsEvent {
    #[inline]
    pub fn connected() -> Self {
        Self {
            code: None,
            err_msg: None,
            body: None,
            is_message: false,
        }
    }

    #[inline]
    pub fn disconnected(code: Option<u16>, reason: Option<String>) -> Self {
        let msg = reason.unwrap_or_default();
        Self {
            code,
            err_msg: if msg.is_empty() { None } else { Some(msg) },
            body: None,
            is_message: false,
        }
    }

    #[inline]
    pub fn from_text(s: String) -> Self {
        Self {
            code: None,
            err_msg: None,
            body: Some(Bytes::from(s.into_bytes())),
            is_message: true,
        }
    }

    #[inline]
    pub fn from_binary(b: Vec<u8>) -> Self {
        Self {
            code: None,
            err_msg: None,
            body: Some(Bytes::from(b)),
            is_message: true,
        }
    }

    #[inline]
    pub fn from_error<E: std::fmt::Display>(e: E) -> Self {
        Self {
            code: None,
            err_msg: Some(e.to_string()),
            body: None,
            is_message: false,
        }
    }

    #[inline]
    pub fn is_message(&self) -> bool {
        self.is_message
    }

    pub fn body_as_text(&self) -> anyhow::Result<String> {
        match &self.body {
            Some(b) => std::str::from_utf8(b)
                .map(|s| s.to_string())
                .map_err(|e| anyhow!(e)),
            None => Err(anyhow!("no body")),
        }
    }
}

impl StreamEventInner for WsEvent {
    type Body = Bytes;
    type Err = String;
    type Code = u16;

    fn status(&self) -> Option<&Self::Code> {
        self.code.as_ref()
    }
    fn is_ok(&self) -> bool {
        self.err_msg.is_none()
    }
    fn error(&self) -> Option<&Self::Err> {
        self.err_msg.as_ref()
    }
    fn body(&self) -> Option<&Self::Body> {
        self.body.as_ref()
    }
    fn into_body(self) -> Option<Self::Body> {
        self.body
    }
}
