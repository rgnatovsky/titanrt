use bytes::Bytes;

#[derive(Debug, Clone)]
pub enum WebSocketMessage {
    Text(String),
    Binary(Bytes),
}

impl WebSocketMessage {
    #[inline]
    pub fn text<T: Into<String>>(value: T) -> Self {
        Self::Text(value.into())
    }

    #[inline]
    pub fn binary<B: Into<Bytes>>(value: B) -> Self {
        Self::Binary(value.into())
    }

    #[inline]
    pub fn as_text(&self) -> Option<&str> {
        match self {
            WebSocketMessage::Text(text) => Some(text.as_str()),
            WebSocketMessage::Binary(_) => None,
        }
    }

    #[inline]
    pub fn as_bytes(&self) -> Option<&Bytes> {
        match self {
            WebSocketMessage::Binary(bytes) => Some(bytes),
            WebSocketMessage::Text(_) => None,
        }
    }

    #[inline]
    pub fn into_bytes(self) -> Bytes {
        match self {
            WebSocketMessage::Text(text) => Bytes::from(text.into_bytes()),
            WebSocketMessage::Binary(bytes) => bytes,
        }
    }
}
