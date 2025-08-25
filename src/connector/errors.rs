use std::error::Error;
use std::fmt;

pub type StreamResult<T> = Result<T, StreamError>;
#[derive(Debug)]
pub enum StreamError {
    Cancelled,
    SendFull,
    SendClosed,
    Timeout,
    JoinPanic(String),
    Inner(String),
    NoActionSender,
    NoStreamReceiver,
    Unhealthy,
    WebSocket(anyhow::Error),
    Unimplemented,
    Reconnect(anyhow::Error),
    Unknown(anyhow::Error),
}

impl fmt::Display for StreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => write!(f, "cancelled"),
            Self::SendFull => write!(f, "send failed: full"),
            Self::SendClosed => write!(f, "send failed: closed"),
            Self::Timeout => write!(f, "stop timeout"),
            Self::JoinPanic(s) => write!(f, "stream panicked: {s}"),
            Self::Inner(s) => write!(f, "stream inner error: {s}"),
            Self::NoActionSender => write!(f, "action_tx is None"),
            Self::NoStreamReceiver => write!(f, "stream_rx is None"),
            Self::Unhealthy => write!(f, "yellowstone_grpc unhealthy"),
            Self::Unimplemented => write!(f, "unimplemented"),
            Self::WebSocket(s) => write!(f, "websocket error: {s}"),
            Self::Unknown(err) => write!(f, "unknown error: {err}"),
            Self::Reconnect(err) => write!(f, "reconnect error: {err}"),
        }
    }
}

impl Error for StreamError {}

impl From<anyhow::Error> for StreamError {
    fn from(err: anyhow::Error) -> Self {
        StreamError::Unknown(err)
    }
}
