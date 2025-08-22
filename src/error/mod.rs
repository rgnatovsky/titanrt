// error.rs
use std::{error::Error, fmt};

const ERR_MSG_QUEUE_FULL: &str = "action queue is full";
const ERR_MSG_TRANSPORT_CLOSED: &str = "action transport is closed";
const ERR_MSG_TIMEOUT: &str = "operation timed out";
const ERR_MSG_DISCONNECTED: &str = "connection disconnected";
const ERR_MSG_CANCELLED: &str = "operation cancelled";

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum SendFailReason {
    Timeout,
    Cancelled,
    Full,   // очередь заполнена
    Closed, // транспорт закрыт/поломан
}

impl fmt::Display for SendFailReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendFailReason::Timeout => write!(f, "{ERR_MSG_TIMEOUT}"),
            SendFailReason::Cancelled => write!(f, "{ERR_MSG_CANCELLED}"),
            SendFailReason::Full => write!(f, "{ERR_MSG_QUEUE_FULL}"),
            SendFailReason::Closed => write!(f, "{ERR_MSG_TRANSPORT_CLOSED}"),
        }
    }
}

#[derive(Debug)]
pub struct SendError<Action> {
    pub value: Option<Action>,
    pub reason: SendFailReason,
}

impl<Action> SendError<Action> {
    pub fn full(value: Option<Action>) -> Self {
        Self {
            value,
            reason: SendFailReason::Full,
        }
    }

    pub fn closed(value: Option<Action>) -> Self {
        Self {
            value,
            reason: SendFailReason::Closed,
        }
    }

    pub fn cancelled(value: Option<Action>) -> Self {
        Self {
            value,
            reason: SendFailReason::Cancelled,
        }
    }

    pub fn timeout(value: Option<Action>) -> Self {
        Self {
            value,
            reason: SendFailReason::Timeout,
        }
    }
}

impl<Action> fmt::Display for SendError<Action> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.reason {
            SendFailReason::Full => write!(f, "{ERR_MSG_QUEUE_FULL}"),
            SendFailReason::Closed => write!(f, "{ERR_MSG_TRANSPORT_CLOSED}"),
            SendFailReason::Timeout => write!(f, "{ERR_MSG_TIMEOUT}"),
            SendFailReason::Cancelled => write!(f, "{ERR_MSG_CANCELLED}"),
        }
    }
}

impl<Action: fmt::Debug> Error for SendError<Action> {}

#[derive(Debug)]
pub enum TryRecvError {
    Empty,
    Disconnected,
}

#[derive(Debug)]
pub enum RecvError {
    Timeout,
    Disconnected,
    Cancelled,
    Absent,
    Unknown(anyhow::Error),
}

impl Error for RecvError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            RecvError::Unknown(err) => Some(err.as_ref()),
            _ => None,
        }
    }
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecvError::Timeout => write!(f, "{ERR_MSG_TIMEOUT}"),
            RecvError::Disconnected => write!(f, "{ERR_MSG_DISCONNECTED}"),
            RecvError::Cancelled => write!(f, "{ERR_MSG_CANCELLED}"),
            RecvError::Absent => write!(f, "RX is absent"),
            RecvError::Unknown(err) => write!(f, "unknown error: {err}"),
        }
    }
}

impl From<anyhow::Error> for RecvError {
    fn from(err: anyhow::Error) -> Self {
        RecvError::Unknown(err)
    }
}
