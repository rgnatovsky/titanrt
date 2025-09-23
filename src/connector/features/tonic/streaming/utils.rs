use crate::connector::features::shared::events::StreamEvent;
use crate::connector::features::tonic::streaming::StreamingMode;
use crate::connector::features::tonic::streaming::event::StreamingEvent;

use bytes::Bytes;
use crossbeam::channel::Sender;
use futures::Stream;
use serde_json::Value;
use std::borrow::Cow;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use uuid::Uuid;

pub struct ActiveStream {
    pub mode: StreamingMode,
    pub sender: Option<mpsc::Sender<Bytes>>,
    pub handle: tokio::task::JoinHandle<()>,
}

impl ActiveStream {
    pub fn stop(mut self) {
        if let Some(sender) = self.sender.take() {
            drop(sender);
        }
        self.handle.abort();
    }
}

pub enum StreamLifecycle {
    Closed { stream_name: String },
}

#[derive(Clone)]
pub struct StreamContext {
    pub req_id: Option<Uuid>,
    pub name: String,
    pub payload: Option<Value>,
}

pub fn emit_event(
    sender: &Sender<StreamEvent<StreamingEvent>>,
    conn_id: Option<usize>,
    req_id: Option<Uuid>,
    label: Option<Cow<'static, str>>,
    payload: Option<&Value>,
    inner: StreamingEvent,
) {
    let mut builder = StreamEvent::builder(Some(inner))
        .conn_id(conn_id)
        .req_id(req_id)
        .label(label);

    if let Some(payload) = payload {
        builder = builder.payload(Some(payload.clone()));
    }

    if let Ok(event) = builder.build() {
        let _ = sender.try_send(event);
    }
}
pub struct MpscBytesStream {
    inner: mpsc::Receiver<Bytes>,
}

impl MpscBytesStream {
    pub fn new(inner: mpsc::Receiver<Bytes>) -> Self {
        Self { inner }
    }
}

impl Stream for MpscBytesStream {
    type Item = Bytes;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.inner.poll_recv(cx)
    }
}
