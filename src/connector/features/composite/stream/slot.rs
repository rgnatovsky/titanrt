use crate::connector::features::composite::stream::{
    StreamWrapper,
    event::{StreamEventContext, StreamEventParsed},
    spec::StreamSpec,
};

#[derive(Debug, Clone)]
pub struct StreamStatus {
    pub enabled: bool,
    pub alive: bool,
    pub last_error: Option<String>,
}

pub struct StreamSlot<E: StreamEventParsed> {
    pub(crate) spec: StreamSpec,
    pub(crate) stream: Option<StreamWrapper<E>>,
    pub(crate) ctx: StreamEventContext<E>,
    pub(crate) enabled: bool,
    pub(crate) last_error: Option<String>,
}

impl<E: StreamEventParsed> StreamSlot<E> {
    pub fn new(spec: StreamSpec, payload: StreamEventContext<E>) -> Self {
        let enabled = !spec.ignore_init.unwrap_or(false);
        Self {
            spec,
            stream: None,
            ctx: payload,
            enabled,
            last_error: None,
        }
    }
    pub fn spec(&self) -> &StreamSpec {
        &self.spec
    }

    pub fn stream_mut(&mut self) -> Option<&mut StreamWrapper<E>> {
        self.stream.as_mut()
    }

    pub fn cancel(&mut self) {
        if let Some(stream) = self.stream.as_mut() {
            stream.cancel();
        }
    }

    pub fn status(&self) -> StreamStatus {
        StreamStatus {
            enabled: self.enabled,
            alive: self.stream.as_ref().map(|s| s.is_alive()).unwrap_or(false),
            last_error: self.last_error.clone(),
        }
    }

}
