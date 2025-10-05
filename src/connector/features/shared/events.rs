use serde_json::Value;
use std::borrow::Cow;
use uuid::Uuid;

use crate::utils::time::timestamp::now_micros;

/// Интерфейс "внутреннего" события стрима.
/// Возвращаем ссылки, чтобы избежать ненужного клонирования.

pub trait StreamEventInner {
    type Body;
    type Err;
    type Code;

    fn status(&self) -> Option<&Self::Code>;
    fn is_ok(&self) -> bool;
    fn error(&self) -> Option<&Self::Err>;
    fn body(&self) -> Option<&Self::Body>;
    fn into_body(self) -> Option<Self::Body>;
}

#[derive(Debug)]
pub struct NoInner;

impl StreamEventInner for NoInner {
    type Body = ();
    type Err = ();
    type Code = ();

    fn status(&self) -> Option<&Self::Code> {
        None
    }

    fn is_ok(&self) -> bool {
        true
    }
    fn error(&self) -> Option<&Self::Err> {
        None
    }
    fn body(&self) -> Option<&Self::Body> {
        None
    }

    fn into_body(self) -> Option<Self::Body> {
        None
    }
}

/// Событие стрима — приватные поля, геттеры и билдер.
/// Поля не навязывают Send/Sync — добавляй bounds там, где требуется.
#[derive(Debug)]
pub struct StreamEventRaw<Inner>
where
    Inner: StreamEventInner,
{
    inner: Inner,
    conn_id: Option<usize>,
    req_id: Option<Uuid>,
    label: Option<Cow<'static, str>>,
    payload: Option<Value>,
    timestamp_us: u64,
}

impl<Inner> StreamEventRaw<Inner>
where
    Inner: StreamEventInner,
{
    /// Создать из inner (owned)
    pub fn new(inner: Inner) -> Self {
        Self {
            inner,
            conn_id: None,
            req_id: None,
            label: None,
            payload: None,
            timestamp_us: now_micros(),
        }
    }

    /// Создать билдер
    pub fn builder(inner: Option<Inner>) -> StreamEventBuilder<Inner> {
        StreamEventBuilder::new(inner)
    }

    /// Ссылка на inner
    pub fn inner(&self) -> &Inner {
        &self.inner
    }

    /// Забрать inner по ownership
    pub fn into_inner(self) -> Inner {
        self.inner
    }

    /// Дешёвый доступ к метаданным
    pub fn conn_id(&self) -> Option<usize> {
        self.conn_id
    }

    pub fn req_id(&self) -> Option<Uuid> {
        self.req_id
    }

    pub fn label(&self) -> Option<&str> {
        self.label.as_deref()
    }

    pub fn payload(&self) -> Option<&Value> {
        self.payload.as_ref()
    }

    /// Проксирующие методы к inner (возвращают ссылки)
    pub fn status(&self) -> Option<&<Inner as StreamEventInner>::Code> {
        self.inner.status()
    }

    pub fn error(&self) -> Option<&<Inner as StreamEventInner>::Err> {
        self.inner.error()
    }

    pub fn body(&self) -> Option<&<Inner as StreamEventInner>::Body> {
        self.inner.body()
    }

    /// Забрать всё сразу (useful when moving out)
    pub fn into_parts(
        self,
    ) -> (
        Inner,
        Option<usize>,
        Option<Uuid>,
        Option<Cow<'static, str>>,
        Option<Value>,
    ) {
        (
            self.inner,
            self.conn_id,
            self.req_id,
            self.label,
            self.payload,
        )
    }
}

/// Билдер для удобного составления StreamEvent
#[derive(Debug)]
pub struct StreamEventBuilder<Inner>
where
    Inner: StreamEventInner,
{
    inner: Option<Inner>,
    conn_id: Option<usize>,
    req_id: Option<Uuid>,
    label: Option<Cow<'static, str>>,
    payload: Option<Value>,
}

impl<Inner> StreamEventBuilder<Inner>
where
    Inner: StreamEventInner,
{
    pub fn new(inner: Option<Inner>) -> Self {
        Self {
            inner,
            conn_id: None,
            req_id: None,
            label: None,
            payload: None,
        }
    }

    pub fn conn_id(mut self, id: Option<usize>) -> Self {
        self.conn_id = id;

        self
    }

    pub fn req_id(mut self, id: Option<Uuid>) -> Self {
        self.req_id = id;
        self
    }

    /// Принимает &'static str или String
    pub fn label<S>(mut self, s: Option<S>) -> Self
    where
        S: Into<Cow<'static, str>>,
    {
        self.label = s.map(Into::into);
        self
    }

    pub fn payload(mut self, p: Option<Value>) -> Self {
        self.payload = p;
        self
    }

    pub fn inner(mut self, inner: Inner) -> Self {
        self.inner = Some(inner);
        self
    }

    pub fn build(self) -> anyhow::Result<StreamEventRaw<Inner>> {
        if self.inner.is_none() {
            return Err(anyhow::anyhow!("StreamEvent Inner is None"));
        }

        Ok(StreamEventRaw {
            inner: self.inner.unwrap(),
            conn_id: self.conn_id,
            req_id: self.req_id,
            label: self.label,
            payload: self.payload,
            timestamp_us: now_micros(),
        })
    }
}

/// Условный Clone: только если Inner: Clone
impl<Inner> Clone for StreamEventRaw<Inner>
where
    Inner: StreamEventInner + Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            conn_id: self.conn_id,
            req_id: self.req_id,
            label: self.label.clone(),
            payload: self.payload.clone(),
            timestamp_us: self.timestamp_us,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[derive(Debug, Clone)]
    struct MyInner {
        code: Option<u16>,
        err: Option<String>,
        body: Option<String>,
    }

    impl StreamEventInner for MyInner {
        type Body = String;
        type Err = String;
        type Code = u16;

        fn status(&self) -> Option<&Self::Code> {
            self.code.as_ref()
        }
        fn is_ok(&self) -> bool {
            self.code.is_none()
        }
        fn error(&self) -> Option<&Self::Err> {
            self.err.as_ref()
        }
        fn body(&self) -> Option<&Self::Body> {
            self.body.as_ref()
        }
        fn into_body(self) -> Option<Self::Body> {
            self.body
        }
    }

    #[test]
    fn build_and_access_event() {
        let inner = MyInner {
            code: Some(200),
            err: None,
            body: Some("ok".to_string()),
        };

        let req_id = Uuid::new_v4();

        let ev = StreamEventRaw::builder(Some(inner))
            .conn_id(Some(7))
            .req_id(Some(req_id))
            .label(Some("stream:event"))
            .payload(Some(json!({"k": "v"})))
            .build()
            .unwrap();

        assert_eq!(ev.conn_id(), Some(7));
        assert_eq!(ev.req_id(), Some(req_id));
        assert_eq!(ev.status().map(|c| *c), Some(200));
        assert_eq!(ev.body().map(|s| s.as_str()), Some("ok"));
        assert_eq!(
            ev.payload()
                .and_then(|v| v.get("k"))
                .and_then(|v| v.as_str()),
            Some("v")
        );
    }

    #[test]
    fn clone_when_inner_clone() {
        let inner = MyInner {
            code: Some(201),
            err: None,
            body: Some("ok2".to_string()),
        };

        let ev = StreamEventRaw::builder(Some(inner))
            .label(Some("l"))
            .build()
            .unwrap();

        let ev2 = ev.clone();

        assert_eq!(ev2.status().map(|c| *c), Some(201));
    }

    #[test]
    fn build_no_inner() {
        let ev = StreamEventRaw::<NoInner>::builder(None)
            .build()
            .unwrap_err();
        assert_eq!(ev.to_string(), "inner is None");
    }
}
