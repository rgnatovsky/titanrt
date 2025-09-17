use bytes::Bytes;
use serde_json::Value;
use tonic::metadata::MetadataMap;
use tonic::{Code, Status};
use uuid::Uuid;

/// Универсальное событие, которое выходит наружу в Hook.
#[derive(Debug, Clone)]
pub struct GrpcEvent {
    code: Option<Code>,     // код ошибки gRPC
    err_msg: Option<String>,
    metadata: MetadataMap,  // метаданные ответа
    body: Option<Bytes>,    // тело (если есть)
    is_stream_item: bool,   // true = элемент стрима, false = финальный ответ
    req_id: Option<Uuid>,
    label: Option<&'static str>,
    payload: Option<Value>, // полезная нагрузка (для логов)
}

impl GrpcEvent {
    /// from_ok_unary — успешный unary/финальный ответ стрима.
    pub fn from_ok_unary(
        body: Bytes,
        metadata: MetadataMap,
        req_id: Option<Uuid>,
        label: Option<&'static str>,
        payload: Option<Value>,
    ) -> Self {
        Self {
            code: None,
            err_msg: None,
            metadata,
            body: Some(body),
            is_stream_item: false,
            req_id,
            label,
            payload,
        }
    }
    /// from_ok_stream_item — элемент стрима.
    pub fn from_ok_stream_item(
        body: Bytes,
        req_id: Option<Uuid>,
        label: Option<&'static str>,
        payload: Option<Value>,
    ) -> Self {
        Self {
            code: None,
            err_msg: None,
            metadata: MetadataMap::new(),
            body: Some(body),
            is_stream_item: true,
            req_id,
            label,
            payload,
        }
    }
    /// from_status — ошибка (Status).
    pub fn from_status(
        st: Status,
        req_id: Option<Uuid>,
        label: Option<&'static str>,
        payload: Option<Value>,
    ) -> Self {
        Self {
            code: Some(st.code()),
            err_msg: Some(st.message().to_string()),
            metadata: MetadataMap::new(),
            body: None,
            is_stream_item: false,
            req_id,
            label,
            payload,
        }
    }

    pub fn payload(&self) -> Option<&Value> {
        self.payload.as_ref()
    }
    pub fn req_id(&self) -> Option<&Uuid> {
        self.req_id.as_ref()
    }
    pub fn label(&self) -> Option<&'static str> {
        self.label
    }
    pub fn is_stream_item(&self) -> bool {
        self.is_stream_item
    }
    pub fn metadata(&self) -> &MetadataMap {
        &self.metadata
    }
    pub fn body_bytes(&self) -> Option<&Bytes> {
        self.body.as_ref()
    }

    pub fn is_ok(&self) -> bool {
        self.code.is_none()
    }
    pub fn maybe_error_msg(&self) -> Option<String> {
        self.code.map(|c| {
            format!(
                "gRPC status: {:?}, {}",
                c,
                self.err_msg.clone().unwrap_or_default()
            )
        })
    }
}
