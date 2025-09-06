use anyhow::{anyhow, Result};
use bytes::Bytes;
use reqwest::header::HeaderMap;
use reqwest::{Response, StatusCode};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug)]
pub struct ReqwestEvent {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Option<Bytes>,
    pub req_id: Option<Uuid>,
    pub label: Option<&'static str>,
    pub payload: Option<Value>
}

impl ReqwestEvent {
    /// Creates TcpResponse from hyper Response.
    /// Consumes the hyper response and collects its body.

    pub async fn from_raw(
        resp: Response,
        req_id: Option<Uuid>,
        label: Option<&'static str>,
        payload: Option<Value>
    ) -> Self {
        let status = resp.status();
        let headers = resp.headers().clone();

        let body = match resp.bytes().await {
            Ok(bytes) if !bytes.is_empty() => Some(bytes),
            Ok(_) => None,
            Err(e) => {
                let body = Some(Bytes::from(e.to_string()));
                return Self {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    headers,
                    body,
                    req_id,
                    label,
                    payload
                };
            }
        };

        Self {
            req_id,
            status,
            headers,
            body,
            label,
            payload
        }
    }

    pub fn from_error(
        error: reqwest::Error,
        req_id: Option<Uuid>,
        label: Option<&'static str>,
        payload: Option<Value>
    ) -> Self {
        let status = StatusCode::INTERNAL_SERVER_ERROR;
        let headers = HeaderMap::new();
        let body = Some(Bytes::from(error.to_string()));
        Self {
            req_id,
            status,
            headers,
            body,
            label,
            payload
        }
    }
    /// Returns payload of the request
    pub fn payload(&self) -> Option<&Value> {
        self.payload.as_ref()
    }
    /// Returns request id
    pub fn req_id(&self) -> Option<&Uuid> {
        self.req_id.as_ref()
    }
    /// Returns label of the request
    pub fn label(&self) -> Option<&'static str> {
        self.label
    }
    /// Returns HTTP status code of the response.
    pub fn status(&self) -> &StatusCode {
        &self.status
    }

    /// Returns reference to response headers.
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Returns reference to raw body bytes if present.
    pub fn body_bytes(&self) -> Option<&Bytes> {
        self.body.as_ref()
    }

    /// Returns body as Vec<u8> if present (copies data).
    pub fn body_vec(&self) -> Option<Vec<u8>> {
        self.body.as_ref().map(|b| b.to_vec())
    }

    /// Returns body as UTF-8 string if present and valid.
    /// If the body is None, returns an error.
    pub fn body_as_str(&self) -> Result<&str> {
        if let Some(body) = self.body.as_ref() {
            Ok(std::str::from_utf8(body)?)
        } else {
            Err(anyhow!("Tcp Response has no body"))
        }
    }

    /// Parses body as JSON of type T, consuming the body.
    /// If the body is not valid JSON or is None, returns an error.
    pub fn consume_body_as_json<T: serde::de::DeserializeOwned>(&mut self) -> Result<T> {
        if let Some(body) = self.body.take() {
            Ok(serde_json::from_slice(&body)?)
        } else {
            Err(anyhow!("Tcp Response has no body"))
        }
    }

    /// Tries to parse the body as JSON without consuming it
    pub fn peek_body_as_json<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
        self.body_bytes()
            .ok_or(anyhow!("Tcp Response has no body"))
            .and_then(|b| serde_json::from_slice(b).map_err(|e| e.into()))
    }

    /// Parses body as JSON Value, consuming the body.
    /// If the body is not valid JSON or is None, returns an error.
    pub fn consume_body_as_json_value(&mut self) -> Result<serde_json::Value> {
        self.consume_body_as_json()
    }

    /// Takes ownership of the body bytes if present.
    pub fn take_body(&mut self) -> Option<Bytes> {
        self.body.take()
    }

    /// Returns true if status code is 2xx (success).
    pub fn is_success(&self) -> bool {
        self.status.is_success()
    }

    /// Returns true if status code is 4xx (client error).
    pub fn is_client_error(&self) -> bool {
        self.status.is_client_error()
    }

    /// Returns true if status code is 5xx (server error).
    pub fn is_server_error(&self) -> bool {
        self.status.is_server_error()
    }

    /// This function returns an error message as a string if the HTTP status code indicates a client or server error.
    /// If there is no error, it returns None.
    /// The error message includes the status code and the response body, if available.
    pub fn maybe_error_msg(&self) -> Option<String> {
        if self.status.is_client_error() || self.status.is_server_error() {
            let msg = format!(
                "Request failed with status code: {} ({}), {}",
                self.status,
                self.status.canonical_reason().unwrap_or("Unknown reason"),
                self.body_as_str().unwrap_or("No response body")
            );
            return Some(msg);
        }

        None
    }
}
