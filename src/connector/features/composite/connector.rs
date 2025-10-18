use std::collections::HashMap;
use std::time::{Duration, Instant};
use std::vec::Vec;

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

#[cfg(feature = "ws_conn")]
use super::inner::ConnectorInner;
#[cfg(feature = "ws_conn")]
use crate::connector::features::composite::ConnectorGuard;
use crate::connector::features::composite::stream::event::{StreamEvent, StreamEventParsed};
use crate::connector::features::composite::stream::{
    CompositeAction, StreamSlot, StreamStatus, StreamWrapper,
};
use crate::connector::features::grpc::stream::GrpcCommand;
use crate::connector::features::http::stream::actions::HttpAction;
use crate::connector::features::shared::actions::StreamActionRaw;
use crate::connector::features::websocket::stream::WebSocketCommand;
use crate::io::mpmc::MpmcSender;
use crate::io::ringbuffer::RingSender;
use crate::utils::encoder::{EncodableRequest, EncoderRegistry};
use crate::utils::time::Timeframe;
use crate::utils::{CancelToken, SharedStr, StateCell};

#[cfg(feature = "grpc_conn")]
use crate::connector::features::grpc::connector::{GrpcConnector, GrpcConnectorConfig};
#[cfg(feature = "http_conn")]
use crate::connector::features::http::connector::{HttpConnector, HttpConnectorConfig};
#[cfg(feature = "ws_conn")]
use crate::connector::features::websocket::connector::{
    WebSocketConnector, WebSocketConnectorConfig,
};

#[derive(Debug, Clone, Default, Deserialize, Serialize, Copy, Eq, PartialEq)]
pub enum CancelStreamsPolicy {
    #[default]
    Ignore,
    OnInstanceDrop,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct CompositeConnectorConfig {
    #[cfg(feature = "ws_conn")]
    pub websocket: Option<WebSocketConnectorConfig>,
    #[cfg(feature = "http_conn")]
    pub http: Option<HttpConnectorConfig>,
    #[cfg(feature = "grpc_conn")]
    pub grpc: Option<GrpcConnectorConfig>,
    #[serde(default)]
    pub cancel_streams_policy: CancelStreamsPolicy,
    #[serde(default)]
    pub max_streams: Option<usize>,
    #[serde(default)]
    pub ensure_interval: Option<Timeframe>,
}

pub struct CompositeConnector<E: StreamEventParsed, A: EncodableRequest> {
    cancel_token: CancelToken,
    reserved_core_ids: Option<Vec<usize>>,
    slots: HashMap<SharedStr, StreamSlot<E>>,
    pub(crate) encoders: EncoderRegistry<A, CompositeAction>,
    pub(crate) event_tx: MpmcSender<StreamEvent<E>>,
    ensure_interval: Option<Duration>,
    max_streams: usize,
    last_ensure: Instant,
    #[cfg(feature = "ws_conn")]
    websocket: ConnectorInner<WebSocketConnector>,
    #[cfg(feature = "http_conn")]
    http: ConnectorInner<HttpConnector>,
    #[cfg(feature = "grpc_conn")]
    grpc: ConnectorInner<GrpcConnector>,
}

impl<E: StreamEventParsed, A: EncodableRequest> CompositeConnector<E, A> {
    pub fn new(
        config: CompositeConnectorConfig,
        action_pipelines: EncoderRegistry<A, CompositeAction>,
        cancel_token: CancelToken,
        reserved_core_ids: Option<Vec<usize>>,
        event_tx: MpmcSender<StreamEvent<E>>,
    ) -> Self {
        Self {
            #[cfg(feature = "ws_conn")]
            websocket: ConnectorInner::new(config.websocket, config.cancel_streams_policy),
            #[cfg(feature = "http_conn")]
            http: ConnectorInner::new(config.http, config.cancel_streams_policy),
            #[cfg(feature = "grpc_conn")]
            grpc: ConnectorInner::new(config.grpc, config.cancel_streams_policy),
            encoders: action_pipelines,
            cancel_token,
            reserved_core_ids,
            event_tx,
            slots: HashMap::new(),
            ensure_interval: config.ensure_interval.map(|tf| tf.duration()),
            last_ensure: Instant::now(),
            max_streams: config.max_streams.unwrap_or(usize::MAX),
        }
    }

    #[cfg(feature = "ws_conn")]
    pub fn websocket(&self) -> Result<Option<ConnectorGuard<'_, WebSocketConnector>>> {
        self.websocket
            .ensure(&self.cancel_token, &self.reserved_core_ids)
    }

    #[cfg(feature = "ws_conn")]
    pub fn with_websocket<F, R>(&self, f: F) -> Result<Option<R>>
    where
        F: FnOnce(&mut WebSocketConnector) -> Result<R>,
    {
        self.websocket
            .with(&self.cancel_token, &self.reserved_core_ids, f)
    }

    #[cfg(feature = "http_conn")]
    pub fn http(&self) -> Result<Option<ConnectorGuard<'_, HttpConnector>>> {
        self.http
            .ensure(&self.cancel_token, &self.reserved_core_ids)
    }

    #[cfg(feature = "http_conn")]
    pub fn with_http<F, R>(&self, f: F) -> Result<Option<R>>
    where
        F: FnOnce(&mut HttpConnector) -> Result<R>,
    {
        self.http
            .with(&self.cancel_token, &self.reserved_core_ids, f)
    }

    #[cfg(feature = "grpc_conn")]
    pub fn grpc(&self) -> Result<Option<ConnectorGuard<'_, GrpcConnector>>> {
        self.grpc
            .ensure(&self.cancel_token, &self.reserved_core_ids)
    }

    #[cfg(feature = "grpc_conn")]
    pub fn with_grpc<F, R>(&self, f: F) -> Result<Option<R>>
    where
        F: FnOnce(&mut GrpcConnector) -> Result<R>,
    {
        self.grpc
            .with(&self.cancel_token, &self.reserved_core_ids, f)
    }

    #[cfg(feature = "ws_conn")]
    pub fn configure_websocket(&self, config: Option<WebSocketConnectorConfig>) {
        self.websocket.update_config(config);
    }

    #[cfg(feature = "http_conn")]
    pub fn configure_reqwest(&self, config: Option<HttpConnectorConfig>) {
        self.http.update_config(config);
    }

    #[cfg(feature = "grpc_conn")]
    pub fn configure_tonic(&self, config: Option<GrpcConnectorConfig>) {
        self.grpc.update_config(config);
    }

    #[cfg(feature = "ws_conn")]
    pub fn unload_websocket(&self) {
        self.websocket.unload();
    }

    #[cfg(feature = "http_conn")]
    pub fn unload_reqwest(&self) {
        self.http.unload();
    }

    #[cfg(feature = "grpc_conn")]
    pub fn unload_tonic(&self) {
        self.grpc.unload();
    }

    #[cfg(feature = "ws_conn")]
    pub fn websocket_config(&self) -> Option<WebSocketConnectorConfig> {
        self.websocket.config_snapshot()
    }

    #[cfg(feature = "http_conn")]
    pub fn reqwest_config(&self) -> Option<HttpConnectorConfig> {
        self.http.config_snapshot()
    }

    #[cfg(feature = "grpc_conn")]
    pub fn tonic_config(&self) -> Option<GrpcConnectorConfig> {
        self.grpc.config_snapshot()
    }

    pub fn cancel_token(&self) -> CancelToken {
        self.cancel_token.clone()
    }

    pub fn reserved_core_ids(&self) -> Option<Vec<usize>> {
        self.reserved_core_ids.clone()
    }

    pub fn http_sender_mut(
        &mut self,
        stream: impl AsRef<str>,
    ) -> Option<&mut RingSender<StreamActionRaw<HttpAction>>> {
        self.stream_mut(stream)
            .and_then(|s| s.get_http())
            .map(|s| s.action_tx_mut())
    }

    pub fn grpc_sender_mut(
        &mut self,
        stream: impl AsRef<str>,
    ) -> Option<&mut RingSender<StreamActionRaw<GrpcCommand>>> {
        self.stream_mut(stream)
            .and_then(|s| s.get_grpc())
            .map(|s| s.action_tx_mut())
    }

    pub fn ws_sender_mut(
        &mut self,
        stream: impl AsRef<str>,
    ) -> Option<&mut RingSender<StreamActionRaw<WebSocketCommand>>> {
        self.stream_mut(stream)
            .and_then(|s| s.get_ws())
            .map(|s| s.action_tx_mut())
    }

    pub fn event_tx(&self) -> &MpmcSender<StreamEvent<E>> {
        &self.event_tx
    }

    pub fn event_tx_mut(&mut self) -> &mut MpmcSender<StreamEvent<E>> {
        &mut self.event_tx
    }

    /// Creates new stream slot
    /// Returns `Err` if the maximum number of streams is exceeded
    /// Returns `Ok(None)` if the stream was successfully created
    /// Returns `Ok(Some(old_slot))` if the stream was replaced
    pub fn new_slot(&mut self, slot: StreamSlot<E>) -> anyhow::Result<Option<StreamSlot<E>>> {
        if self.slots.len() < self.max_streams {
            let stream_name = slot.spec().name.clone();

            let old_slot = self.slots.insert(stream_name.clone(), slot);

            return Ok(old_slot);
        } else {
            return Err(anyhow!("too many streams"));
        }
    }

    /// Returns status of the stream
    /// Returns `Err` if the stream is not found
    /// Returns `None` if the stream is alive
    pub fn check_stream(&self, name: impl AsRef<str>) -> anyhow::Result<Option<StreamStatus>> {
        let slot = self
            .slots
            .get(name.as_ref())
            .ok_or_else(|| anyhow!("unknown stream {}", name.as_ref()))?;

        // Если стрим отключен - возвращаем статус, не запускаем
        if !slot.enabled {
            return Ok(Some(slot.status()));
        }

        let needs_restart = match slot.stream.as_ref() {
            Some(stream) if stream.is_alive() => false,
            Some(_) => true,
            None => true,
        };

        if needs_restart {
            return Ok(Some(slot.status()));
        }

        Ok(None)
    }

    /// Ensures that stream is alive and spawn it if needed
    pub fn ensure_stream(
        &mut self,
        name: impl AsRef<str>,
        force_enable: bool,
    ) -> anyhow::Result<()> {
        let name = name.as_ref();
        let status = self.check_stream(name)?;

        if let Some(status) = status {
            if (status.enabled || force_enable) && !status.alive {
                let mut slot = self.slots.remove(name).unwrap();

                slot.enabled = true;

                if let Some(mut old_stream) = slot.stream.take() {
                    old_stream.cancel();
                }

                let stream = self.spawn_stream(&slot.spec, &slot.ctx)?;

                slot.stream = Some(stream);
                slot.last_error = None;

                self.slots.insert(name.into(), slot);
            }
        }

        Ok(())
    }

    /// Ensures that all streams are alive and spawn them if needed
    /// Returns list of errors and stream names
    /// Returns empty list if all streams are alive
    pub fn ensure_all_streams(&mut self, force_enable: bool) -> Vec<(SharedStr, anyhow::Error)> {
        if let Some(ensure_interval) = self.ensure_interval
            && self.last_ensure.elapsed() < ensure_interval
        {
            return Vec::new();
        }

        let mut errors = Vec::new();

        let mut to_restart = Vec::with_capacity(5);

        for name in self.slots.keys() {
            let status = self.check_stream(name).unwrap();

            if let Some(status) = status {
                if !status.alive && (status.enabled || force_enable) {
                    to_restart.push(name.clone());
                }
            }
        }

        for name in to_restart {
            if let Err(err) = self.ensure_stream(&name, force_enable) {
                errors.push((name, err));
            }
        }

        self.last_ensure = Instant::now();

        errors
    }

    /// Returns mutable reference to the stream
    pub fn stream(&self, name: impl AsRef<str>) -> Option<&StreamWrapper<E>> {
        self.slots
            .get(name.as_ref())
            .and_then(|slot| slot.stream.as_ref())
    }

    /// Returns mutable reference to the stream
    pub fn stream_mut(&mut self, name: impl AsRef<str>) -> Option<&mut StreamWrapper<E>> {
        self.slots
            .get_mut(name.as_ref())
            .and_then(|slot| slot.stream_mut())
    }

    /// Returns status of the stream
    /// Returns `None` if the stream is not found
    pub fn stream_status(&self, name: impl AsRef<str>) -> Option<StreamStatus> {
        self.slots.get(name.as_ref()).map(|slot| slot.status())
    }

    /// Cancel all streams
    pub fn cancel_all_streams(&mut self) {
        for slot in self.slots.values_mut() {
            slot.cancel();
        }
    }

    pub fn http_state(&self, stream: impl AsRef<str>) -> Option<&StateCell<E::HttpState>> {
        self.slots
            .get(stream.as_ref())
            .and_then(|slot| slot.stream.as_ref())
            .and_then(|stream| stream.http_state())
    }

    pub fn grpc_state(&self, stream: impl AsRef<str>) -> Option<&StateCell<E::GrpcState>> {
        self.slots
            .get(stream.as_ref())
            .and_then(|slot| slot.stream.as_ref())
            .and_then(|stream| stream.grpc_state())
    }

    pub fn ws_state(&self, stream: impl AsRef<str>) -> Option<&StateCell<E::WsState>> {
        self.slots
            .get(stream.as_ref())
            .and_then(|slot| slot.stream.as_ref())
            .and_then(|stream| stream.ws_state())
    }

    pub fn encoders(&self) -> &EncoderRegistry<A, CompositeAction> {
        &self.encoders
    }

    pub fn encoders_mut(&mut self) -> &mut EncoderRegistry<A, CompositeAction> {
        &mut self.encoders
    }
}
