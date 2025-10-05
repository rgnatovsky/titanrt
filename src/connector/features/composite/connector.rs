use std::ops::{Deref, DerefMut};
use std::vec::Vec;

use anyhow::Result;
use parking_lot::{Mutex, MutexGuard, RwLock};
use serde::{Deserialize, Serialize};

use crate::connector::BaseConnector;
use crate::utils::CancelToken;

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
}

pub struct CompositeConnector {
    cancel_token: CancelToken,
    reserved_core_ids: Option<Vec<usize>>,
    #[cfg(feature = "ws_conn")]
    websocket: ConnectorSlot<WebSocketConnector>,
    #[cfg(feature = "http_conn")]
    http: ConnectorSlot<HttpConnector>,
    #[cfg(feature = "grpc_conn")]
    grpc: ConnectorSlot<GrpcConnector>,
}

impl CompositeConnector {
    pub fn new(
        config: CompositeConnectorConfig,
        cancel_token: CancelToken,
        reserved_core_ids: Option<Vec<usize>>,
    ) -> Self {
        Self {
            #[cfg(feature = "ws_conn")]
            websocket: ConnectorSlot::new(config.websocket, config.cancel_streams_policy),
            #[cfg(feature = "http_conn")]
            http: ConnectorSlot::new(config.http, config.cancel_streams_policy),
            #[cfg(feature = "grpc_conn")]
            grpc: ConnectorSlot::new(config.grpc, config.cancel_streams_policy),
            cancel_token,
            reserved_core_ids,
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
    pub fn with_reqwest<F, R>(&self, f: F) -> Result<Option<R>>
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
    pub fn with_tonic<F, R>(&self, f: F) -> Result<Option<R>>
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
}

struct ConnectorSlot<C>
where
    C: BaseConnector,
{
    config: RwLock<Option<C::MainConfig>>,
    instance: Mutex<Option<C>>,
    cancel_streams: CancelStreamsPolicy,
}

impl<C> ConnectorSlot<C>
where
    C: BaseConnector,
{
    fn new(config: Option<C::MainConfig>, cancel_streams: CancelStreamsPolicy) -> Self {
        Self {
            config: RwLock::new(config),
            instance: Mutex::new(None),
            cancel_streams,
        }
    }

    fn update_config(&self, config: Option<C::MainConfig>) {
        {
            let mut guard = self.config.write();
            *guard = config;
        }
        self.drop_instance();
    }

    fn unload(&self) {
        self.drop_instance();
    }

    fn drop_instance(&self) {
        let mut guard = self.instance.lock();
        match self.cancel_streams {
            CancelStreamsPolicy::OnInstanceDrop => {
                if let Some(connector) = guard.as_mut() {
                    connector.cancel_token().cancel();
                }
            }
            CancelStreamsPolicy::Ignore => {}
        }
        *guard = None;
    }

    fn config_snapshot(&self) -> Option<C::MainConfig> {
        self.config.read().clone()
    }

    fn ensure(
        &self,
        cancel_token: &CancelToken,
        reserved_core_ids: &Option<Vec<usize>>,
    ) -> Result<Option<ConnectorGuard<'_, C>>> {
        let config = { self.config.read().clone() };

        let Some(config) = config else {
            return Ok(None);
        };

        if let Some(guard) = self.try_get_existing() {
            return Ok(Some(guard));
        }

        let connector = C::init(config, cancel_token.clone(), reserved_core_ids.clone())?;
        let mut guard = self.instance.lock();
        if guard.is_none() {
            *guard = Some(connector);
            return Ok(Some(ConnectorGuard::new(guard)));
        }

        drop(connector);
        Ok(Some(ConnectorGuard::new(guard)))
    }

    fn with<F, R>(
        &self,
        cancel_token: &CancelToken,
        reserved_core_ids: &Option<Vec<usize>>,
        f: F,
    ) -> Result<Option<R>>
    where
        F: FnOnce(&mut C) -> Result<R>,
    {
        match self.ensure(cancel_token, reserved_core_ids)? {
            Some(mut guard) => {
                let out = f(&mut guard)?;
                Ok(Some(out))
            }
            None => Ok(None),
        }
    }

    fn try_get_existing(&self) -> Option<ConnectorGuard<'_, C>> {
        let guard = self.instance.lock();
        if guard.is_some() {
            Some(ConnectorGuard::new(guard))
        } else {
            None
        }
    }
}

impl<C> Drop for ConnectorSlot<C>
where
    C: BaseConnector,
{
    fn drop(&mut self) {
        match self.cancel_streams {
            CancelStreamsPolicy::OnInstanceDrop => {
                if let Some(connector) = self.instance.get_mut().take() {
                    connector.cancel_token().cancel();
                }
            }
            CancelStreamsPolicy::Ignore => return,
        }
    }
}

pub struct ConnectorGuard<'a, C>
where
    C: BaseConnector,
{
    guard: MutexGuard<'a, Option<C>>,
}

impl<'a, C> ConnectorGuard<'a, C>
where
    C: BaseConnector,
{
    fn new(guard: MutexGuard<'a, Option<C>>) -> Self {
        debug_assert!(guard.is_some(), "Connector guard must hold a value");
        Self { guard }
    }
}

impl<'a, C> Deref for ConnectorGuard<'a, C>
where
    C: BaseConnector,
{
    type Target = C;

    fn deref(&self) -> &Self::Target {
        self.guard.as_ref().expect("Connector is uninitialized")
    }
}

impl<'a, C> DerefMut for ConnectorGuard<'a, C>
where
    C: BaseConnector,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.as_mut().expect("Connector is uninitialized")
    }
}
