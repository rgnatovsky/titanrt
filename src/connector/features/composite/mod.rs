use std::ops::{Deref, DerefMut};
use std::vec::Vec;

use anyhow::Result;
use parking_lot::{Mutex, MutexGuard, RwLock};
use serde::{Deserialize, Serialize};

use crate::connector::BaseConnector;
use crate::utils::CancelToken;

#[cfg(feature = "reqwest_conn")]
use crate::connector::features::reqwest::connector::{ReqwestConnector, ReqwestConnectorConfig};
#[cfg(feature = "tonic_conn")]
use crate::connector::features::tonic::connector::{TonicConnector, TonicConnectorConfig};
#[cfg(feature = "websocket")]
use crate::connector::features::websocket::connector::{
    WebSocketConnector, WebSocketConnectorConfig,
};

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct CompositeConnectorConfig {
    #[cfg(feature = "websocket")]
    pub websocket: Option<WebSocketConnectorConfig>,
    #[cfg(feature = "reqwest_conn")]
    pub reqwest: Option<ReqwestConnectorConfig>,
    #[cfg(feature = "tonic_conn")]
    pub tonic: Option<TonicConnectorConfig>,
}

pub struct CompositeConnector {
    cancel_token: CancelToken,
    reserved_core_ids: Option<Vec<usize>>,
    #[cfg(feature = "websocket")]
    websocket: ConnectorSlot<WebSocketConnector>,
    #[cfg(feature = "reqwest_conn")]
    reqwest: ConnectorSlot<ReqwestConnector>,
    #[cfg(feature = "tonic_conn")]
    tonic: ConnectorSlot<TonicConnector>,
}

impl CompositeConnector {
    pub fn new(
        config: CompositeConnectorConfig,
        cancel_token: CancelToken,
        reserved_core_ids: Option<Vec<usize>>,
    ) -> Self {
        Self {
            #[cfg(feature = "websocket")]
            websocket: ConnectorSlot::new(config.websocket),
            #[cfg(feature = "reqwest_conn")]
            reqwest: ConnectorSlot::new(config.reqwest),
            #[cfg(feature = "tonic_conn")]
            tonic: ConnectorSlot::new(config.tonic),
            cancel_token,
            reserved_core_ids,
        }
    }

    #[cfg(feature = "websocket")]
    pub fn websocket(&self) -> Result<Option<ConnectorGuard<'_, WebSocketConnector>>> {
        self.websocket
            .ensure(&self.cancel_token, &self.reserved_core_ids)
    }

    #[cfg(feature = "websocket")]
    pub fn with_websocket<F, R>(&self, f: F) -> Result<Option<R>>
    where
        F: FnOnce(&mut WebSocketConnector) -> Result<R>,
    {
        self.websocket
            .with(&self.cancel_token, &self.reserved_core_ids, f)
    }

    #[cfg(feature = "reqwest_conn")]
    pub fn reqwest(&self) -> Result<Option<ConnectorGuard<'_, ReqwestConnector>>> {
        self.reqwest
            .ensure(&self.cancel_token, &self.reserved_core_ids)
    }

    #[cfg(feature = "reqwest_conn")]
    pub fn with_reqwest<F, R>(&self, f: F) -> Result<Option<R>>
    where
        F: FnOnce(&mut ReqwestConnector) -> Result<R>,
    {
        self.reqwest
            .with(&self.cancel_token, &self.reserved_core_ids, f)
    }

    #[cfg(feature = "tonic_conn")]
    pub fn tonic(&self) -> Result<Option<ConnectorGuard<'_, TonicConnector>>> {
        self.tonic
            .ensure(&self.cancel_token, &self.reserved_core_ids)
    }

    #[cfg(feature = "tonic_conn")]
    pub fn with_tonic<F, R>(&self, f: F) -> Result<Option<R>>
    where
        F: FnOnce(&mut TonicConnector) -> Result<R>,
    {
        self.tonic
            .with(&self.cancel_token, &self.reserved_core_ids, f)
    }

    #[cfg(feature = "websocket")]
    pub fn configure_websocket(&self, config: Option<WebSocketConnectorConfig>) {
        self.websocket.update_config(config);
    }

    #[cfg(feature = "reqwest_conn")]
    pub fn configure_reqwest(&self, config: Option<ReqwestConnectorConfig>) {
        self.reqwest.update_config(config);
    }

    #[cfg(feature = "tonic_conn")]
    pub fn configure_tonic(&self, config: Option<TonicConnectorConfig>) {
        self.tonic.update_config(config);
    }

    #[cfg(feature = "websocket")]
    pub fn unload_websocket(&self) {
        self.websocket.unload();
    }

    #[cfg(feature = "reqwest_conn")]
    pub fn unload_reqwest(&self) {
        self.reqwest.unload();
    }

    #[cfg(feature = "tonic_conn")]
    pub fn unload_tonic(&self) {
        self.tonic.unload();
    }

    #[cfg(feature = "websocket")]
    pub fn websocket_config(&self) -> Option<WebSocketConnectorConfig> {
        self.websocket.config_snapshot()
    }

    #[cfg(feature = "reqwest_conn")]
    pub fn reqwest_config(&self) -> Option<ReqwestConnectorConfig> {
        self.reqwest.config_snapshot()
    }

    #[cfg(feature = "tonic_conn")]
    pub fn tonic_config(&self) -> Option<TonicConnectorConfig> {
        self.tonic.config_snapshot()
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
}

impl<C> ConnectorSlot<C>
where
    C: BaseConnector,
{
    fn new(config: Option<C::MainConfig>) -> Self {
        Self {
            config: RwLock::new(config),
            instance: Mutex::new(None),
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
        if let Some(connector) = guard.as_ref() {
            connector.cancel_token().cancel();
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
        if let Some(connector) = self.instance.get_mut().take() {
            connector.cancel_token().cancel();
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
