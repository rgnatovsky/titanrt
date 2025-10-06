use std::ops::{Deref, DerefMut};
use std::vec::Vec;

use anyhow::Result;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::connector::BaseConnector;
use crate::connector::features::composite::CancelStreamsPolicy;
use crate::utils::CancelToken;

pub struct ConnectorInner<C>
where
    C: BaseConnector,
{
    config: RwLock<Option<C::MainConfig>>,
    instance: Mutex<Option<C>>,
    cancel_streams: CancelStreamsPolicy,
}

impl<C> ConnectorInner<C>
where
    C: BaseConnector,
{
    pub fn new(config: Option<C::MainConfig>, cancel_streams: CancelStreamsPolicy) -> Self {
        Self {
            config: RwLock::new(config),
            instance: Mutex::new(None),
            cancel_streams,
        }
    }

    pub fn update_config(&self, config: Option<C::MainConfig>) {
        {
            let mut guard = self.config.write();
            *guard = config;
        }
        self.drop_instance();
    }

    pub fn unload(&self) {
        self.drop_instance();
    }

    pub fn drop_instance(&self) {
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

    pub fn config_snapshot(&self) -> Option<C::MainConfig> {
        self.config.read().clone()
    }

    pub fn ensure(
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

    pub fn with<F, R>(
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

    pub fn try_get_existing(&self) -> Option<ConnectorGuard<'_, C>> {
        let guard = self.instance.lock();
        if guard.is_some() {
            Some(ConnectorGuard::new(guard))
        } else {
            None
        }
    }
}

impl<C> Drop for ConnectorInner<C>
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
