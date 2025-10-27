use std::{collections::HashSet, marker::PhantomData, sync::Arc};

use ahash::AHashMap;
use anyhow::{Result, anyhow};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SpecificClient<Spec: Clone> {
    pub id: usize,
    pub ip: Option<String>,
    pub spec: Spec,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClientConfig<Spec: Clone> {
    pub default: Option<Spec>,
    pub specific: Vec<SpecificClient<Spec>>,
    #[serde(default)]
    pub fail_on_empty: bool,
}

pub trait ClientInitializer<Spec: Clone>
where
    Self: Sized,
{
    fn init(spec: &SpecificClient<Spec>, rt: Option<Arc<Runtime>>) -> Result<Self>;
}

#[derive(Clone)]
pub struct ClientsMap<Client: ClientInitializer<Spec>, Spec: Clone> {
    inner: Arc<RwLock<AHashMap<usize, Arc<Client>>>>,
    default_id: Arc<RwLock<Option<usize>>>,
    init_rt: Option<Arc<Runtime>>,
    _spec: PhantomData<Spec>,
}

impl<Client: ClientInitializer<Config>, Config: Clone> ClientsMap<Client, Config> {
    pub fn new(config: &ClientConfig<Config>, rt: Option<Arc<Runtime>>) -> Result<Self> {
        let map = Self {
            inner: Arc::new(RwLock::new(AHashMap::new())),
            default_id: Arc::new(RwLock::new(None)),
            init_rt: rt,
            _spec: PhantomData,
        };

        map.rebuild(config)?;

        Ok(map)
    }

    /// Rebuild the map from a full configuration snapshot.
    pub fn rebuild(&self, config: &ClientConfig<Config>) -> Result<()> {
        let mut id_seen: HashSet<usize> = HashSet::default();
        let mut prepared: Vec<(usize, Arc<Client>)> = Vec::with_capacity(config.specific.len() + 1);
        let mut default_slot: Option<usize> = None;

        for spec in config.specific.iter() {
            if !id_seen.insert(spec.id) {
                tracing::warn!(
                    "[CientsPool] ID <{}> of IP <{:?}> addr is not unique",
                    spec.id,
                    spec.ip
                );
                continue;
            }

            let client = self.init_client(spec)?;
            prepared.push((spec.id, client));
        }

        if let Some(spec) = &config.default {
            let default_id = id_seen.len();
            let default_client = SpecificClient {
                id: default_id,
                ip: None,
                spec: spec.clone(),
            };
            let client = self.init_client(&default_client)?;
            prepared.push((default_id, client));
            default_slot = Some(default_id);
        }

        *self.default_id.write() = default_slot;

        if config.fail_on_empty && prepared.is_empty() {
            return Err(anyhow!("Clients map is empty"));
        }

        let mut guard = self.inner.write();
        guard.clear();
        for (id, client) in prepared {
            guard.insert(id, client);
        }

        Ok(())
    }

    fn init_client(&self, spec: &SpecificClient<Config>) -> Result<Arc<Client>> {
        Client::init(spec, self.init_rt.clone()).map(Arc::new)
    }

    pub fn get(&self, id: &usize) -> Option<Arc<Client>> {
        self.inner.read().get(id).cloned()
    }

    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn contains(&self, id: &usize) -> bool {
        self.inner.read().contains_key(id)
    }

    pub fn default_id(&self) -> Option<usize> {
        *self.default_id.read()
    }

    pub fn ids(&self) -> Vec<usize> {
        self.inner.read().keys().copied().collect()
    }

    pub fn upsert(&self, client: SpecificClient<Config>) -> Result<Arc<Client>> {
        let id = client.id;
        let client = self.init_client(&client)?;
        let mut guard = self.inner.write();
        guard.insert(id, client.clone());
        Ok(client)
    }

    pub fn remove(&self, id: usize) -> Option<Arc<Client>> {
        let removed = self.inner.write().remove(&id);
        if removed.is_some() {
            let mut default_guard = self.default_id.write();
            if default_guard.as_ref() == Some(&id) {
                *default_guard = None;
            }
        }
        removed
    }

    pub fn next_vacant_id(&self) -> usize {
        let guard = self.inner.read();
        let mut candidate = guard.len();
        while guard.contains_key(&candidate) {
            candidate += 1;
        }
        candidate
    }

    pub fn sole_entry_id(&self) -> Option<usize> {
        let guard = self.inner.read();
        if guard.len() == 1 {
            guard.keys().next().copied()
        } else {
            None
        }
    }
}
