use std::{collections::HashSet, marker::PhantomData, sync::Arc};

use ahash::AHashMap;
use anyhow::Result;
use serde::{Deserialize, Serialize};

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
    fn init(spec: &SpecificClient<Spec>) -> Result<Self>;
}

#[derive(Clone)]
pub struct ClientsMap<Client: ClientInitializer<Spec>, Spec: Clone> {
    pub(crate) inner: Arc<AHashMap<usize, Client>>,
    _spec: PhantomData<Spec>,
}

impl<Client: ClientInitializer<Config>, Config: Clone> ClientsMap<Client, Config> {
    pub fn new(config: &ClientConfig<Config>) -> Result<Self> {
        let mut clients_map = AHashMap::new();

        let mut id_seen: HashSet<usize> = HashSet::default();

        for spec in config.specific.iter() {
            if !id_seen.insert(spec.id) {
                tracing::warn!(
                    "[CientsPool] ID <{}> of IP <{:?}> addr is not unique",
                    spec.id,
                    spec.ip
                );
                continue;
            }

            let client = Client::init(spec)?;

            clients_map.insert(spec.id, client);
        }

        if let Some(spec) = &config.default {
            let default_id = id_seen.len();

            let default_client = Client::init(&SpecificClient {
                id: default_id,
                ip: None,
                spec: spec.clone(),
            })?;

            clients_map.insert(default_id, default_client);
        }

        if config.fail_on_empty {
            return Err(anyhow::anyhow!("Clients map is empty"));
        }

        Ok(Self {
            inner: Arc::new(clients_map),
            _spec: PhantomData,
        })
    }

    pub fn get(&self, id: &usize) -> Option<&Client> {
        self.inner.get(id)
    }
}
