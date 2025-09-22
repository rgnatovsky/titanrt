use crate::connector::BaseConnector;
use crate::connector::features::shared::clients_map::{ClientConfig, ClientsMap};
use crate::connector::features::tonic::client::{TonicChannelSpec, TonicClient};
use crate::utils::{CancelToken, CoreStats};

use serde::{Deserialize, Serialize};
use std::{fmt::Display, sync::Arc};
use tokio::runtime::{Builder, Runtime};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TonicConnectorConfig {
    pub default_max_cores: Option<usize>,
    pub specific_core_ids: Vec<usize>,
    pub use_core_stats: bool,
    pub client: ClientConfig<TonicChannelSpec>,
}

pub struct TonicConnector {
    config: TonicConnectorConfig,
    clients_map: ClientsMap<TonicClient, TonicChannelSpec>,
    cancel_token: CancelToken,
    core_stats: Option<Arc<CoreStats>>,
    rt_tokio: Arc<tokio::runtime::Runtime>,
}

impl TonicConnector {
    pub fn clients_map(&self) -> ClientsMap<TonicClient, TonicChannelSpec> {
        self.clients_map.clone()
    }

    pub fn rt_tokio(&self) -> Arc<Runtime> {
        self.rt_tokio.clone()
    }
}

impl BaseConnector for TonicConnector {
    type MainConfig = TonicConnectorConfig;
    ///  создаются gRPC-каналы (tonic::transport::Channel) через create_channel().
    fn init(
        config: Self::MainConfig,
        cancel_token: CancelToken,
        reserved_core_ids: Option<Vec<usize>>,
    ) -> anyhow::Result<Self> {
        let core_stats = if config.use_core_stats {
            Some(CoreStats::new(
                config.default_max_cores,
                config.specific_core_ids.clone(),
                reserved_core_ids.unwrap_or_default(),
            )?)
        } else {
            None
        };

        let rt_tokio = Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .map_err(|e| anyhow::anyhow!("Tokio Runtime error: {e}"))?;

        let clients_map = rt_tokio.block_on(async { ClientsMap::new(&config.client) })?;

        Ok(Self {
            config,
            clients_map,
            cancel_token,
            core_stats,
            rt_tokio: Arc::new(rt_tokio),
        })
    }

    fn name(&self) -> impl AsRef<str> + Display {
        "tonic"
    }
    fn config(&self) -> &Self::MainConfig {
        &self.config
    }
    fn cancel_token(&self) -> &CancelToken {
        &self.cancel_token
    }
    fn cores_stats(&self) -> Option<Arc<CoreStats>> {
        self.core_stats.clone()
    }
}

impl Display for TonicConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TonicConnector")
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        connector::{
            BaseConnector, HookArgs,
            features::{
                shared::{clients_map::SpecificClient, events::StreamEvent},
                tonic::{
                    client::TonicChannelSpec,
                    connector::{TonicConnector, TonicConnectorConfig},
                    unary::{TonicUnaryDescriptor, UnaryEvent},
                },
            },
        },
        io::ringbuffer::RingSender,
        utils::{CancelToken, NullReducer, NullState},
    };

    #[test]
    fn connector_init_test() {
        let cancel = CancelToken::new_root();
        let mut tonic_conn = TonicConnector::init(
            TonicConnectorConfig {
                default_max_cores: Some(4),
                specific_core_ids: vec![],
                use_core_stats: true,
                client: crate::connector::features::shared::clients_map::ClientConfig {
                    default: None,
                    specific: vec![SpecificClient {
                        id: 0,
                        ip: None,
                        spec: TonicChannelSpec {
                            uri: "https://ny.testnet.block-engine.jito.wtf".to_string(),
                            connect_timeout_ms: Some(10000),
                            request_timeout_ms: Some(10000),
                            tcp_nodelay: Some(true),
                            http2_keepalive_interval_ms: Some(10000),
                            http2_keepalive_timeout_ms: Some(10000),
                        },
                    }],
                    fail_on_empty: false,
                },
            },
            cancel,
            None,
        )
        .unwrap();

        let jito_unary_descriptor = TonicUnaryDescriptor::high_throughput();

        let jito_unary_stream = tonic_conn
            .spawn_stream(jito_unary_descriptor, jito_unary_hook)
            .expect("spawn_stream failed");

        std::thread::sleep(std::time::Duration::from_secs(10));
        jito_unary_stream.cancel();
    }

    pub fn jito_unary_hook(
        args: HookArgs<
            StreamEvent<UnaryEvent>,
            RingSender<()>,
            NullReducer,
            NullState,
            TonicUnaryDescriptor,
        >,
    ) {
        println!(" Receive event: {:?}", args.raw);
    }
}
