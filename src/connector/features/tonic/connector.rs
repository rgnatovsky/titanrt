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

        let rt_tokio = Arc::new(
            Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
                .map_err(|e| anyhow::anyhow!("Tokio Runtime error: {e}"))?,
        );

        let clients_map = ClientsMap::new(&config.client, Some(rt_tokio.clone()))?;

        Ok(Self {
            config,
            clients_map,
            cancel_token,
            core_stats,
            rt_tokio,
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

    use bytes::Bytes;
    use prost::Message;

    use crate::{
        connector::{
            BaseConnector, HookArgs,
            features::{
                shared::{actions::StreamAction, clients_map::SpecificClient, events::StreamEvent},
                tonic::{
                    client::TonicChannelSpec,
                    connector::{TonicConnector, TonicConnectorConfig},
                    unary::{TonicUnaryDescriptor, UnaryAction, UnaryEvent},
                },
            },
        },
        io::ringbuffer::RingSender,
        utils::{CancelToken, NullReducer, NullState, logger::LoggerConfig},
    };

    #[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
    pub struct NextScheduledLeaderRequest {
        /// Defaults to the currently connected region if no region provided.
        #[prost(string, repeated, tag = "1")]
        pub regions: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    }

    #[test]
    fn connector_init_test() {
        let mut _logger = LoggerConfig::default();
        _logger.level = "debug".to_string();
        let _logger = _logger.init();

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
                            uri: "https://mainnet.block-engine.jito.wtf".to_string(),
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

        let mut jito_unary_stream = tonic_conn
            .spawn_stream(jito_unary_descriptor, jito_unary_hook)
            .expect("spawn_stream failed");
        let mut buf = vec![];

        NextScheduledLeaderRequest {
            regions: vec![],
        }
        .encode(&mut buf)
        .unwrap();

        let unary = UnaryAction::new(
            "searcher.SearcherService/GetNextScheduledLeader",
            Bytes::from(buf),
        )
        .expect("Block engine fee info request should be valid");

        let action = StreamAction::builder(Some(unary)).conn_id(0).build();

        match jito_unary_stream.try_send(action) {
            Ok(_) => {
                tracing::info!("Sent jito unary action");
            }
            Err(e) => tracing::info!("Failed to send jito unary action: {:?}", e),
        }

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
        tracing::info!("Receive event: {:?}", args.raw);
    }
}
