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

    use tracing_appender::non_blocking::WorkerGuard;

    use crate::{
        connector::{
            BaseConnector, HookArgs,
            features::{
                shared::{clients_map::SpecificClient, events::StreamEvent},
                tonic::{
                    client::TonicChannelSpec,
                    connector::{TonicConnector, TonicConnectorConfig},
                    grpcbin::{
                        DummyMessage, SubscribeRequest, SubscribeRequestFilterBlocks,
                        SubscribeRequestFilterSlots, dummy_message,
                    },
                    streaming::{
                        StreamingAction, StreamingEvent, StreamingMode, TonicStreamingDescriptor,
                    },
                    unary::{GrpcMethod, TonicUnaryDescriptor, UnaryAction, UnaryEvent},
                },
            },
        },
        io::ringbuffer::RingSender,
        utils::{CancelToken, NullReducer, NullState, logger::LoggerConfig},
    };

    fn logging_init(level: &str) -> Option<WorkerGuard> {
        let mut logger = LoggerConfig::default();
        logger.level = level.to_string();
        logger.init().unwrap()
    }

    fn tonic_conn_init(uri: &str, tls: bool, conn_id: usize) -> TonicConnector {
        let cancel = CancelToken::new_root();
        TonicConnector::init(
            TonicConnectorConfig {
                default_max_cores: Some(4),
                specific_core_ids: vec![],
                use_core_stats: true,
                client: crate::connector::features::shared::clients_map::ClientConfig {
                    default: None,
                    specific: vec![SpecificClient {
                        id: conn_id,
                        ip: None,
                        spec: TonicChannelSpec {
                            uri: uri.to_string(),
                            connect_timeout_ms: Some(10000),
                            request_timeout_ms: Some(10000),
                            tcp_nodelay: Some(true),
                            http2_keepalive_interval_ms: Some(10000),
                            http2_keepalive_timeout_ms: Some(10000),
                            tls: tls,
                        },
                    }],
                    fail_on_empty: false,
                },
            },
            cancel,
            None,
        )
        .unwrap()
    }

    #[test]
    fn test_grpc_streaming() {
        let _logger = logging_init("debug");

        let conn_id = 0;

        let mut tonic_conn = tonic_conn_init(
            "https://yellowstone-solana-mainnet.core.chainstack.com",
            true,
            conn_id,
        );

        let streaming_descriptor = TonicStreamingDescriptor::high_throughput();
        
        let mut geyser_stream = tonic_conn
            .spawn_stream(streaming_descriptor, streaming_geyser_hook)
            .unwrap();

        let mut req = SubscribeRequest::default();

        req.slots.insert(
            "slots-main".into(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                interslot_updates: Some(false),
            },
        );

        let connection = StreamingAction::connect(
            GrpcMethod::Full {
                pkg: "geyser",
                service: "Geyser",
                method: "Subscribe",
            },
            StreamingMode::Bidi,
        )
        .subscription(req)
        .header_kv("x-token", "--token-value--")
        .build()
        .to_builder()
        .conn_id(conn_id)
        .build();

        geyser_stream.try_send(connection).unwrap();

        std::thread::sleep(std::time::Duration::from_secs(5));

        let mut req = SubscribeRequest::default();

        req.blocks.insert(
            "blocks".into(),
            SubscribeRequestFilterBlocks {
                account_include: vec!["6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P".to_string()],
                include_transactions: Some(true),
                include_accounts: Some(false),
                include_entries: Some(false),
            },
        );

        geyser_stream
            .try_send(
                StreamingAction::send(req)
                    .to_builder()
                    .conn_id(conn_id)
                    .build(),
            )
            .unwrap();

        std::thread::sleep(std::time::Duration::from_secs(5));

        geyser_stream.cancel();
    }

    pub fn streaming_geyser_hook(
        args: HookArgs<
            StreamEvent<StreamingEvent>,
            RingSender<()>,
            NullReducer,
            NullState,
            TonicStreamingDescriptor,
        >,
    ) {
        tracing::debug!("Geyser hook: event={:?}", args.raw);
    }

    #[test]
    fn test_unary_stream() {
    
        let _logger = logging_init("debug");
        let conn_id = 0;
        let mut tonic_conn = tonic_conn_init("https://grpcb.in:9001", true, conn_id);

        let mut jito_unary_descriptor = TonicUnaryDescriptor::high_throughput();
        jito_unary_descriptor.max_decoding_message_size = Some(10 * 1024 * 1024);
        jito_unary_descriptor.max_encoding_message_size = Some(10 * 1024 * 1024);

        let mut grpc_unary_stream = tonic_conn
            .spawn_stream(jito_unary_descriptor, test_unary_hook)
            .expect("spawn grpc stream failed");

        let msg = DummyMessage {
            f_string: "test".to_string(),
            f_strings: vec!["test1".to_string(), "test2".to_string()],
            f_int32: 42,
            f_int32s: vec![1, 2, 3],
            f_enum: dummy_message::Enum::Enum1 as i32,
            f_enums: vec![dummy_message::Enum::Enum2 as i32],
            f_sub: Some(dummy_message::Sub {
                f_string: "sub_test".to_string(),
            }),
            f_subs: vec![dummy_message::Sub {
                f_string: "sub_test1".to_string(),
            }],
            f_bool: true,
            f_bools: vec![true, false, true],
            f_int64: 123456789,
            f_int64s: vec![987654321, 123456789],
            f_bytes: vec![1, 2, 3, 4, 5],
            f_bytess: vec![vec![10, 20, 30], vec![40, 50, 60]],
            f_float: 0.5,
            f_floats: vec![1.1, 2.2, 3.3],
        };

        let action = UnaryAction::new(
            GrpcMethod::Full {
                pkg: "grpcbin",
                service: "GRPCBin",
                method: "DummyUnary",
            },
            msg,
        )
        .to_builder()
        .conn_id(conn_id)
        .build();

        match grpc_unary_stream.try_send(action) {
            Ok(_) => {
                tracing::info!("Sent unary action");
            }
            Err(e) => tracing::info!("Failed to send unary action: {:?}", e),
        }

        std::thread::sleep(std::time::Duration::from_secs(5));

        grpc_unary_stream.cancel();
    }

    pub fn test_unary_hook(
        args: HookArgs<
            StreamEvent<UnaryEvent>,
            RingSender<()>,
            NullReducer,
            NullState,
            TonicUnaryDescriptor,
        >,
    ) {
        if let Ok(m) = args.raw.inner().decode_as::<DummyMessage>() {
            tracing::info!("Received message: {:?}", m);
        } else {
            tracing::info!("Failed to decode message");
        }
    }
}
