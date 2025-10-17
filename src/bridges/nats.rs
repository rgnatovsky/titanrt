use crate::control::inputs::Input;
use crate::io::mpmc::MpmcReceiver;
use crate::model::{BaseModel, Output};
use crate::prelude::{BaseRx, BaseTx, Runtime};
use crate::utils::CancelToken;
use crate::utils::crypto::SecretValue;
use crate::utils::time::Timeframe;
use anyhow::Context;
use async_nats::ConnectOptions;
use futures::StreamExt;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NatsConfig {
    pub host: String,
    pub cmd_subject: String,
    pub out_subject: String,
    pub user_pass: Option<(SecretValue, SecretValue)>,
    pub base_backoff: Timeframe,
    pub connection_timeout: Timeframe,
    pub request_timeout: Timeframe,
    pub cmd_recv_timeout: Timeframe,
    pub tls: bool,
}

pub fn run_nats_bridge<M: BaseModel>(
    ncfg: NatsConfig,
    mut rt: Runtime<M>,
    output_rx: &mut MpmcReceiver<Output<M::OutputEvent>>,
) -> anyhow::Result<()>
where
    M::Event: for<'a> Deserialize<'a>,
    M::OutputEvent: Serialize,
{
    let rt_tokio = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("[NatsBridge] Failed to build Tokio runtime")?;

    rt_tokio.block_on(async move {
        let out_subject = ncfg.out_subject.clone();
        let cmd_subject = ncfg.cmd_subject.clone();
        let connection_tm = ncfg.connection_timeout.duration();
        let request_tm = ncfg.request_timeout.duration();
        let mut base_backoff = ncfg.base_backoff.duration();
        let wait_cmd = ncfg.cmd_recv_timeout.duration();
        let ctrl_tx = rt.control_tx();
        let cancel = CancelToken::new_root();

        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            let _ = shutdown_tx.send(());
        });

        'connect: loop {
            if shutdown_rx.try_recv().is_ok() {
                tracing::info!("shutdown requested (Ctrl+C)");
                rt.shutdown();
                break 'connect;
            }

            tracing::info!("[NatsBridge] Connecting to async-nats at {}", ncfg.host);

            let mut connect_opt = ConnectOptions::new()
                .request_timeout(Some(request_tm))
                .require_tls(ncfg.tls)
                .connection_timeout(connection_tm);

            if let Some(user_pass) = &ncfg.user_pass {
                connect_opt = connect_opt.user_and_password(
                    user_pass.0.value().to_string(),
                    user_pass.1.value().to_string(),
                );
            }

            let client = match connect_opt.connect(ncfg.host.clone()).await {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!("[NatsBridge] Connect error: {e}");
                    let mut rng = rand::thread_rng();
                    let jitter = rng.gen_range(0..=base_backoff.as_millis() as u64);
                    sleep(base_backoff + Duration::from_millis(jitter)).await;
                    base_backoff = (base_backoff * 2).min(Duration::from_secs(10));
                    continue;
                }
            };

            let mut sub = match client.subscribe(ncfg.cmd_subject.clone()).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("[NatsBridge] Subscribe '{}' failed: {e}", ncfg.cmd_subject);
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

            base_backoff = Duration::from_millis(500);

            tracing::info!(
                "[NatsBridge] Connected. Subscribed to '{}' → publishing to '{}'",
                ncfg.cmd_subject,
                ncfg.out_subject
            );

            'main: loop {
                match timeout(wait_cmd, sub.next()).await {
                    Ok(Some(msg)) => {
                        match serde_json::from_slice::<Input<M::Event>>(msg.payload.as_ref()) {
                            Ok(evt) => {
                                if let Err(e) =
                                    ctrl_tx.send(evt, &cancel, Some(Duration::from_millis(50)))
                                {
                                    tracing::error!("[NatsBridge - CMD] Send error: {e}");
                                }
                            }
                            Err(e) => tracing::error!("[NatsBridge - CMD] Parse error: {e}"),
                        }
                    }
                    Ok(None) => {
                        tracing::warn!(
                            "[NatsBridge - CMD] Subscription '{cmd_subject}' closed by server"
                        );
                        break 'main;
                    }
                    Err(_) => { /* timeout, fallthrough to drain outputs */ }
                }

                // Drain model outputs and publish
                while let Ok(output) = output_rx.try_recv() {
                    let body = match serde_json::to_vec(&output) {
                        Ok(b) => b,
                        Err(e) => {
                            tracing::error!("[NatsBridge - OUT] Serialize error: {e}");
                            continue;
                        }
                    };
                    match client.publish(out_subject.to_string(), body.into()).await {
                        Ok(_) => {}
                        Err(e) => {
                            tracing::error!("[NatsBridge - OUT] Publish error: {e}");
                            break 'main;
                        }
                    }
                }

                if shutdown_rx.try_recv().is_ok() {
                    tracing::info!("shutdown requested (Ctrl+C)");
                    rt.shutdown();

                    while let Ok(output) = output_rx.try_recv() {
                        let body = match serde_json::to_vec(&output) {
                            Ok(b) => b,
                            Err(e) => {
                                tracing::error!("[NatsBridge - OUT] Serialize error: {e}");
                                continue;
                            }
                        };
                        match client.publish(out_subject.to_string(), body.into()).await {
                            Ok(_) => {}
                            Err(e) => {
                                tracing::error!("[NatsBridge - OUT] Publish error: {e}");
                            }
                        }
                    }
                    break 'connect;
                }
            }

            sleep(Duration::from_millis(100)).await;
        }
    });

    Ok(())
}
