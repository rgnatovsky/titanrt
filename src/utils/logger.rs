use anyhow::Context;
use std::str::FromStr;
use tracing::Level;
use tracing_appender::rolling::{RollingFileAppender, Rotation};

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct LoggerConfig {
    pub level: String,
    pub file_dir: Option<String>,
    pub file_prefix: Option<String>,
    pub rolling: Option<String>,
    #[serde(default)]
    pub max_files: usize,
}

impl LoggerConfig {
    /// Loads logging configuration from environment variables.
    /// If a variable is not set, it will use a default value.
    /// Filled in variables are: LOG_LEVEL, LOG_FILE_DIR, LOG_FILE_PREFIX, LOG_ROLLING
    pub fn from_env() -> Self {
        let level = std::env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string());
        let file_dir = std::env::var("LOG_FILE_DIR").ok();
        let file_prefix = std::env::var("LOG_FILE_PREFIX").ok();
        let rolling = std::env::var("LOG_ROLLING").ok();

        Self {
            level,
            file_dir,
            file_prefix,
            rolling,
            max_files: 2,
        }
    }
}

impl Default for LoggerConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            file_dir: None,
            file_prefix: None,
            rolling: Some("daily".to_string()),
            max_files: 2,
        }
    }
}

pub fn init_logging(
    cfg: &LoggerConfig,
) -> anyhow::Result<Option<tracing_appender::non_blocking::WorkerGuard>> {
    let level = Level::from_str(&cfg.level).unwrap_or(Level::INFO);

    if let Some(dir_str) = cfg.file_dir.as_deref() {
        let prefix = cfg.file_prefix.as_deref().unwrap_or("");

        let rotation = match cfg.rolling.as_deref() {
            Some("hourly") => Rotation::HOURLY,
            Some("minutely") => Rotation::MINUTELY,
            _ => Rotation::DAILY,
        };

        let appender: RollingFileAppender = RollingFileAppender::builder()
            .rotation(rotation)
            .max_log_files(cfg.max_files)
            .filename_prefix(prefix)
            .build(dir_str)
            .with_context(|| format!("failed to create rolling appender in {}", dir_str))?;

        let (nb, guard) = tracing_appender::non_blocking(appender);

        let _ = tracing_subscriber::fmt()
            .with_max_level(level)
            .with_writer(nb)
            .try_init();

        tracing::info!(
            "logging to dir: {}, prefix: {}, rotation: {:?}",
            dir_str,
            prefix,
            cfg.rolling
        );
        Ok(Some(guard))
    } else {
        let _ = tracing_subscriber::fmt().with_max_level(level).try_init();
        tracing::info!("logging to stdout (no file_dir)");
        Ok(None)
    }
}
