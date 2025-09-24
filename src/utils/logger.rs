use anyhow::Context;
use std::str::FromStr;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
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

    pub fn init(&self) -> anyhow::Result<Option<tracing_appender::non_blocking::WorkerGuard>> {
        let level = Level::from_str(&self.level).unwrap_or(Level::INFO);

        if let Some(dir_str) = self.file_dir.as_deref() {
            let prefix = self.file_prefix.as_deref().unwrap_or("");

            let rotation = match self.rolling.as_deref() {
                Some("hourly") => Rotation::HOURLY,
                Some("minutely") => Rotation::MINUTELY,
                _ => Rotation::DAILY,
            };

            let appender: RollingFileAppender = RollingFileAppender::builder()
                .rotation(rotation)
                .max_log_files(self.max_files)
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
                self.rolling
            );
            Ok(Some(guard))
        } else {
            let _ = tracing_subscriber::fmt().with_max_level(level).try_init();
            tracing::info!("logging to stdout (no file_dir)");
            Ok(None)
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

///  Throttle for log messages.
///  It allows to limit the frequency of log messages and suppresses repeated messages within a specified interval.
///  When a message is logged after the interval, it also logs how many messages were suppressed
/// Usage:
///   error_throttled!(std::time::Duration::from_secs(5), "connect failed: {e:?}");

pub struct Throttle {
    last: Mutex<Instant>,
    suppressed: AtomicU64,
    interval: Duration,
}

impl Throttle {
    pub fn new(interval: Duration) -> Self {
        // стартуем так, чтобы сразу можно было эмитить
        let start = Instant::now()
            .checked_sub(interval)
            .unwrap_or_else(Instant::now);
        Self {
            last: Mutex::new(start),
            suppressed: AtomicU64::new(0),
            interval,
        }
    }

    #[inline]
    pub fn poll(&self) -> Option<u64> {
        if self.interval.as_nanos() > 0 {
            if let Ok(guard) = self.last.try_lock() {
                if guard.elapsed() < self.interval {
                    self.suppressed.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
                drop(guard);
            }
        }

        // точная проверка с блокировкой
        let mut last = self.last.lock().unwrap();
        if last.elapsed() >= self.interval {
            *last = Instant::now();
            let skipped = self.suppressed.swap(0, Ordering::Relaxed);
            Some(skipped)
        } else {
            self.suppressed.fetch_add(1, Ordering::Relaxed);
            None
        }
    }
}

// --- Макросы уровня ---

/// Универсальный макрос: уровень задаётся идентификатором (`error`, `warn`, `info`, `debug`, `trace`).
#[macro_export]
macro_rules! log_throttled {
    ($level:ident, $interval:expr, $($arg:tt)*) => {{
        // Пер-коллсайт статик: один throttle на точку логирования.
        static _THROTTLE: std::sync::OnceLock<$crate::Throttle> = std::sync::OnceLock::new();
        let t = _THROTTLE.get_or_init(|| $crate::Throttle::new($interval));
        if let Some(_suppressed) = t.poll() {
            if _suppressed > 0 {
                tracing::$level!(suppressed = _suppressed, $($arg)*);
            } else {
                tracing::$level!($($arg)*);
            }
        }
    }};
}

/// Сокращённые удобные макросы под каждый уровень
#[macro_export]
macro_rules! error_throttled { ($interval:expr, $($arg:tt)*) => { $crate::log_throttled!(error, $interval, $($arg)*); } }
#[macro_export]
macro_rules! warn_throttled  { ($interval:expr, $($arg:tt)*) => { $crate::log_throttled!(warn,  $interval, $($arg)*); } }
#[macro_export]
macro_rules! info_throttled  { ($interval:expr, $($arg:tt)*) => { $crate::log_throttled!(info,  $interval, $($arg)*); } }
#[macro_export]
macro_rules! debug_throttled { ($interval:expr, $($arg:tt)*) => { $crate::log_throttled!(debug, $interval, $($arg)*); } }
#[macro_export]
macro_rules! trace_throttled { ($interval:expr, $($arg:tt)*) => { $crate::log_throttled!(trace, $interval, $($arg)*); } }
