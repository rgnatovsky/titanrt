use std::time::Duration;

use chrono::DateTime;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeUnit {
    Nanos,
    Micros,
    Millis,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
}

impl TimeUnit {
    /// Convert to nanoseconds (base unit)
    #[inline]
    pub const fn to_nanos(&self, value: u64) -> u128 {
        value as u128
            * match self {
                TimeUnit::Nanos => 1,
                TimeUnit::Micros => 1_000,
                TimeUnit::Millis => 1_000_000,
                TimeUnit::Second => 1_000_000_000,
                TimeUnit::Minute => 60 * 1_000_000_000,
                TimeUnit::Hour => 3_600 * 1_000_000_000,
                TimeUnit::Day => 86_400 * 1_000_000_000,
                TimeUnit::Week => 604_800 * 1_000_000_000,
                TimeUnit::Month => 2_629_800 * 1_000_000_000,
            }
    }

    pub fn duration(&self, value: u64) -> Duration {
        match self {
            TimeUnit::Nanos => Duration::from_nanos(value),
            TimeUnit::Micros => Duration::from_micros(value),
            TimeUnit::Millis => Duration::from_millis(value),
            TimeUnit::Second => Duration::from_secs(value),
            TimeUnit::Minute => Duration::from_secs(value * 60),
            TimeUnit::Hour => Duration::from_secs(value * 60 * 60),
            TimeUnit::Day => Duration::from_secs(value * 60 * 60 * 24),
            TimeUnit::Week => Duration::from_secs(value * 60 * 60 * 24 * 7),
            TimeUnit::Month => Duration::from_secs(value * 60 * 60 * 24 * 30), // Approximate
        }
    }

    /// Converts `value` from this unit to `target_unit`.
    #[inline]
    pub const fn convert(&self, value: u64, target_unit: TimeUnit) -> u64 {
        let nanos = self.to_nanos(value);
        (nanos / target_unit.to_nanos(1)) as u64
    }

    // Fast specialized helpers
    #[inline]
    pub const fn to_micros(&self, value: u64) -> u64 {
        self.convert(value, TimeUnit::Micros)
    }

    #[inline]
    pub const fn to_millis(&self, value: u64) -> u64 {
        self.convert(value, TimeUnit::Millis)
    }

    #[inline]
    pub const fn to_seconds(&self, value: u64) -> u64 {
        self.convert(value, TimeUnit::Second)
    }

    pub fn ts_to_dt_auto(timestamp: i64) -> String {
        // Infer most likely unit based on magnitude
        let unit = if timestamp > 253_402_300_799_000_000 {
            // ~year 10000 in nanoseconds
            TimeUnit::Nanos
        } else if timestamp > 253_402_300_799_000 {
            // ~year 10000 in microseconds
            TimeUnit::Micros
        } else if timestamp > 253_402_300_799 {
            // ~year 10000 in milliseconds
            TimeUnit::Millis
        } else {
            TimeUnit::Second
        };

        unit.ts_to_dt(timestamp)
    }

    /// Convert UNIX timestamp to a human-readable string using the specified unit
    pub fn ts_to_dt(&self, timestamp: i64) -> String {
        match self {
            TimeUnit::Nanos => DateTime::from_timestamp_nanos(timestamp)
                .format("%Y-%m-%d %H:%M:%S%.9f")
                .to_string(),
            TimeUnit::Micros => DateTime::from_timestamp_micros(timestamp)
                .unwrap()
                .format("%Y-%m-%d %H:%M:%S%.6f")
                .to_string(),
            TimeUnit::Millis => DateTime::from_timestamp_millis(timestamp)
                .unwrap()
                .format("%Y-%m-%d %H:%M:%S%.3f")
                .to_string(),
            TimeUnit::Second => DateTime::from_timestamp(timestamp, 0)
                .unwrap()
                .format("%Y-%m-%d %H:%M:%S")
                .to_string(),
            TimeUnit::Minute => DateTime::from_timestamp(timestamp * 60, 0)
                .unwrap()
                .format("%Y-%m-%d %H:%M:%S")
                .to_string(),
            TimeUnit::Hour => DateTime::from_timestamp(timestamp * 3600, 0)
                .unwrap()
                .format("%Y-%m-%d %H:%M:%S")
                .to_string(),
            TimeUnit::Day => DateTime::from_timestamp(timestamp * 86400, 0)
                .unwrap()
                .format("%Y-%m-%d %H:%M:%S")
                .to_string(),
            TimeUnit::Week => DateTime::from_timestamp(timestamp * 604800, 0)
                .unwrap()
                .format("%Y-%m-%d %H:%M:%S")
                .to_string(),
            TimeUnit::Month => {
                DateTime::from_timestamp(timestamp * 2_592_000, 0) // Approximately 30 days
                    .unwrap()
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string()
            }
        }
    }
}
