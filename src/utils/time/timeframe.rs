use std::{str::FromStr, time::Duration};

use anyhow::{Result, anyhow};
use regex::Regex;
use serde::{Deserialize, Serialize};

use super::TimeUnit;

#[derive(Debug, Clone, PartialEq)]
pub enum TimeframeType {
    StringValue(String),
    StructValue(Timeframe),
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Timeframe {
    pub value: u64,
    pub unit: TimeUnit,
}

impl Timeframe {
    pub fn new(value: u64, unit: TimeUnit) -> Self {
        Self { value, unit }
    }

    pub fn convert(&self, target_unit: TimeUnit) -> u64 {
        self.unit.convert(self.value, target_unit)
    }

    pub fn duration(&self) -> Duration {
        self.unit.duration(self.value)
    }
}

impl FromStr for Timeframe {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let trimmed = s.trim();
        let re = Regex::new(r"(\d+)([a-zA-Z]+)")
            .map_err(|_| anyhow!("Ошибка при компиляции регулярного выражения"))?;
        let caps = re
            .captures(trimmed)
            .ok_or_else(|| anyhow!("Неверный формат таймфрейма: {}", trimmed))?;

        let value_str = &caps[1];
        let unit_str = &caps[2].to_lowercase();
        let value = value_str
            .parse()
            .map_err(|_| anyhow!("Неверный формат таймфрейма: {}", trimmed))?;

        let unit = match unit_str.as_str() {
            "sec" => TimeUnit::Second,
            "min" => TimeUnit::Minute,
            "h" => TimeUnit::Hour,
            "d" => TimeUnit::Day,
            "w" => TimeUnit::Week,
            "m" => TimeUnit::Month,
            _ => {
                return Err(anyhow!(
                    "Не поддерживаемая единица таймфрейма: '{}'",
                    unit_str
                ));
            }
        };

        Ok(Timeframe { value, unit })
    }
}
