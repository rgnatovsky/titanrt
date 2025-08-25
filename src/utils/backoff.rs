use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct ReconnectCfg {
    /// Быстрые попытки сразу после обрыва
    pub fast_attempts: u32, // напр. 3
    pub fast_delay_ms: u64, // напр. 50

    /// Экспоненциальная часть
    pub base_delay_ms: u64, // напр. 200
    pub max_delay_ms: u64, // напр. 10_000
    pub factor: f64,       // напр. 1.8

    /// Сбросить счётчик, если соединение живо дольше этого времени
    pub reset_after_ms: u64, // напр. 30_000

    /// Ограничение на общее число попыток (None = бесконечно)
    pub max_retries: Option<u32>, // напр. None
}

pub struct Backoff {
    cfg: ReconnectCfg,
    attempt: u32,
    last_ok: Instant,
}

impl Backoff {
    pub fn new(cfg: ReconnectCfg) -> Self {
        Self {
            last_ok: Instant::now(), // «сейчас всё ок» до первого коннекта
            attempt: 0,
            cfg,
        }
    }

    #[inline]
    pub fn on_success(&mut self) {
        self.last_ok = Instant::now();
        self.attempt = 0;
    }

    #[inline]
    pub fn next_delay(&mut self) -> Duration {
        // быстрые попытки
        if self.attempt < self.cfg.fast_attempts {
            self.attempt += 1;
            return Duration::from_millis(self.cfg.fast_delay_ms);
        }

        let capped = {
            let exp = (self.cfg.base_delay_ms as f64)
                * self
                    .cfg
                    .factor
                    .powi((self.attempt - self.cfg.fast_attempts + 1) as i32);
            exp.min(self.cfg.max_delay_ms as f64) as u64
        };
        let base = Duration::from_millis(capped);

        self.attempt = self.attempt.saturating_add(1);
        base
    }

    #[inline]
    pub fn should_reset(&self) -> bool {
        self.last_ok.elapsed() >= Duration::from_millis(self.cfg.reset_after_ms)
    }

    #[inline]
    pub fn attempts_left(&self) -> Option<u32> {
        self.cfg.max_retries.map(|m| m.saturating_sub(self.attempt))
    }
}
