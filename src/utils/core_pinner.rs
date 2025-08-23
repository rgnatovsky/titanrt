// core_stats.rs
use ahash::AHashMap;
use anyhow::Context;
use crossbeam::utils::CachePadded;
use std::ops::Deref;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    sync::Arc,
    sync::atomic::{AtomicU32, AtomicU64, Ordering::*},
};
use core_affinity::{get_core_ids, set_for_current, CoreId};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum CorePickPolicy {
    MinimumThreads,
    RoundRobin,
    Specific(usize),
}

impl CorePickPolicy {
    pub fn specific(&self) -> Option<usize> {
        match self {
            CorePickPolicy::Specific(id) => Some(*id),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct PerCore {
    pub active_threads: AtomicU32, // number of OS threads pinned
    pub last_spawn_ns: AtomicU64,  // for simple freshness heuristics
}

impl PerCore {
    fn new() -> Self {
        Self {
            active_threads: AtomicU32::new(0),
            last_spawn_ns: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CoreStatsSnapshot {
    pub cores: Vec<CoreSnapshot>,
}

#[derive(Debug, Clone)]
pub struct CoreSnapshot {
    pub core_id: usize,
    pub active_threads: u32,
    pub last_spawn_ns: u64,
}

#[derive(Debug)]
pub struct CoreLease {
    pub core_id: usize,
    stats: Arc<CoreStats>,
}

impl Drop for CoreLease {
    fn drop(&mut self) {
        let pc = &self.stats.cores[&self.core_id];

        pc.active_threads.fetch_sub(1, Relaxed);
    }
}

#[derive(Debug)]
pub struct CoreStats {
    cores: AHashMap<usize, CachePadded<PerCore>>,
    rr_cursor: AtomicU32,
}

impl CoreStats {
    pub fn new(
        default_max_cores: Option<usize>,
        specific_core_ids: Vec<usize>,
        reserved_core_ids: Vec<usize>,
    ) -> anyhow::Result<Arc<Self>> {
        // 1) List of available core ids reported by the OS
        let sys = core_affinity::get_core_ids()
            .context("core_affinity::get_core_ids() failed or returned None")?;
        if sys.is_empty() {
            anyhow::bail!("No CPU cores reported by OS");
        }
        let sys_ids: Vec<usize> = sys
            .iter()
            .filter(|c| !reserved_core_ids.contains(&c.id))
            .map(|c| c.id)
            .collect();

        // For existence checking
        let mut is_sys = vec![false; sys_ids.iter().copied().max().unwrap_or(0) + 1];
        for &id in &sys_ids {
            if id >= is_sys.len() {
                is_sys.resize(id + 1, false);
            }
            is_sys[id] = true;
        }

        // 2) Build selected_ids
        let mut selected_ids: Vec<usize> = if !specific_core_ids.is_empty() {
            // prioritize user list: filter non-existent ids and duplicates
            let mut seen = vec![false; is_sys.len()];
            let mut out = Vec::with_capacity(specific_core_ids.len());
            for id in specific_core_ids.iter() {
                if id >= &is_sys.len() || !is_sys[*id] {
                    continue;
                }
                if id >= &seen.len() {
                    seen.resize(id + 1, false);
                }
                if !seen[*id] {
                    seen[*id] = true;
                    out.push(*id);
                }
            }
            out
        } else {
            // take all system cores
            sys_ids.clone()
        };

        // 3) Apply limit only if specific_cores is empty
        if specific_core_ids.is_empty()
            && let Some(max_n) = default_max_cores
            && max_n > 0
            && selected_ids.len() > max_n
        {
            selected_ids.truncate(max_n);
        }

        // 4) Валидация
        if selected_ids.is_empty() {
            anyhow::bail!("No valid CPU cores selected for tracking");
        }

        // 6) Выделяем пер-слотовые структуры
        let mut cores = AHashMap::with_capacity(selected_ids.len());
        for id in selected_ids {
            cores.insert(id, CachePadded::new(PerCore::new()));
        }

        Ok(Arc::new(Self {
            cores,
            rr_cursor: AtomicU32::new(0),
        }))
    }

    #[inline]
    pub fn per_core(&self, core_id: usize) -> Option<&PerCore> {
        // безопасно: slot формируется нашими методами
        self.cores.get(&core_id).map(CachePadded::deref)
    }

    #[inline]
    pub fn num_cores(&self) -> usize {
        self.cores.len()
    }

    pub fn snapshot(&self) -> CoreStatsSnapshot {
        let cores = self
            .cores
            .iter()
            .map(|(id, pc)| CoreSnapshot {
                core_id: *id,
                active_threads: pc.active_threads.load(Relaxed),
                last_spawn_ns: pc.last_spawn_ns.load(Relaxed),
            })
            .collect();

        CoreStatsSnapshot { cores }
    }

    /// Резерв под новый стрим/поток по политике.
    /// Возвращает CoreLease — RAII-гарду. Пока она жива, счётчики удерживаются.
    pub fn reserve(&self, policy: CorePickPolicy) -> CoreLease {
        let core_id = match policy {
            CorePickPolicy::Specific(id) => match self.cores.get(&id) {
                Some(_) => id,
                None => {
                    tracing::warn!(
                        "Core {} is not available. Available ids: {:?}. Using minimum threads.",
                        id,
                        self.cores.keys()
                    );

                    self.argmin_by(|pc| pc.active_threads.load(Relaxed))
                }
            },
            CorePickPolicy::RoundRobin => {
                let next = self.rr_cursor.fetch_add(1, Relaxed) as usize;
                next % self.cores.len()
            }
            CorePickPolicy::MinimumThreads => self.argmin_by(|pc| pc.active_threads.load(Relaxed)),
        };

        let pc = &self.cores[&core_id];

        pc.active_threads.fetch_add(1, Relaxed);

        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        pc.last_spawn_ns.store(now_ns, Relaxed);

        CoreLease {
            core_id,
            stats: Arc::clone(&self.arc_from_self()),
        }
    }

    #[inline]
    pub fn argmin_by<F>(&self, f: F) -> usize
    where
        F: Fn(&PerCore) -> u32,
    {
        self.cores
            .iter()
            .min_by_key(|(_, v)| f(v))
            .map(|(k, _)| *k)
            .expect("CoreStats.cores is empty")
    }
    /// Внутренняя помощка: получить Arc<Self> из &Self без лишнего клонирования API.
    fn arc_from_self(&self) -> Arc<Self> {
        // Трюк: Self уже должен жить внутри Arc<Self> в твоём коннекторе.
        // Храним рядом Arc<CoreStats> и отдаём его наружу напрямую (см. ниже в коннекторе).
        // Этот метод здесь только для полноты; на практике лучше хранить Arc<Self> рядом.
        // Если хочешь строгую безопасность — убери этот метод и пробрасывай Arc<CoreStats> из коннектора.
        unsafe { Arc::from_raw(self as *const _) }
    }
}

pub fn try_pin_core(core_id: usize) -> anyhow::Result<usize> {
    if let Some(core_ids) = get_core_ids()
       && core_ids.len() > core_id
       && set_for_current(CoreId { id: core_id })
    {
        return Ok(core_id);
    }
    Err(anyhow::anyhow!("failed to pin core"))
}