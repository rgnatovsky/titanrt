use serde::{Deserialize, Serialize};

use crate::utils::time::Timeframe;
use ahash::AHashMap;
use bytes::Bytes;
use std::collections::VecDeque;
use std::hash::Hash;
use std::{
    cmp,
    collections::HashMap,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};
use tokio::task;
use tokio::time::{Instant as TokioInstant, sleep_until};

#[derive(Debug, Clone, Deserialize, Serialize)]
/// Top-level rate limit configuration for a venue (exchange).
pub struct RateLimitConfig {
    /// Human-readable venue/exchange identifier.
    pub venue: String,
    /// Rules keyed by logical group (e.g., endpoint path or label).
    /// The exact key semantics are application-defined.
    pub rules: HashMap<String, Vec<RateLimitRule>>,
    /// Fallback rules applied when no entry exists in `rules` for a given key.
    pub default_rules: Vec<RateLimitRule>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Scope at which the rule is enforced.
pub enum RateLimitScope {
    /// One global budget across all requests.
    Global,
    /// A separate budget per user/account.
    PerAccount,
    /// A separate budget per endpoint.
    PerEndpoint,
}

impl RateLimitScope {
    /// Stable string form used in logs/keys/serialization.
    pub fn as_str(&self) -> &str {
        match self {
            RateLimitScope::Global => "global",
            RateLimitScope::PerAccount => "per_account",
            RateLimitScope::PerEndpoint => "per_endpoint",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
/// Dimension on which requests are partitioned for limiting.
pub enum RateLimitType {
    /// Group by IP address.
    Ip,
    /// Group by user/account id.
    Uid,
    /// Group by endpoint/path.
    Endpoint,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
/// Single rate limit rule: "limit events per timeframe", optionally weighted.
pub struct RateLimitRule {
    /// Partitioning dimension (ip/uid/endpoint).
    pub limit_type: RateLimitType,
    /// Enforcement scope (global/per_account/per_endpoint).
    pub scope: RateLimitScope,
    /// Time window over which the limit applies.
    pub timeframe: Timeframe,
    /// Maximum allowed events within `timeframe` (for weight=1).
    pub limit: u64,
    /// Cost of a single event relative to the budget (default: 1).
    #[serde(default = "default_weight")]
    pub weight: u64,
    /// Whether the weight can be overridden by the user.
    #[serde(default = "default_can_override_weight")]
    pub can_override_weight: bool,
    /// Precomputed cache key used by the limiter; not deserialized from config.
    #[serde(skip_deserializing)]
    pub key: RateLimitKey,
}

fn default_can_override_weight() -> bool {
    false
}

fn default_weight() -> u64 {
    1
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct RateLimitContext<'a> {
    pub venue: &'a str,
    pub endpoint: Option<&'a str>,
    pub ip: Option<&'a str>,
    pub account_id: Option<&'a str>,
}

impl<'a> RateLimitContext<'a> {
    /// Create a new context.
    pub fn new(venue: &'a str) -> Self {
        Self {
            venue,
            endpoint: None,
            ip: None,
            account_id: None,
        }
    }

    /// Set endpoint.
    pub fn with_endpoint(mut self, endpoint: &'a str) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    /// Set IP address.
    pub fn with_ip(mut self, ip: &'a str) -> Self {
        self.ip = Some(ip);
        self
    }

    /// Set user/account id.
    pub fn with_account_id(mut self, account_id: &'a str) -> Self {
        self.account_id = Some(account_id);
        self
    }

    /// Serializes the implementing struct into a byte slice. The function writes string and optional
    /// string fields to the byte slice.
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = Vec::new();

        fn write_str(buf: &mut Vec<u8>, s: &str) {
            buf.push(s.len() as u8);
            buf.extend_from_slice(s.as_bytes());
        }

        write_str(&mut buf, self.venue);

        fn write_opt(buf: &mut Vec<u8>, v: Option<&str>) {
            match v {
                Some(s) => {
                    buf.push(1);
                    write_str(buf, s);
                }
                None => buf.push(0),
            }
        }

        write_opt(&mut buf, self.endpoint);
        write_opt(&mut buf, self.ip);
        write_opt(&mut buf, self.account_id);

        Bytes::from(buf)
    }

    /// Parses a byte slice into an instance of the implementing struct, if the slice adheres to the
    /// expected format. The function reads string and optional string fields from the byte slice and
    /// creates an object with the extracted data.

    pub fn from_bytes(mut bytes: &'a [u8]) -> Option<Self> {
        fn read_str<'a>(bytes: &mut &'a [u8]) -> Option<&'a str> {
            if bytes.is_empty() {
                return None;
            }
            let len = bytes[0] as usize;
            *bytes = &bytes[1..];
            if bytes.len() < len {
                return None;
            }
            let (s, rest) = bytes.split_at(len);
            *bytes = rest;

            Some(unsafe { str::from_utf8_unchecked(s) })
        }

        fn read_opt<'a>(bytes: &mut &'a [u8]) -> Option<Option<&'a str>> {
            if bytes.is_empty() {
                return None;
            }
            let flag = bytes[0];
            *bytes = &bytes[1..];
            if flag == 1 {
                Some(read_str(bytes).map(Some)?)
            } else {
                Some(None)
            }
        }

        let venue = read_str(&mut bytes)?;
        let endpoint = read_opt(&mut bytes)?;
        let ip = read_opt(&mut bytes)?;
        let account_id = read_opt(&mut bytes)?;

        Some(Self {
            venue,
            endpoint,
            ip,
            account_id,
        })
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Default, Serialize)]
pub struct RateLimitKey(String);

impl RateLimitKey {
    pub fn new(rule: &RateLimitRule, ctx: &RateLimitContext) -> RateLimitKey {
        let key = match rule.limit_type {
            RateLimitType::Ip => format!(
                "{}:ip:{}:{}",
                ctx.venue,
                ctx.ip.unwrap_or_default(),
                rule.scope.as_str()
            ),
            RateLimitType::Uid => format!(
                "{}:uid:{}:{}",
                ctx.venue,
                ctx.account_id.unwrap_or_default(),
                rule.scope.as_str()
            ),
            RateLimitType::Endpoint => format!(
                "{}:endpoint:{}:{}",
                ctx.venue,
                ctx.endpoint.unwrap_or_default(),
                rule.scope.as_str()
            ),
        };

        RateLimitKey(key)
    }
}

#[derive(Debug)]
struct PermitReq {
    pub weight: u32,
    pub reply: oneshot::Sender<()>,
}

#[derive(Clone)]
pub(crate) struct BucketHandle {
    tx: mpsc::Sender<PermitReq>,
}

impl BucketHandle {
    pub async fn wait(&self, weight: u32) {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(PermitReq { weight, reply: tx }).await;
        let _ = rx.await;
    }

    fn spawn_bucket_actor(limit_per_window: u32, interval: Duration) -> BucketHandle {
        assert!(limit_per_window > 0, "limit_per_window must be > 0");

        let spacing = {
            let ns = interval.as_nanos() / (limit_per_window as u128);
            let ns = ns.max(1);
            Duration::from_nanos(ns as u64)
        };

        let tau = interval
            .checked_sub(spacing)
            .unwrap_or(Duration::from_nanos(0));

        let (tx, mut rx) = mpsc::channel::<PermitReq>(1024);

        task::spawn_local(async move {
            let mut queue: VecDeque<PermitReq> = VecDeque::new();

            let mut tat: Instant = Instant::now();

            loop {
                if queue.is_empty() {
                    match rx.recv().await {
                        Some(req) => queue.push_back(req),
                        None => break,
                    }
                    continue;
                }

                let now = Instant::now();

                let eligible_at = tat.checked_sub(tau).unwrap_or(now);

                if now >= eligible_at {
                    let PermitReq { weight, reply } = queue.pop_front().unwrap();

                    let step = spacing.checked_mul(weight).unwrap_or(Duration::MAX);

                    tat = cmp::max(tat, now).checked_add(step).unwrap();

                    let _ = reply.send(());
                    continue;
                }

                tokio::select! {
                    Some(req) = rx.recv() => {
                        queue.push_back(req);
                    }
                    _ = sleep_until(TokioInstant::from_std(eligible_at)) => {
                    }
                }
            }
        });

        BucketHandle { tx }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RuleSpec {
    pub key: RateLimitKey,
    pub limit: u32,
    pub interval: Duration,
    pub weight: u32,
    pub can_override_weight: bool,
}

pub(crate) struct RateLimitManager {
    venue_configs: AHashMap<String, RateLimitConfig>,
    rules_cache: AHashMap<Bytes, Vec<RuleSpec>>,
    buckets: AHashMap<RateLimitKey, BucketHandle>,
}

impl RateLimitManager {
    pub fn new(configs: Vec<RateLimitConfig>) -> Self {
        let venue_configs = configs.into_iter().map(|c| (c.venue.clone(), c)).collect();
        Self {
            venue_configs,
            rules_cache: AHashMap::default(),
            buckets: AHashMap::default(),
        }
    }

    fn resolve_rules(&mut self, bytes_ctx: &Bytes) -> bool {
        if self.rules_cache.contains_key(bytes_ctx) {
            return true;
        }

        let Some(ctx) = RateLimitContext::from_bytes(bytes_ctx) else {
            return false;
        };
        let Some(venue_cfg) = self.venue_configs.get(ctx.venue) else {
            return false;
        };

        let mut out: Vec<RuleSpec> = Vec::new();

        if let Some(ep) = ctx.endpoint {
            if let Some(rules) = venue_cfg.rules.get(ep) {
                for r in rules {
                    let key = RateLimitKey::new(r, &ctx);
                    out.push(RuleSpec {
                        key,
                        limit: r.limit as u32,
                        interval: r.timeframe.duration(),
                        weight: r.weight as u32,
                        can_override_weight: r.can_override_weight,
                    });
                }
            }
        }

        for r in &venue_cfg.default_rules {
            let key = RateLimitKey::new(r, &ctx);
            out.push(RuleSpec {
                key,
                limit: r.limit as u32,
                interval: r.timeframe.duration(),
                weight: r.weight as u32,
                can_override_weight: r.can_override_weight,
            });
        }

        self.rules_cache.insert(bytes_ctx.clone(), out);

        true
    }

    pub fn plan(
        &mut self,
        bytes_ctx: &Bytes,
        weight_override: Option<usize>,
    ) -> Option<Vec<(BucketHandle, u32)>> {
        if !self.resolve_rules(bytes_ctx) {
            return None;
        }

        let rules = self.rules_cache.get(bytes_ctx).unwrap();
        let mut plan = Vec::with_capacity(rules.len());

        for spec in rules.iter() {
            let handle = self
                .buckets
                .entry(spec.key.clone())
                .or_insert_with(|| BucketHandle::spawn_bucket_actor(spec.limit, spec.interval))
                .clone();

            let weight = if spec.can_override_weight
                && let Some(w) = weight_override
            {
                w as u32
            } else {
                spec.weight
            };

            plan.push((handle, weight));
        }
        Some(plan)
    }
}
