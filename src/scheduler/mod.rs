pub mod health;
pub mod scorer;
pub mod selector;

use std::sync::atomic::{AtomicI64, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use tokio::sync::Notify;

// ─── 健康等级 ───

pub const TIER_HEALTHY: u8 = 0;
pub const TIER_WARM: u8 = 1;
pub const TIER_RISKY: u8 = 2;
pub const TIER_BANNED: u8 = 3;

pub fn tier_name(tier: u8) -> &'static str {
    match tier {
        TIER_HEALTHY => "healthy",
        TIER_WARM => "warm",
        TIER_RISKY => "risky",
        TIER_BANNED => "banned",
        _ => "unknown",
    }
}

// ─── 运行时账号 ───

/// 单个账号的运行时状态 — 所有热路径字段用 atomic
pub struct Account {
    pub db_id: i64,
    pub email: RwLock<String>,
    pub plan_type: RwLock<String>,
    pub proxy_url: RwLock<String>,
    /// Codex 真实 account_id（来自 credentials.account_id）
    pub codex_account_id: RwLock<String>,

    // Token（冷路径，只在刷新时写入）
    pub access_token: RwLock<String>,
    pub refresh_token: RwLock<String>,
    pub expires_at: RwLock<DateTime<Utc>>,

    // 热路径 — 全部 atomic
    pub active_requests: AtomicI64,
    pub total_requests: AtomicU64,
    pub health_tier: AtomicU8,
    /// 评分 × 100 存储为整数（如 8520 = 85.20）
    pub score: AtomicI64,
    pub dynamic_concurrency_limit: AtomicI64,
    pub cooldown_until: AtomicI64, // unix timestamp，0 = 无冷却

    // 延迟 EWMA（×100 存储）
    pub latency_ewma_100: AtomicI64,

    // 连续成功/失败
    pub success_streak: AtomicI64,
    pub failure_streak: AtomicI64,

    // 最近 N 次请求结果（1=成功，0=失败）— 滑动窗口
    pub recent_results: RwLock<RecentWindow>,

    // 用量百分比 ×100 存储
    pub usage_7d_pct_100: AtomicI64,
    pub usage_5h_pct_100: AtomicI64,

    // 时间戳（unix）
    pub last_success_at: AtomicI64,
    pub last_failure_at: AtomicI64,
    pub last_unauthorized_at: AtomicI64,
    pub last_rate_limited_at: AtomicI64,
    pub last_timeout_at: AtomicI64,
    pub last_server_error_at: AtomicI64,

    // 数据库时间（RFC3339 字符串，list_accounts 用）
    pub db_created_at: RwLock<String>,
    pub db_updated_at: RwLock<String>,

    // 创建时间
    pub created_at: Instant,
}

/// 滑动窗口 — 最近 20 次请求结果
#[derive(Debug, Clone)]
pub struct RecentWindow {
    pub results: [u8; 20],
    pub idx: usize,
    pub count: usize,
}

impl Default for RecentWindow {
    fn default() -> Self {
        Self {
            results: [0; 20],
            idx: 0,
            count: 0,
        }
    }
}

impl RecentWindow {
    pub fn push(&mut self, success: bool) {
        self.results[self.idx] = if success { 1 } else { 0 };
        self.idx = (self.idx + 1) % 20;
        if self.count < 20 {
            self.count += 1;
        }
    }

    pub fn success_rate(&self) -> f64 {
        if self.count == 0 {
            return 1.0;
        }
        let successes = self.results[..self.count].iter().filter(|&&v| v == 1).count();
        successes as f64 / self.count as f64
    }
}

impl Account {
    pub fn new(db_id: i64) -> Self {
        Self {
            db_id,
            email: RwLock::new(String::new()),
            plan_type: RwLock::new(String::new()),
            proxy_url: RwLock::new(String::new()),
            codex_account_id: RwLock::new(String::new()),
            access_token: RwLock::new(String::new()),
            refresh_token: RwLock::new(String::new()),
            expires_at: RwLock::new(Utc::now()),
            active_requests: AtomicI64::new(0),
            total_requests: AtomicU64::new(0),
            health_tier: AtomicU8::new(TIER_HEALTHY),
            score: AtomicI64::new(10000), // 100.00
            dynamic_concurrency_limit: AtomicI64::new(2),
            cooldown_until: AtomicI64::new(0),
            latency_ewma_100: AtomicI64::new(0),
            success_streak: AtomicI64::new(0),
            failure_streak: AtomicI64::new(0),
            recent_results: RwLock::new(RecentWindow::default()),
            usage_7d_pct_100: AtomicI64::new(0),
            usage_5h_pct_100: AtomicI64::new(0),
            last_success_at: AtomicI64::new(0),
            last_failure_at: AtomicI64::new(0),
            last_unauthorized_at: AtomicI64::new(0),
            last_rate_limited_at: AtomicI64::new(0),
            last_timeout_at: AtomicI64::new(0),
            last_server_error_at: AtomicI64::new(0),
            db_created_at: RwLock::new(Utc::now().to_rfc3339()),
            db_updated_at: RwLock::new(Utc::now().to_rfc3339()),
            created_at: Instant::now(),
        }
    }

    /// 当前是否处于冷却期
    pub fn is_in_cooldown(&self) -> bool {
        let until = self.cooldown_until.load(Ordering::Relaxed);
        if until == 0 {
            return false;
        }
        Utc::now().timestamp() < until
    }

    /// 是否可用（非 banned、非冷却、有 token、未满并发）
    pub fn is_available(&self) -> bool {
        let tier = self.health_tier.load(Ordering::Relaxed);
        if tier == TIER_BANNED {
            return false;
        }
        if self.is_in_cooldown() {
            return false;
        }
        if self.access_token.read().is_empty() {
            return false;
        }
        let active = self.active_requests.load(Ordering::Relaxed);
        let limit = self.dynamic_concurrency_limit.load(Ordering::Relaxed);
        active < limit
    }

    /// 获取并发（CAS 递增 active_requests）
    pub fn try_acquire(&self) -> bool {
        loop {
            let current = self.active_requests.load(Ordering::Acquire);
            let limit = self.dynamic_concurrency_limit.load(Ordering::Relaxed);
            if current >= limit {
                return false;
            }
            if self
                .active_requests
                .compare_exchange_weak(current, current + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// 释放并发
    pub fn release(&self) {
        self.active_requests.fetch_sub(1, Ordering::Release);
    }

    /// 记录请求成功
    pub fn report_success(&self, latency_ms: u64) {
        let now = Utc::now().timestamp();
        self.last_success_at.store(now, Ordering::Relaxed);
        self.success_streak.fetch_add(1, Ordering::Relaxed);
        self.failure_streak.store(0, Ordering::Relaxed);
        self.total_requests.fetch_add(1, Ordering::Relaxed);

        // 更新 EWMA 延迟（alpha=0.3）
        let new_val = (latency_ms * 100) as i64;
        let old = self.latency_ewma_100.load(Ordering::Relaxed);
        let ewma = if old == 0 {
            new_val
        } else {
            (old * 70 + new_val * 30) / 100
        };
        self.latency_ewma_100.store(ewma, Ordering::Relaxed);

        self.recent_results.write().push(true);
    }

    /// 记录请求失败
    pub fn report_failure(&self, error_type: FailureType) {
        let now = Utc::now().timestamp();
        self.last_failure_at.store(now, Ordering::Relaxed);
        self.failure_streak.fetch_add(1, Ordering::Relaxed);
        self.success_streak.store(0, Ordering::Relaxed);
        self.total_requests.fetch_add(1, Ordering::Relaxed);

        match error_type {
            FailureType::Unauthorized => self.last_unauthorized_at.store(now, Ordering::Relaxed),
            FailureType::RateLimited => self.last_rate_limited_at.store(now, Ordering::Relaxed),
            FailureType::Timeout => self.last_timeout_at.store(now, Ordering::Relaxed),
            FailureType::ServerError => self.last_server_error_at.store(now, Ordering::Relaxed),
            FailureType::Other => {}
        }

        self.recent_results.write().push(false);
    }
}

#[derive(Debug, Clone, Copy)]
pub enum FailureType {
    Unauthorized,
    RateLimited,
    Timeout,
    ServerError,
    Other,
}

// ─── 调度器 ───

pub struct Scheduler {
    /// 所有账号（包括 banned 的，方便恢复探测）
    pub accounts: RwLock<Vec<Arc<Account>>>,
    /// 分桶索引：按 tier 分组的账号索引
    pub tier_buckets: RwLock<TierBuckets>,
    /// 通知：有账号变为可用时唤醒等待者
    pub available_notify: Notify,
    /// 最大并发配置
    pub max_concurrency: AtomicI64,
}

/// 分桶结构 — 定期重建
pub struct TierBuckets {
    pub healthy: Vec<usize>,  // 指向 accounts 的索引
    pub warm: Vec<usize>,
    pub risky: Vec<usize>,
    pub cursors: [AtomicU64; 3], // 每个桶的 round-robin 游标
}

impl Default for TierBuckets {
    fn default() -> Self {
        Self {
            healthy: Vec::new(),
            warm: Vec::new(),
            risky: Vec::new(),
            cursors: [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)],
        }
    }
}

impl Scheduler {
    pub fn new(max_concurrency: i64) -> Self {
        Self {
            accounts: RwLock::new(Vec::new()),
            tier_buckets: RwLock::new(TierBuckets::default()),
            available_notify: Notify::new(),
            max_concurrency: AtomicI64::new(max_concurrency),
        }
    }

    /// 添加账号到调度器
    pub fn add_account(&self, account: Arc<Account>) {
        let max_c = self.max_concurrency.load(Ordering::Relaxed);
        account.dynamic_concurrency_limit.store(max_c, Ordering::Relaxed);
        self.accounts.write().push(account);
        self.rebuild_buckets();
    }

    /// 移除账号
    pub fn remove_account(&self, db_id: i64) {
        self.accounts.write().retain(|a| a.db_id != db_id);
        self.rebuild_buckets();
    }

    /// 根据当前 health_tier 重建分桶索引
    pub fn rebuild_buckets(&self) {
        let accounts = self.accounts.read();
        let mut healthy = Vec::new();
        let mut warm = Vec::new();
        let mut risky = Vec::new();

        for (idx, acc) in accounts.iter().enumerate() {
            match acc.health_tier.load(Ordering::Relaxed) {
                TIER_HEALTHY => healthy.push(idx),
                TIER_WARM => warm.push(idx),
                TIER_RISKY => risky.push(idx),
                _ => {} // banned 不入桶
            }
        }

        let mut buckets = self.tier_buckets.write();
        buckets.healthy = healthy;
        buckets.warm = warm;
        buckets.risky = risky;
    }

    /// 获取账号引用
    pub fn get_account(&self, db_id: i64) -> Option<Arc<Account>> {
        self.accounts
            .read()
            .iter()
            .find(|a| a.db_id == db_id)
            .cloned()
    }

    /// 获取所有账号快照
    pub fn all_accounts(&self) -> Vec<Arc<Account>> {
        self.accounts.read().clone()
    }

    /// 可用账号数
    pub fn available_count(&self) -> usize {
        self.accounts
            .read()
            .iter()
            .filter(|a| a.is_available())
            .count()
    }

    /// 总账号数（不含 banned）
    pub fn active_count(&self) -> usize {
        self.accounts
            .read()
            .iter()
            .filter(|a| a.health_tier.load(Ordering::Relaxed) != TIER_BANNED)
            .count()
    }

    /// 通知等待者：有账号变为可用
    pub fn notify_available(&self) {
        self.available_notify.notify_waiters();
    }
}
