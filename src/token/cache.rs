use dashmap::DashMap;
use std::time::{Duration, Instant};

/// 内存 Token 缓存 — 线程安全、无锁读取
pub struct TokenCache {
    /// key: account_id → (access_token, expires_at)
    tokens: DashMap<i64, CacheEntry>,
    /// 刷新锁：account_id → 锁定时刻
    refresh_locks: DashMap<i64, Instant>,
}

struct CacheEntry {
    access_token: String,
    expires_at: Instant,
}

impl TokenCache {
    pub fn new() -> Self {
        Self {
            tokens: DashMap::new(),
            refresh_locks: DashMap::new(),
        }
    }

    /// 获取缓存的 Token（未过期才返回）
    pub fn get(&self, account_id: i64) -> Option<String> {
        let entry = self.tokens.get(&account_id)?;
        if Instant::now() < entry.expires_at {
            Some(entry.access_token.clone())
        } else {
            drop(entry);
            self.tokens.remove(&account_id);
            None
        }
    }

    /// 设置 Token 缓存
    pub fn set(&self, account_id: i64, token: String, ttl: Duration) {
        self.tokens.insert(
            account_id,
            CacheEntry {
                access_token: token,
                expires_at: Instant::now() + ttl,
            },
        );
    }

    /// 删除 Token
    pub fn remove(&self, account_id: i64) {
        self.tokens.remove(&account_id);
    }

    /// 尝试获取刷新锁（防止重复刷新）
    pub fn acquire_refresh_lock(&self, account_id: i64, ttl: Duration) -> bool {
        let now = Instant::now();
        // 检查是否已有锁
        if let Some(locked_at) = self.refresh_locks.get(&account_id) {
            if now < *locked_at + ttl {
                return false; // 锁未过期
            }
        }
        // 设置锁
        self.refresh_locks.insert(account_id, now);
        true
    }

    /// 释放刷新锁
    pub fn release_refresh_lock(&self, account_id: i64) {
        self.refresh_locks.remove(&account_id);
    }

    /// 缓存条目数量
    pub fn len(&self) -> usize {
        self.tokens.len()
    }

    /// 清理过期条目
    pub fn cleanup_expired(&self) {
        let now = Instant::now();
        self.tokens.retain(|_, entry| now < entry.expires_at);
        // 清理超过 60 秒的锁
        self.refresh_locks
            .retain(|_, locked_at| now.duration_since(*locked_at) < Duration::from_secs(60));
    }
}
