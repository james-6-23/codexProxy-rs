use std::sync::RwLock;
use std::time::Instant;

use dashmap::DashMap;
use tokio::sync::mpsc;

use crate::config::AppConfig;
use crate::db::models::SystemSettings;
use crate::db::DbPool;
use crate::proxy::ratelimit::RateLimiter;
use crate::scheduler::Scheduler;
use crate::token::cache::TokenCache;

/// 全局共享状态
pub struct AppState {
    pub config: AppConfig,
    db: RwLock<DbPool>,
    pub scheduler: Scheduler,
    pub rate_limiter: RateLimiter,
    pub token_cache: TokenCache,
    pub log_sender: mpsc::Sender<crate::db::models::UsageLog>,
    pub settings: tokio::sync::RwLock<SystemSettings>,
    pub db_settings_cache: RwLock<SystemSettings>,
    pub start_time: Instant,
    /// HTTP 客户端池：按 proxy_url 复用（同一代理共享连接池和 TCP 连接）
    pub http_clients: DashMap<String, reqwest::Client>,
}

impl AppState {
    pub fn new(
        config: AppConfig,
        db: DbPool,
        scheduler: Scheduler,
        rate_limiter: RateLimiter,
        log_sender: mpsc::Sender<crate::db::models::UsageLog>,
        settings: SystemSettings,
    ) -> Self {
        Self {
            config,
            db: RwLock::new(db),
            scheduler,
            rate_limiter,
            token_cache: TokenCache::new(),
            log_sender,
            settings: tokio::sync::RwLock::new(settings.clone()),
            db_settings_cache: RwLock::new(settings),
            start_time: Instant::now(),
            http_clients: DashMap::new(),
        }
    }

    /// 获取数据库连接池（PgPool 是 Arc 包装，clone 零开销）
    pub fn db(&self) -> DbPool {
        self.db.read().unwrap().clone()
    }

    /// 替换连接池（用于动态修改 pg_max_conns）
    pub fn replace_db(&self, new_pool: DbPool) {
        *self.db.write().unwrap() = new_pool;
    }
}
