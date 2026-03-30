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
    pub db: DbPool,
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
