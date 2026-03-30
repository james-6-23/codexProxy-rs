mod admin;
mod config;
mod db;
mod proxy;
mod scheduler;
mod state;
mod token;

use std::sync::Arc;
use std::time::Duration;

use axum::extract::Path;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post, put};
use axum::Router;
use tower_http::cors::CorsLayer;
use tracing::{error, info};

use crate::config::AppConfig;
use crate::db::models::UsageLog;
use crate::proxy::ratelimit::RateLimiter;
use crate::scheduler::{Account, Scheduler};
use crate::state::AppState;
use crate::token::cache::TokenCache;

#[tokio::main]
async fn main() {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,tower_http=debug".parse().unwrap()),
        )
        .init();

    // 加载 .env
    let _ = dotenvy::dotenv();

    // 加载配置
    let config = AppConfig::from_env();
    info!(port = config.port, "启动 codex-proxy");

    // 初始化数据库
    let db_pool = db::init(&config.database_url, config.db_pool_size)
        .await
        .expect("数据库初始化失败");

    // 加载系统设置
    let settings = db::queries::get_system_settings(&db_pool)
        .await
        .expect("加载系统设置失败");
    info!(max_concurrency = settings.max_concurrency, global_rpm = settings.global_rpm, "系统设置已加载");

    // 初始化调度器
    let scheduler = Scheduler::new(settings.max_concurrency as i64);

    // 从数据库加载现有账号
    let db_accounts = db::queries::list_active_accounts(&db_pool).await.unwrap_or_default();
    let loaded_count = db_accounts.len();
    for row in db_accounts {
        let creds: db::models::Credentials =
            serde_json::from_str(&row.credentials).unwrap_or_default();

        let account = Arc::new(Account::new(row.id));
        *account.email.write() = creds.email;
        *account.plan_type.write() = creds.plan_type;
        *account.proxy_url.write() = row.proxy_url;
        *account.codex_account_id.write() = creds.account_id;
        *account.access_token.write() = creds.access_token;
        *account.refresh_token.write() = creds.refresh_token;

        // 缓存 DB 时间（list_accounts 直接从内存读取，不再每次查库）
        *account.db_created_at.write() = row.created_at;
        *account.db_updated_at.write() = row.updated_at;

        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&creds.expires_at) {
            *account.expires_at.write() = dt.with_timezone(&chrono::Utc);
        }

        // 恢复用量数据
        account.usage_7d_pct_100.store(
            (creds.codex_7d_used_percent * 100.0) as i64,
            std::sync::atomic::Ordering::Relaxed,
        );
        account.usage_5h_pct_100.store(
            (creds.codex_5h_used_percent * 100.0) as i64,
            std::sync::atomic::Ordering::Relaxed,
        );

        scheduler.add_account(account);
    }
    info!(count = loaded_count, "已加载账号到调度器");

    // 限流器
    let rate_limiter = RateLimiter::new(settings.global_rpm as i64);

    // 使用日志异步写入通道
    let (log_tx, log_rx) = tokio::sync::mpsc::channel::<UsageLog>(10000);

    // 全局状态
    let state = Arc::new(AppState {
        config: config.clone(),
        db: db_pool.clone(),
        scheduler,
        rate_limiter,
        token_cache: TokenCache::new(),
        log_sender: log_tx,
        settings: tokio::sync::RwLock::new(settings.clone()),
        db_settings_cache: std::sync::RwLock::new(settings.clone()),
        start_time: std::time::Instant::now(),
        http_clients: dashmap::DashMap::new(),
    });

    // 启动后台任务
    spawn_background_tasks(state.clone(), log_rx);

    // 构建路由
    let app = build_router(state.clone());

    // 启动服务器
    let addr = format!("0.0.0.0:{}", config.port);
    info!(%addr, "HTTP 服务器启动");

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("绑定地址失败");

    axum::serve(listener, app)
        .await
        .expect("服务器运行失败");
}

/// 构建 axum 路由
fn build_router(state: Arc<AppState>) -> Router {
    let cors = CorsLayer::permissive();

    // 代理 API
    let proxy_routes = Router::new()
        .route("/v1/chat/completions", post(proxy::handler::chat_completions))
        .route("/v1/responses", post(proxy::handler::responses))
        .route("/v1/models", get(proxy::handler::list_models));

    // 管理 API — 匹配前端 api.ts 的全部端点
    let admin_routes = Router::new()
        // 健康 & 统计
        .route("/api/admin/health", get(admin::handler::health))
        .route("/api/admin/stats", get(admin::handler::stats))
        // 账号管理
        .route("/api/admin/accounts", get(admin::handler::list_accounts))
        .route("/api/admin/accounts", post(admin::handler::add_account))
        .route("/api/admin/accounts/at", post(admin::handler::add_at_account))
        .route("/api/admin/accounts/batch", post(admin::handler::batch_import))
        .route("/api/admin/accounts/import", post(admin::handler::import_accounts))
        .route("/api/admin/accounts/{id}", delete(admin::handler::delete_account))
        .route("/api/admin/accounts/batch-delete", post(admin::handler::batch_delete_accounts))
        .route("/api/admin/accounts/{id}/refresh", post(admin::handler::refresh_account))
        .route("/api/admin/accounts/{id}/test", get(admin::handler::test_connection))
        .route("/api/admin/accounts/{id}/usage", get(admin::handler::account_usage))
        .route("/api/admin/accounts/batch-test", post(admin::handler::batch_test))
        .route("/api/admin/accounts/clean-banned", post(admin::handler::clean_banned))
        .route("/api/admin/accounts/clean-rate-limited", post(admin::handler::clean_rate_limited))
        .route("/api/admin/accounts/clean-error", post(admin::handler::clean_error))
        // 使用统计
        .route("/api/admin/usage/stats", get(admin::handler::usage_stats))
        .route("/api/admin/usage/logs", get(admin::handler::usage_logs))
        .route("/api/admin/usage/logs", delete(admin::handler::clear_usage_logs))
        .route("/api/admin/usage/chart-data", get(admin::handler::chart_data))
        // 运维
        .route("/api/admin/ops/overview", get(admin::handler::ops_overview))
        // 设置
        .route("/api/admin/settings", get(admin::handler::get_settings))
        .route("/api/admin/settings", put(admin::handler::update_settings))
        // API Keys
        .route("/api/admin/keys", get(admin::handler::list_keys))
        .route("/api/admin/keys", post(admin::handler::create_key))
        .route("/api/admin/keys/{id}", delete(admin::handler::delete_key))
        // 模型列表
        .route("/api/admin/models", get(admin::handler::list_models));

    // 健康检查（根路径）
    let health = Router::new().route("/health", get(|| async { "ok" }));

    // 前端静态文件 — /admin/ 路径下的所有请求由嵌入的前端处理
    let frontend = Router::new()
        .route("/admin", get(serve_frontend_index))
        .route("/admin/", get(serve_frontend_index))
        .route("/admin/{*path}", get(serve_frontend));

    Router::new()
        .merge(proxy_routes)
        .merge(admin_routes)
        .merge(health)
        .merge(frontend)
        .layer(cors)
        .with_state(state)
}

// ─── 前端静态文件服务 ───

// 使用 include_dir 在编译时嵌入前端产物
use include_dir::{include_dir, Dir};

static FRONTEND_DIR: Dir = include_dir!("$CARGO_MANIFEST_DIR/frontend/dist");

async fn serve_frontend_index() -> impl IntoResponse {
    serve_frontend_file("index.html")
}

async fn serve_frontend(Path(path): Path<String>) -> impl IntoResponse {
    // 先尝试精确匹配文件
    if let Some(resp) = try_serve_file(&path) {
        return resp;
    }
    // SPA fallback: 非文件路径都返回 index.html
    serve_frontend_file("index.html")
}

fn try_serve_file(path: &str) -> Option<axum::response::Response> {
    let file = FRONTEND_DIR.get_file(path)?;
    let mime = mime_from_path(path);
    Some(
        axum::response::Response::builder()
            .status(200)
            .header("Content-Type", mime)
            .header("Cache-Control", "public, max-age=31536000, immutable")
            .body(axum::body::Body::from(file.contents().to_vec()))
            .unwrap(),
    )
}

fn serve_frontend_file(path: &str) -> axum::response::Response {
    match FRONTEND_DIR.get_file(path) {
        Some(file) => {
            let mime = mime_from_path(path);
            axum::response::Response::builder()
                .status(200)
                .header("Content-Type", mime)
                .body(axum::body::Body::from(file.contents().to_vec()))
                .unwrap()
        }
        None => axum::response::Response::builder()
            .status(404)
            .body(axum::body::Body::from("Not Found"))
            .unwrap(),
    }
}

fn mime_from_path(path: &str) -> &'static str {
    if path.ends_with(".html") {
        "text/html; charset=utf-8"
    } else if path.ends_with(".js") {
        "application/javascript; charset=utf-8"
    } else if path.ends_with(".css") {
        "text/css; charset=utf-8"
    } else if path.ends_with(".json") {
        "application/json"
    } else if path.ends_with(".png") {
        "image/png"
    } else if path.ends_with(".svg") {
        "image/svg+xml"
    } else if path.ends_with(".ico") {
        "image/x-icon"
    } else if path.ends_with(".woff2") {
        "font/woff2"
    } else if path.ends_with(".woff") {
        "font/woff"
    } else {
        "application/octet-stream"
    }
}

/// 启动后台任务
fn spawn_background_tasks(
    state: Arc<AppState>,
    mut log_rx: tokio::sync::mpsc::Receiver<UsageLog>,
) {
    // 1. 使用日志批量写入
    let db = state.db.clone();
    tokio::spawn(async move {
        let mut buffer: Vec<UsageLog> = Vec::with_capacity(64);
        // 使用 interval 而非 sleep — interval 不会因 recv 分支触发而重置
        let mut flush_tick = tokio::time::interval(Duration::from_secs(2));
        flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                Some(log) = log_rx.recv() => {
                    buffer.push(log);
                    if buffer.len() >= 64 {
                        if let Err(e) = db::queries::batch_insert_usage_logs(&db, &buffer).await {
                            error!("批量写入日志失败: {}", e);
                        }
                        buffer.clear();
                    }
                }
                _ = flush_tick.tick() => {
                    if !buffer.is_empty() {
                        if let Err(e) = db::queries::batch_insert_usage_logs(&db, &buffer).await {
                            error!("批量写入日志失败: {}", e);
                        }
                        buffer.clear();
                    }
                }
            }
        }
    });

    // 2. Token 定时刷新（每 2 分钟）
    let state2 = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(120));
        let client = reqwest::Client::new();
        loop {
            interval.tick().await;
            refresh_expiring_tokens(&state2, &client).await;
        }
    });

    // 3. 健康状态定期重算 + 分桶重建（每分钟）
    let state3 = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            state3.scheduler.recompute_all();
        }
    });

    // 4. Token 缓存清理（每 5 分钟）
    let state4 = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(300));
        loop {
            interval.tick().await;
            state4.token_cache.cleanup_expired();
        }
    });

    // 5. 恢复探测（每 2 分钟检查 banned 账号）
    let state5 = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(120));
        let client = reqwest::Client::new();
        loop {
            interval.tick().await;
            probe_recovery(&state5, &client).await;
        }
    });

    // 6. 自动清理巡检（每 30 秒 — 401/429/error）
    let state6 = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            auto_cleanup_sweep(&state6).await;
        }
    });

    // 7. 用量满账号清理（每 5 分钟）
    let state7 = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(300));
        loop {
            interval.tick().await;
            auto_cleanup_full_usage(&state7).await;
        }
    });

    // 8. 过期账号清理（每 15 分钟）
    let state8 = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(900));
        loop {
            interval.tick().await;
            auto_cleanup_expired(&state8).await;
        }
    });
}

/// 刷新即将过期的 Token
async fn refresh_expiring_tokens(state: &AppState, client: &reqwest::Client) {
    let accounts = state.scheduler.all_accounts();
    let now = chrono::Utc::now();
    let threshold = now + chrono::Duration::minutes(5);

    let semaphore = Arc::new(tokio::sync::Semaphore::new(10));
    let mut handles = Vec::new();

    for acc in accounts {
        let expires = *acc.expires_at.read();
        let rt = acc.refresh_token.read().clone();

        // 只刷新即将过期且有 RT 的账号
        if rt.is_empty() || expires > threshold {
            continue;
        }

        // 检查缓存锁
        if !state.token_cache.acquire_refresh_lock(acc.db_id, Duration::from_secs(30)) {
            continue;
        }

        let client = client.clone();
        let sem = semaphore.clone();
        let db = state.db.clone();

        let acc_clone = acc.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            match token::refresh::refresh_with_retry(&client, &rt).await {
                Ok(resp) => {
                    let info = token::parse_id_token(&resp.id_token).unwrap_or_default();
                    let new_expires = chrono::Utc::now()
                        + chrono::Duration::seconds(resp.expires_in);

                    *acc_clone.access_token.write() = resp.access_token.clone();
                    if !resp.refresh_token.is_empty() {
                        *acc_clone.refresh_token.write() = resp.refresh_token.clone();
                    }
                    *acc_clone.expires_at.write() = new_expires;
                    if !info.email.is_empty() {
                        *acc_clone.email.write() = info.email.clone();
                    }
                    if !info.chatgpt_plan_type.is_empty() {
                        *acc_clone.plan_type.write() = info.chatgpt_plan_type.clone();
                    }

                    // 更新数据库
                    let creds = db::models::Credentials {
                        refresh_token: acc_clone.refresh_token.read().clone(),
                        access_token: resp.access_token,
                        id_token: resp.id_token,
                        expires_at: new_expires.to_rfc3339(),
                        email: info.email,
                        account_id: info.chatgpt_account_id,
                        plan_type: info.chatgpt_plan_type,
                        ..Default::default()
                    };
                    let _ = db::queries::update_account_credentials(&db, acc_clone.db_id, &creds).await;

                    info!(account_id = acc_clone.db_id, "Token 刷新成功");
                }
                Err(e) => {
                    error!(account_id = acc_clone.db_id, error = %e, "Token 刷新失败");
                }
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }
}

/// 探测 banned 账号是否恢复
async fn probe_recovery(state: &AppState, client: &reqwest::Client) {
    let accounts = state.scheduler.all_accounts();

    for acc in &accounts {
        let tier = acc.health_tier.load(std::sync::atomic::Ordering::Relaxed);
        if tier != scheduler::TIER_BANNED {
            continue;
        }

        let rt = acc.refresh_token.read().clone();
        if rt.is_empty() {
            // AT-only 账号：无法刷新也无法探测，直接跳过
            continue;
        }

        match token::refresh::refresh_access_token(client, &rt).await {
            Ok(resp) => {
                let new_expires = chrono::Utc::now()
                    + chrono::Duration::seconds(resp.expires_in);
                *acc.access_token.write() = resp.access_token;
                if !resp.refresh_token.is_empty() {
                    *acc.refresh_token.write() = resp.refresh_token;
                }
                *acc.expires_at.write() = new_expires;

                state.scheduler.try_recover(acc);
                info!(account_id = acc.db_id, "Banned 账号恢复成功");
            }
            Err(_) => {}
        }
    }
}

/// 自动清理巡检（30s）— 401 / 429 / error
async fn auto_cleanup_sweep(state: &AppState) {
    let settings = state.db_settings_cache.read().unwrap().clone();

    if !settings.auto_clean_unauthorized
        && !settings.auto_clean_rate_limited
        && !settings.auto_clean_error
    {
        return;
    }

    let accounts = state.scheduler.all_accounts();
    let mut cleaned = 0u32;

    for acc in &accounts {
        let tier = acc.health_tier.load(std::sync::atomic::Ordering::Relaxed);
        let should_clean = match tier {
            // BANNED（401）
            scheduler::TIER_BANNED if settings.auto_clean_unauthorized => {
                acc.last_unauthorized_at.load(std::sync::atomic::Ordering::Relaxed) > 0
            }
            // RISKY（多次失败 / error）
            scheduler::TIER_RISKY if settings.auto_clean_error => true,
            _ => false,
        };

        // 429 rate_limited — 处于冷却期的账号
        let rate_limited_clean = settings.auto_clean_rate_limited && acc.is_in_cooldown();

        if should_clean || rate_limited_clean {
            let _ = db::queries::delete_account(&state.db, acc.db_id).await;
            state.scheduler.remove_account(acc.db_id);
            cleaned += 1;
        }
    }

    if cleaned > 0 {
        info!(cleaned, "自动清理完成");
    }
}

/// 用量满账号清理（5 分钟）— usage ≥ 100%
async fn auto_cleanup_full_usage(state: &AppState) {
    let enabled = state.db_settings_cache.read()
        .map(|s| s.auto_clean_full_usage)
        .unwrap_or(false);
    if !enabled {
        return;
    }

    let accounts = state.scheduler.all_accounts();
    let mut cleaned = 0u32;

    for acc in &accounts {
        // 跳过正在处理请求的账号
        if acc.active_requests.load(std::sync::atomic::Ordering::Relaxed) > 0 {
            continue;
        }

        // 7 天用量 ≥ 100%（存储为 pct * 100 的整数）
        let usage_7d = acc.usage_7d_pct_100.load(std::sync::atomic::Ordering::Relaxed);
        if usage_7d >= 10000 {
            let _ = db::queries::delete_account(&state.db, acc.db_id).await;
            state.scheduler.remove_account(acc.db_id);
            cleaned += 1;
            info!(account_id = acc.db_id, usage_pct = usage_7d as f64 / 100.0, "用量满，自动清理");
        }
    }

    if cleaned > 0 {
        info!(cleaned, "用量满清理完成");
    }
}

/// 过期账号清理（15 分钟）— AT 过期超过 30 分钟
async fn auto_cleanup_expired(state: &AppState) {
    let enabled = state.db_settings_cache.read()
        .map(|s| s.auto_clean_expired)
        .unwrap_or(false);
    if !enabled {
        return;
    }

    let accounts = state.scheduler.all_accounts();
    let now = chrono::Utc::now();
    let threshold = now - chrono::Duration::minutes(30);
    let mut cleaned = 0u32;

    for acc in &accounts {
        let rt = acc.refresh_token.read().clone();
        let expires = *acc.expires_at.read();

        // AT-only 账号（无 refresh_token）且已过期超过 30 分钟
        if rt.is_empty() && expires < threshold {
            let _ = db::queries::delete_account(&state.db, acc.db_id).await;
            state.scheduler.remove_account(acc.db_id);
            cleaned += 1;
        }
    }

    if cleaned > 0 {
        info!(cleaned, "过期账号清理完成");
    }
}
