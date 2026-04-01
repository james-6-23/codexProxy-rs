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
use tracing::{error, info, warn};

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

    // 初始化数据库（先小池读配置，再按配置建正式池）
    let boot_pool = db::init(&config.database_url, 2)
        .await
        .expect("数据库初始化失败");

    // 加载系统设置
    let settings = db::queries::get_system_settings(&boot_pool)
        .await
        .expect("加载系统设置失败");

    // 用 pg_max_conns 创建正式连接池
    let pool_size = if settings.pg_max_conns > 0 { settings.pg_max_conns as u32 } else { config.db_pool_size };
    let db_pool = if pool_size > 2 {
        boot_pool.close().await;
        db::init(&config.database_url, pool_size)
            .await
            .expect("数据库初始化失败")
    } else {
        boot_pool
    };
    info!(max_concurrency = settings.max_concurrency, global_rpm = settings.global_rpm, pg_max_conns = pool_size, "系统设置已加载");

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

        // 恢复用量重置时间
        if !creds.codex_7d_reset_at.is_empty() {
            if let Ok(ts) = creds.codex_7d_reset_at.parse::<i64>() {
                account.resets_at.store(ts, std::sync::atomic::Ordering::Relaxed);
            }
        }

        scheduler.add_account(account);
    }
    info!(count = loaded_count, "已加载账号到调度器");

    // 从数据库恢复请求计数（跨重启保持一致）
    if let Ok(counts) = db::queries::get_account_request_counts(&db_pool).await {
        for acc in scheduler.all_accounts() {
            if let Some(&(total, errors)) = counts.get(&acc.db_id) {
                acc.total_requests.store(total, std::sync::atomic::Ordering::Relaxed);
                acc.error_requests.store(errors, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    // 限流器
    let rate_limiter = RateLimiter::new(settings.global_rpm as i64);

    // 使用日志异步写入通道
    let (log_tx, log_rx) = tokio::sync::mpsc::channel::<UsageLog>(10000);

    // 全局状态
    let state = Arc::new(AppState::new(
        config.clone(),
        db_pool.clone(),
        scheduler,
        rate_limiter,
        log_tx,
        settings,
    ));

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
        .route("/api/admin/accounts/batch-refresh", post(admin::handler::batch_refresh))
        .route("/api/admin/accounts/{id}/test", get(admin::handler::test_connection))
        .route("/api/admin/accounts/{id}/usage", get(admin::handler::account_usage))
        .route("/api/admin/accounts/batch-test", post(admin::handler::batch_test))
        .route("/api/admin/accounts/clean-banned", post(admin::handler::clean_banned))
        .route("/api/admin/accounts/clean-rate-limited", post(admin::handler::clean_rate_limited))
        .route("/api/admin/accounts/clean-error", post(admin::handler::clean_error))
        .route("/api/admin/accounts/event-trend", get(admin::handler::account_event_trend))
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
    let db = state.db();
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

    // 9. 用量重置倒计时检查（每 30 秒）
    let state9 = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            check_usage_reset(&state9).await;
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
        let db = state.db();

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
            let _ = db::queries::delete_account(&state.db(), acc.db_id).await;
            state.scheduler.remove_account(acc.db_id);
            db::queries::insert_account_event(&state.db(), acc.db_id, "deleted", "auto_clean").await;
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
            let _ = db::queries::delete_account(&state.db(), acc.db_id).await;
            state.scheduler.remove_account(acc.db_id);
            db::queries::insert_account_event(&state.db(), acc.db_id, "deleted", "clean_full_usage").await;
            cleaned += 1;
        }
    }

    if cleaned > 0 {
        info!(cleaned, "用量满清理完成");
    }
}

/// 过期账号清理（15 分钟）— 加入号池超过 30 分钟且未被充分验证的账号
async fn auto_cleanup_expired(state: &AppState) {
    let enabled = state.db_settings_cache.read()
        .map(|s| s.auto_clean_expired)
        .unwrap_or(false);
    if !enabled {
        return;
    }

    let accounts = state.scheduler.all_accounts();
    let cutoff = std::time::Instant::now() - std::time::Duration::from_secs(30 * 60);
    let mut cleaned = 0u32;

    for acc in &accounts {
        // 加入时间未超过 30 分钟 → 跳过
        if acc.created_at > cutoff {
            continue;
        }
        // 正在处理请求 → 跳过
        if acc.active_requests.load(std::sync::atomic::Ordering::Relaxed) > 0 {
            continue;
        }
        // 已验证账号（成功请求 > 10 次）→ 跳过
        if acc.total_requests.load(std::sync::atomic::Ordering::Relaxed) > 10 {
            continue;
        }

        let _ = db::queries::delete_account(&state.db(), acc.db_id).await;
        state.scheduler.remove_account(acc.db_id);
        db::queries::insert_account_event(&state.db(), acc.db_id, "deleted", "clean_expired").await;
        cleaned += 1;
    }

    if cleaned > 0 {
        info!(cleaned, "过期账号清理完成");
    }
}

/// 用量重置检查 — 倒计时到期后发单次探针确认
///
/// 安全策略：
/// 1. 每 30s 检查 resets_at 倒计时，未到期的绝不发请求
/// 2. 到期后仅发一次最小探针（使用 test_model）
/// 3. 200 → 恢复账号；429 → 记录新 resets_at 继续等待
/// 4. 探针失败不改变账号状态，等下个周期重试
async fn check_usage_reset(state: &AppState) {
    let accounts = state.scheduler.all_accounts();
    let now = chrono::Utc::now().timestamp();

    // 收集到期账号（避免在遍历中持有锁太久）
    let due: Vec<_> = accounts
        .iter()
        .filter(|acc| {
            let ts = acc.resets_at.load(std::sync::atomic::Ordering::Relaxed);
            ts > 0 && ts <= now
        })
        .collect();

    if due.is_empty() {
        return;
    }

    let test_model = state.db_settings_cache.read()
        .map(|s| s.test_model.clone())
        .unwrap_or_else(|_| "gpt-5.4-mini".to_string());

    for acc in due {
        probe_and_recover(state, acc, &test_model).await;
    }
}

/// 对单个到期账号发探针确认，根据结果决定恢复或继续等待
async fn probe_and_recover(state: &AppState, acc: &Arc<Account>, model: &str) {
    let access_token = acc.access_token.read().clone();
    if access_token.is_empty() {
        return;
    }

    let proxy_url = acc.proxy_url.read().clone();
    let codex_account_id = acc.codex_account_id.read().clone();
    let account_id_str = acc.db_id.to_string();

    // 最小探针：stream=false、store=false、最短 prompt
    let payload = serde_json::json!({
        "model": model,
        "input": [{"role": "user", "content": [{"type": "input_text", "text": "hi"}]}],
        "stream": false,
        "store": false,
        "instructions": "",
    });

    let upstream_url = format!("{}/responses", proxy::UPSTREAM_BASE);
    let ua = proxy::useragent::ua_for_account(&account_id_str);
    let version = proxy::useragent::version_from_ua(ua);
    let client = proxy::handler::get_or_create_client(state, &proxy_url);

    let mut req = client
        .post(&upstream_url)
        .header("Authorization", format!("Bearer {}", access_token))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .header("User-Agent", ua)
        .header("Version", version)
        .header("Originator", proxy::ORIGINATOR)
        .json(&payload)
        .timeout(Duration::from_secs(30));

    if !codex_account_id.is_empty() {
        req = req.header("Chatgpt-Account-Id", &codex_account_id);
    }

    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            // 网络失败 → 不动状态，下个周期重试
            warn!(account_id = acc.db_id, error = %e, "重置探针请求失败，稍后重试");
            return;
        }
    };

    let status = resp.status().as_u16();
    let resp_headers = resp.headers().clone();

    // 无论结果如何都刷新用量 header
    proxy::handler::update_usage_from_headers(acc, &resp_headers);

    match status {
        200 => {
            // 200 不代表用量一定恢复 — 检查响应头中的实际用量
            let usage_7d = acc.usage_7d_pct_100.load(std::sync::atomic::Ordering::Relaxed);
            let usage_5h = acc.usage_5h_pct_100.load(std::sync::atomic::Ordering::Relaxed);

            if usage_7d >= 10000 || usage_5h >= 10000 {
                // 用量仍 ≥ 100% — 不恢复，30 分钟后再探测
                let retry_at = chrono::Utc::now().timestamp() + 1800;
                acc.resets_at.store(retry_at, std::sync::atomic::Ordering::Relaxed);
                warn!(
                    account_id = acc.db_id,
                    usage_7d = usage_7d as f64 / 100.0,
                    usage_5h = usage_5h as f64 / 100.0,
                    "重置探针 200 但用量仍满 — 30 分钟后重试"
                );
                return;
            }

            // 用量 < 100% — 确认恢复
            acc.resets_at.store(0, std::sync::atomic::Ordering::Relaxed);
            acc.usage_7d_pct_100.store(0, std::sync::atomic::Ordering::Relaxed);
            acc.usage_5h_pct_100.store(0, std::sync::atomic::Ordering::Relaxed);
            state.scheduler.try_recover(acc);

            let db = state.db();
            let aid = acc.db_id;
            tokio::spawn(async move {
                let _ = db::queries::clear_account_usage_state(&db, aid).await;
            });

            let email = acc.email.read().clone();
            info!(account_id = acc.db_id, email = %email, "重置探针 200 — 账号已恢复调度");
        }
        429 => {
            // 仍然限流 — 从新响应更新 resets_at
            let body = resp.text().await.unwrap_or_default();
            if let Ok(body_json) = serde_json::from_str::<serde_json::Value>(&body) {
                if let Some(ts) = body_json.pointer("/error/resets_at").and_then(|v| v.as_i64()) {
                    acc.resets_at.store(ts, std::sync::atomic::Ordering::Relaxed);
                    let db = state.db();
                    let aid = acc.db_id;
                    tokio::spawn(async move {
                        let _ = db::queries::update_account_resets_at(&db, aid, ts).await;
                    });
                    warn!(account_id = acc.db_id, resets_at = ts, "重置探针 429 — 已更新下次重置时间");
                    return;
                }
            }
            // 429 但没有新的 resets_at → 30 分钟后再试
            let retry_at = chrono::Utc::now().timestamp() + 1800;
            acc.resets_at.store(retry_at, std::sync::atomic::Ordering::Relaxed);
            warn!(account_id = acc.db_id, "重置探针 429 无 resets_at — 30 分钟后重试");
        }
        401 => {
            // Token 失效 — 标记 banned，清除探针
            acc.resets_at.store(0, std::sync::atomic::Ordering::Relaxed);
            state.scheduler.mark_banned(acc);
            let db = state.db();
            let aid = acc.db_id;
            tokio::spawn(async move {
                let _ = db::queries::update_account_resets_at(&db, aid, 0).await;
            });
            warn!(account_id = acc.db_id, "重置探针 401 — 标记 banned");
        }
        _ => {
            // 其他错误 → 不动状态，下个周期重试
            warn!(account_id = acc.db_id, status, "重置探针异常状态码，稍后重试");
        }
    }
}
