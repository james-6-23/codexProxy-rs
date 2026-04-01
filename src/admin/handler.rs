use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use futures::StreamExt;
use serde::Deserialize;
use serde_json::json;

use crate::db::models::*;
use crate::db::queries;
use crate::scheduler::{self, Account, tier_name};
use crate::state::AppState;
use crate::token;
use tracing::{info, warn};

// ─── 认证中间件 ───

pub fn verify_admin(state: &AppState, headers: &HeaderMap) -> Result<(), StatusCode> {
    let secret = if let Some(s) = &state.config.admin_secret {
        s.clone()
    } else {
        let settings = state.db_settings_cache.read().unwrap();
        if settings.admin_secret.is_empty() {
            return Ok(());
        }
        settings.admin_secret.clone()
    };

    let provided = headers
        .get("X-Admin-Key")
        .or_else(|| headers.get("authorization"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.strip_prefix("Bearer ").unwrap_or(s))
        .unwrap_or("");

    if provided == secret {
        Ok(())
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

// ─── 健康检查 ───

/// GET /api/admin/health — 前端 AuthGate 用来验证密钥
pub async fn health(State(state): State<Arc<AppState>>, headers: HeaderMap) -> impl IntoResponse {
    if let Err(_) = verify_admin(&state, &headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "unauthorized"})),
        )
            .into_response();
    }

    let total = state.scheduler.all_accounts().len();
    let available = state.scheduler.available_count();

    Json(json!({
        "status": "ok",
        "available": available,
        "total": total,
    }))
    .into_response()
}

// ─── 仪表盘统计 ───

/// GET /api/admin/stats → StatsResponse
pub async fn stats(State(state): State<Arc<AppState>>, headers: HeaderMap) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    let total = state.scheduler.all_accounts().len();
    let available = state.scheduler.available_count();
    let error_count = state
        .scheduler
        .all_accounts()
        .iter()
        .filter(|a| a.health_tier.load(Ordering::Relaxed) == scheduler::TIER_BANNED)
        .count();

    let today_requests = queries::count_today_requests(&state.db()).await.unwrap_or(0);

    Json(json!({
        "total": total,
        "available": available,
        "error": error_count,
        "today_requests": today_requests,
    }))
    .into_response()
}

// ─── 账号管理 ───

/// GET /api/admin/accounts → { accounts: AccountRow[] }
pub async fn list_accounts(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    let accounts = state.scheduler.all_accounts();
    let now = chrono::Utc::now().timestamp();
    let mut result = Vec::with_capacity(accounts.len());

    for acc in &accounts {
        let email = acc.email.read().clone();
        let plan = acc.plan_type.read().clone();
        let tier = acc.health_tier.load(Ordering::Relaxed);
        let score = acc.score.load(Ordering::Relaxed) as f64 / 100.0;
        let active = acc.active_requests.load(Ordering::Relaxed);
        let total = acc.total_requests.load(Ordering::Relaxed);
        let concurrency = acc.dynamic_concurrency_limit.load(Ordering::Relaxed);
        let latency = acc.latency_ewma_100.load(Ordering::Relaxed) as f64 / 100.0;
        let usage_7d = acc.usage_7d_pct_100.load(Ordering::Relaxed) as f64 / 100.0;
        let usage_5h = acc.usage_5h_pct_100.load(Ordering::Relaxed) as f64 / 100.0;
        let success_rate = acc.recent_results.read().success_rate();
        let rt_empty = acc.refresh_token.read().is_empty();

        let last_401 = acc.last_unauthorized_at.load(Ordering::Relaxed);
        let last_429 = acc.last_rate_limited_at.load(Ordering::Relaxed);
        let last_success = acc.last_success_at.load(Ordering::Relaxed);

        let status = if tier == scheduler::TIER_BANNED {
            "error"
        } else if acc.is_in_cooldown() {
            "rate_limited"
        } else {
            "active"
        };

        let error_requests = acc.error_requests.load(Ordering::Relaxed);
        let success_requests = total.saturating_sub(error_requests);

        let resets_at = acc.resets_at.load(Ordering::Relaxed);

        result.push(json!({
            "id": acc.db_id,
            "name": email,
            "email": email,
            "plan_type": plan,
            "status": status,
            "at_only": rt_empty,
            "health_tier": tier_name(tier),
            "scheduler_score": score,
            "dynamic_concurrency_limit": concurrency,
            "active_requests": active,
            "total_requests": total,
            "success_requests": success_requests,
            "error_requests": error_requests,
            "usage_percent_7d": usage_7d,
            "usage_percent_5h": usage_5h,
            "latency_ms": latency,
            "proxy_url": acc.proxy_url.read().clone(),
            "last_used_at": ts_to_rfc3339(last_success),
            "last_unauthorized_at": ts_to_rfc3339(last_401),
            "last_rate_limited_at": ts_to_rfc3339(last_429),
            "resets_at": ts_to_rfc3339(resets_at),
            "reset_7d_at": ts_to_rfc3339(resets_at),
            "created_at": acc.db_created_at.read().clone(),
            "updated_at": acc.db_updated_at.read().clone(),
        }));
    }

    Json(json!({"accounts": result})).into_response()
}

fn ts_to_rfc3339(ts: i64) -> Option<String> {
    if ts > 0 {
        chrono::DateTime::from_timestamp(ts, 0).map(|dt| dt.to_rfc3339())
    } else {
        None
    }
}

// ─── 添加账号 ───

#[derive(Deserialize)]
pub struct AddAccountRequest {
    pub refresh_token: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub proxy_url: String,
}

/// POST /api/admin/accounts
pub async fn add_account(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<AddAccountRequest>,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    let client = reqwest::Client::new();
    match token::refresh::refresh_with_retry(&client, &req.refresh_token).await {
        Ok(token_resp) => {
            let info = token::parse_id_token(&token_resp.id_token).unwrap_or_default();
            let expires_at =
                chrono::Utc::now() + chrono::Duration::seconds(token_resp.expires_in);

            let creds = Credentials {
                refresh_token: if token_resp.refresh_token.is_empty() {
                    req.refresh_token.clone()
                } else {
                    token_resp.refresh_token
                },
                access_token: token_resp.access_token.clone(),
                id_token: token_resp.id_token,
                expires_at: expires_at.to_rfc3339(),
                email: info.email.clone(),
                account_id: info.chatgpt_account_id.clone(),
                plan_type: info.chatgpt_plan_type.clone(),
                ..Default::default()
            };

            let name = if req.name.is_empty() {
                info.email.clone()
            } else {
                req.name
            };

            match queries::insert_account(&state.db(), &name, &creds, &req.proxy_url).await {
                Ok(id) => {
                    let account = Arc::new(Account::new(id));
                    *account.email.write() = info.email;
                    *account.plan_type.write() = info.chatgpt_plan_type;
                    *account.proxy_url.write() = req.proxy_url;
                    *account.access_token.write() = token_resp.access_token;
                    *account.refresh_token.write() = creds.refresh_token;
                    *account.expires_at.write() = expires_at;

                    let email_out = account.email.read().clone();
                    state.scheduler.add_account(account);
                    queries::insert_account_event(&state.db(), id, "added", "manual").await;

                    (
                        StatusCode::CREATED,
                        Json(json!({"message": "ok", "id": id, "email": email_out})),
                    )
                        .into_response()
                }
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": e.to_string()})),
                )
                    .into_response(),
            }
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": format!("Token 刷新失败: {}", e)})),
        )
            .into_response(),
    }
}

/// POST /api/admin/accounts/at
#[derive(Deserialize)]
pub struct AddATRequest {
    pub access_token: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub proxy_url: String,
}

pub async fn add_at_account(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<AddATRequest>,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    if req.access_token.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "access_token 是必填字段"})),
        )
            .into_response();
    }

    // 按行分割，支持批量添加
    let tokens: Vec<String> = req
        .access_token
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect();

    if tokens.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "未找到有效的 Access Token"})),
        )
            .into_response();
    }

    if tokens.len() > 500 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "单次最多添加 500 个账号"})),
        )
            .into_response();
    }

    let total = tokens.len();
    let success_count = Arc::new(std::sync::atomic::AtomicI64::new(0));
    let fail_count = Arc::new(std::sync::atomic::AtomicI64::new(0));
    let sem = Arc::new(tokio::sync::Semaphore::new(20));
    let req_name = req.name.clone();
    let proxy_url = req.proxy_url.clone();

    let mut handles = Vec::with_capacity(total);

    for (i, at) in tokens.into_iter().enumerate() {
        let sem = sem.clone();
        let state = state.clone();
        let success_count = success_count.clone();
        let fail_count = fail_count.clone();
        let req_name = req_name.clone();
        let proxy_url = proxy_url.clone();

        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();

            let info = token::parse_id_token(&at).unwrap_or_default();

            let expires_at = if info.expires_at > 0 {
                chrono::DateTime::from_timestamp(info.expires_at, 0)
                    .unwrap_or_else(|| chrono::Utc::now() + chrono::Duration::hours(1))
            } else {
                chrono::Utc::now() + chrono::Duration::hours(1)
            };

            let creds = Credentials {
                access_token: at.clone(),
                email: info.email.clone(),
                account_id: info.chatgpt_account_id.clone(),
                plan_type: info.chatgpt_plan_type.clone(),
                expires_at: expires_at.to_rfc3339(),
                ..Default::default()
            };

            let name = if req_name.is_empty() {
                if info.email.is_empty() {
                    format!("at-account-{}", i + 1)
                } else {
                    info.email.clone()
                }
            } else if total > 1 {
                format!("{}-{}", req_name, i + 1)
            } else {
                req_name.clone()
            };

            match queries::insert_at_account(&state.db(), &name, &creds, &proxy_url).await {
                Ok(id) => {
                    let account = Arc::new(Account::new(id));
                    *account.email.write() = info.email;
                    *account.plan_type.write() = info.chatgpt_plan_type;
                    *account.proxy_url.write() = proxy_url;
                    *account.access_token.write() = at;
                    *account.codex_account_id.write() = info.chatgpt_account_id;
                    *account.expires_at.write() = expires_at;
                    state.scheduler.add_account(account);
                    queries::insert_account_event(&state.db(), id, "added", "at").await;
                    success_count.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    tracing::warn!(index = i + 1, error = %e, "AT 账号添加失败");
                    fail_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    let success = success_count.load(Ordering::Relaxed);
    let failed = fail_count.load(Ordering::Relaxed);
    let msg = if failed > 0 {
        format!("成功添加 {} 个 AT 账号，{} 个失败", success, failed)
    } else {
        format!("成功添加 {} 个 AT 账号", success)
    };

    Json(json!({
        "message": msg,
        "success": success,
        "failed": failed,
    }))
    .into_response()
}

/// POST /api/admin/accounts/batch
#[derive(Deserialize)]
pub struct BatchImportRequest {
    pub refresh_tokens: Vec<String>,
    #[serde(default)]
    pub proxy_url: String,
}

pub async fn batch_import(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<BatchImportRequest>,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    let client = reqwest::Client::new();
    let semaphore = Arc::new(tokio::sync::Semaphore::new(10));
    let mut handles = Vec::new();

    for rt in req.refresh_tokens {
        let client = client.clone();
        let sem = semaphore.clone();
        let state = state.clone();
        let proxy_url = req.proxy_url.clone();

        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            match token::refresh::refresh_with_retry(&client, &rt).await {
                Ok(token_resp) => {
                    let info =
                        token::parse_id_token(&token_resp.id_token).unwrap_or_default();
                    let expires_at = chrono::Utc::now()
                        + chrono::Duration::seconds(token_resp.expires_in);

                    let creds = Credentials {
                        refresh_token: if token_resp.refresh_token.is_empty() {
                            rt.clone()
                        } else {
                            token_resp.refresh_token
                        },
                        access_token: token_resp.access_token.clone(),
                        id_token: token_resp.id_token,
                        expires_at: expires_at.to_rfc3339(),
                        email: info.email.clone(),
                        account_id: info.chatgpt_account_id.clone(),
                        plan_type: info.chatgpt_plan_type.clone(),
                        ..Default::default()
                    };

                    match queries::insert_account(&state.db(), &info.email, &creds, &proxy_url).await {
                        Ok(id) => {
                            let account = Arc::new(Account::new(id));
                            *account.email.write() = info.email.clone();
                            *account.plan_type.write() = info.chatgpt_plan_type;
                            *account.proxy_url.write() = proxy_url;
                            *account.access_token.write() = token_resp.access_token;
                            *account.refresh_token.write() = creds.refresh_token;
                            *account.expires_at.write() = expires_at;
                            state.scheduler.add_account(account);
                            queries::insert_account_event(&state.db(), id, "added", "batch_import").await;
                            json!({"email": info.email, "status": "ok", "id": id})
                        }
                        Err(e) => {
                            json!({"token": &rt[..rt.len().min(8)], "status": "error", "error": e.to_string()})
                        }
                    }
                }
                Err(e) => {
                    json!({"token": &rt[..rt.len().min(8)], "status": "error", "error": e.to_string()})
                }
            }
        }));
    }

    let mut results = Vec::new();
    for h in handles {
        if let Ok(result) = h.await {
            results.push(result);
        }
    }

    Json(json!({"results": results})).into_response()
}

/// DELETE /api/admin/accounts/{id}
pub async fn delete_account(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    if let Err(e) = queries::delete_account(&state.db(), id).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response();
    }

    state.scheduler.remove_account(id);
    queries::insert_account_event(&state.db(), id, "deleted", "manual").await;
    Json(json!({"message": "ok"})).into_response()
}

/// POST /api/admin/accounts/batch-delete
pub async fn batch_delete_accounts(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<BatchDeleteRequest>,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    let mut deleted = 0i64;
    if !req.ids.is_empty() {
        if let Ok(n) = queries::batch_delete_accounts(&state.db(), &req.ids).await {
            deleted = n;
        }
        for id in &req.ids {
            state.scheduler.remove_account(*id);
            queries::insert_account_event(&state.db(), *id, "deleted", "batch").await;
        }
    }

    Json(json!({"message": "ok", "deleted": deleted})).into_response()
}

#[derive(Deserialize)]
pub struct BatchDeleteRequest {
    pub ids: Vec<i64>,
}

/// POST /api/admin/accounts/{id}/refresh
pub async fn refresh_account(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    let account = match state.scheduler.get_account(id) {
        Some(a) => a,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "account not found"})),
            )
                .into_response()
        }
    };

    let rt = account.refresh_token.read().clone();
    if rt.is_empty() {
        // AT-only 账号无需刷新，直接返回成功
        return Json(json!({"message": "ok", "at_only": true})).into_response();
    }

    let client = reqwest::Client::new();
    match token::refresh::refresh_with_retry(&client, &rt).await {
        Ok(token_resp) => {
            let info = token::parse_id_token(&token_resp.id_token).unwrap_or_default();
            let expires_at =
                chrono::Utc::now() + chrono::Duration::seconds(token_resp.expires_in);

            *account.access_token.write() = token_resp.access_token;
            if !token_resp.refresh_token.is_empty() {
                *account.refresh_token.write() = token_resp.refresh_token;
            }
            *account.expires_at.write() = expires_at;
            if !info.email.is_empty() {
                *account.email.write() = info.email;
            }
            if !info.chatgpt_plan_type.is_empty() {
                *account.plan_type.write() = info.chatgpt_plan_type;
            }

            state.scheduler.clear_cooldown(&account);

            Json(json!({"message": "ok"})).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// POST /api/admin/accounts/batch-refresh — 一键刷新所有有 RT 的账号
pub async fn batch_refresh(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    let accounts = state.scheduler.all_accounts();
    let total = accounts.len();

    // 筛选有 RT 的账号
    let rt_accounts: Vec<_> = accounts
        .iter()
        .filter(|acc| !acc.refresh_token.read().is_empty())
        .collect();
    let rt_count = rt_accounts.len();

    if rt_count == 0 {
        return Json(json!({
            "total": total, "refreshed": 0, "success": 0, "fail": 0, "skipped": total,
        })).into_response();
    }

    info!(total, rt_count, "批量刷新令牌开始");

    let semaphore = Arc::new(tokio::sync::Semaphore::new(20));
    let mut handles = Vec::with_capacity(rt_count);

    for acc in rt_accounts {
        let acc = Arc::clone(acc);
        let sem = semaphore.clone();
        let state = state.clone();

        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let rt = acc.refresh_token.read().clone();
            let client = reqwest::Client::new();

            match token::refresh::refresh_with_retry(&client, &rt).await {
                Ok(resp) => {
                    let info = token::parse_id_token(&resp.id_token).unwrap_or_default();
                    let expires_at = chrono::Utc::now() + chrono::Duration::seconds(resp.expires_in);

                    *acc.access_token.write() = resp.access_token.clone();
                    if !resp.refresh_token.is_empty() {
                        *acc.refresh_token.write() = resp.refresh_token.clone();
                    }
                    *acc.expires_at.write() = expires_at;
                    if !info.email.is_empty() {
                        *acc.email.write() = info.email.clone();
                    }
                    if !info.chatgpt_plan_type.is_empty() {
                        *acc.plan_type.write() = info.chatgpt_plan_type.clone();
                    }

                    // 更新数据库
                    let creds = Credentials {
                        refresh_token: acc.refresh_token.read().clone(),
                        access_token: resp.access_token,
                        id_token: resp.id_token,
                        expires_at: expires_at.to_rfc3339(),
                        email: info.email,
                        account_id: info.chatgpt_account_id,
                        plan_type: info.chatgpt_plan_type,
                        ..Default::default()
                    };
                    let _ = queries::update_account_credentials(&state.db(), acc.db_id, &creds).await;

                    true
                }
                Err(e) => {
                    warn!(account_id = acc.db_id, error = %e, "批量刷新失败");
                    false
                }
            }
        }));
    }

    let mut success = 0u32;
    let mut fail = 0u32;
    for h in handles {
        match h.await {
            Ok(true) => success += 1,
            _ => fail += 1,
        }
    }

    let skipped = total as u32 - success - fail;
    info!(success, fail, skipped, "批量刷新令牌完成");

    Json(json!({
        "total": total,
        "refreshed": success + fail,
        "success": success,
        "fail": fail,
        "skipped": skipped,
    })).into_response()
}

/// GET /api/admin/accounts/{id}/usage → AccountUsageDetail
pub async fn account_usage(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    match queries::get_account_usage(&state.db(), id).await {
        Ok(detail) => Json(json!(detail)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// GET /api/admin/accounts/{id}/test — 单账号测试连接（SSE 流式）
pub async fn test_connection(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> axum::response::Response {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    let account = match state.scheduler.get_account(id) {
        Some(a) => a,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "账号不在运行时池中"})),
            )
                .into_response();
        }
    };

    let access_token = account.access_token.read().clone();
    if access_token.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "账号没有可用的 Access Token，请先刷新"})),
        )
            .into_response();
    }

    let proxy_url = account.proxy_url.read().clone();
    let codex_account_id = account.codex_account_id.read().clone();
    let account_id_str = id.to_string();
    let global_proxy = state.config.proxy_url.clone();
    let test_model = {
        let settings = state.db_settings_cache.read().unwrap();
        settings.test_model.clone()
    };

    // SSE 流
    let (tx, rx) = tokio::sync::mpsc::channel::<String>(32);
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);

    tokio::spawn(async move {
        let send_event = |tx: &tokio::sync::mpsc::Sender<String>, event: serde_json::Value| {
            let msg = format!("data: {}\n\n", event);
            let _ = tx.try_send(msg);
        };

        // test_start
        send_event(&tx, json!({"type": "test_start", "model": &test_model}));

        // 构建最小测试请求
        let payload = json!({
            "model": test_model,
            "input": [{"role": "user", "content": [{"type": "input_text", "text": "Say hello in one sentence."}]}],
            "stream": true,
            "store": false,
            "instructions": "You are a helpful assistant. Reply briefly.",
        });

        let upstream_url = format!("{}/responses", crate::proxy::UPSTREAM_BASE);
        let ua = crate::proxy::useragent::ua_for_account(&account_id_str);
        let version = crate::proxy::useragent::version_from_ua(ua);

        // 构建 client（带代理）
        let mut builder = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10));
        let proxy_str = if !proxy_url.is_empty() {
            Some(proxy_url.as_str())
        } else {
            global_proxy.as_deref()
        };
        if let Some(url) = proxy_str {
            if let Ok(proxy) = reqwest::Proxy::all(url) {
                builder = builder.proxy(proxy);
            }
        }
        let client = builder.build().unwrap_or_else(|_| reqwest::Client::new());

        let mut req = client
            .post(&upstream_url)
            .header("Authorization", format!("Bearer {}", access_token))
            .header("Content-Type", "application/json")
            .header("Accept", "text/event-stream")
            .header("User-Agent", ua)
            .header("Version", version)
            .header("Originator", crate::proxy::ORIGINATOR)
            .json(&payload)
            .timeout(std::time::Duration::from_secs(60));

        if !codex_account_id.is_empty() {
            req = req.header("Chatgpt-Account-Id", &codex_account_id);
        }

        let start = std::time::Instant::now();

        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                send_event(&tx, json!({"type": "error", "error": format!("请求失败: {}", e)}));
                return;
            }
        };

        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            let truncated = if body.len() > 500 { &body[..500] } else { &body };
            send_event(&tx, json!({"type": "error", "error": format!("上游返回 {}: {}", status, truncated)}));
            return;
        }

        // 读取 SSE 流
        use futures::StreamExt;
        let mut stream = resp.bytes_stream();
        let mut buffer = String::new();
        let mut has_content = false;

        while let Some(chunk) = stream.next().await {
            let chunk = match chunk {
                Ok(c) => c,
                Err(_) => break,
            };
            buffer.push_str(&String::from_utf8_lossy(&chunk));

            while let Some(pos) = buffer.find("\n\n") {
                let line = buffer[..pos].to_string();
                buffer = buffer[pos + 2..].to_string();

                // SSE 事件可能是多行（event: ...\ndata: ...），提取 data: 行
                let data = line
                    .lines()
                    .find_map(|l| l.strip_prefix("data: "))
                    .unwrap_or("");
                if data.is_empty() || data == "[DONE]" {
                    continue;
                }

                if let Ok(event) = serde_json::from_str::<serde_json::Value>(data) {
                    let event_type = event.get("type").and_then(|v| v.as_str()).unwrap_or("");
                    match event_type {
                        "response.output_text.delta" => {
                            if let Some(delta) = event.get("delta").and_then(|v| v.as_str()) {
                                if !delta.is_empty() {
                                    has_content = true;
                                    send_event(&tx, json!({"type": "content", "text": delta}));
                                }
                            }
                        }
                        "response.completed" => {
                            let duration = start.elapsed().as_millis();
                            send_event(&tx, json!({"type": "content", "text": format!("\n\n--- 耗时 {}ms ---", duration)}));
                            send_event(&tx, json!({"type": "test_complete", "success": true}));
                            return;
                        }
                        "response.failed" => {
                            let err_msg = event
                                .pointer("/response/status_details/error/message")
                                .and_then(|v| v.as_str())
                                .unwrap_or("上游返回 response.failed");
                            send_event(&tx, json!({"type": "error", "error": err_msg}));
                            return;
                        }
                        _ => {}
                    }
                }
            }
        }

        if !has_content {
            send_event(&tx, json!({"type": "error", "error": "未收到模型输出"}));
        }
    });

    axum::response::Response::builder()
        .header("Content-Type", "text/event-stream")
        .header("Cache-Control", "no-cache")
        .header("Connection", "keep-alive")
        .header("X-Accel-Buffering", "no")
        .body(axum::body::Body::from_stream(
            stream.map(Ok::<_, std::convert::Infallible>),
        ))
        .unwrap()
}

/// POST /api/admin/accounts/batch-test
pub async fn batch_test(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    let accounts = state.scheduler.all_accounts();
    let total = accounts.len();
    if total == 0 {
        return Json(json!({
            "total": 0, "success": 0, "failed": 0,
            "banned": 0, "rate_limited": 0, "recovered": 0,
        })).into_response();
    }

    let (test_model, concurrency) = {
        let s = state.db_settings_cache.read().unwrap();
        (s.test_model.clone(), (s.test_concurrency as usize).max(1))
    };

    info!(total, concurrency, model = %test_model, "批量测试开始");

    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let mut handles = Vec::with_capacity(total);

    for acc in &accounts {
        // 跳过无 token 的账号
        if acc.access_token.read().is_empty() {
            continue;
        }

        let acc = Arc::clone(acc);
        let state = state.clone();
        let sem = semaphore.clone();
        let model = test_model.clone();

        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            batch_test_one(&state, &acc, &model).await
        }));
    }

    let mut success = 0u32;
    let mut failed = 0u32;
    let mut banned = 0u32;
    let mut rate_limited = 0u32;
    let mut recovered = 0u32;

    for h in handles {
        if let Ok(result) = h.await {
            match result {
                BatchTestResult::Success => success += 1,
                BatchTestResult::Recovered => { recovered += 1; success += 1; }
                BatchTestResult::RateLimited => rate_limited += 1,
                BatchTestResult::Banned => banned += 1,
                BatchTestResult::Failed => failed += 1,
            }
        }
    }

    info!(
        total, success, failed, banned, rate_limited, recovered,
        "批量测试完成"
    );

    Json(json!({
        "total": total,
        "success": success,
        "failed": failed,
        "banned": banned,
        "rate_limited": rate_limited,
        "recovered": recovered,
    })).into_response()
}

enum BatchTestResult {
    Success,
    Recovered,
    RateLimited,
    Banned,
    Failed,
}

/// 单个账号的批量测试逻辑
async fn batch_test_one(
    state: &AppState,
    acc: &Arc<Account>,
    model: &str,
) -> BatchTestResult {
    let access_token = acc.access_token.read().clone();
    let proxy_url = acc.proxy_url.read().clone();
    let codex_account_id = acc.codex_account_id.read().clone();
    let account_id_str = acc.db_id.to_string();
    let email = acc.email.read().clone();

    let payload = json!({
        "model": model,
        "input": [{"role": "user", "content": [{"type": "input_text", "text": "hi"}]}],
        "stream": true,
        "store": false,
        "instructions": "",
    });

    let upstream_url = format!("{}/responses", crate::proxy::UPSTREAM_BASE);
    let ua = crate::proxy::useragent::ua_for_account(&account_id_str);
    let version = crate::proxy::useragent::version_from_ua(ua);
    let client = crate::proxy::handler::get_or_create_client(state, &proxy_url);

    let mut req = client
        .post(&upstream_url)
        .header("Authorization", format!("Bearer {}", access_token))
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .header("User-Agent", ua)
        .header("Version", version)
        .header("Originator", crate::proxy::ORIGINATOR)
        .json(&payload)
        .timeout(std::time::Duration::from_secs(30));

    if !codex_account_id.is_empty() {
        req = req.header("Chatgpt-Account-Id", &codex_account_id);
    }

    let start = std::time::Instant::now();

    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            warn!(account_id = acc.db_id, email = %email, error = %e, "批量测试 — 请求失败");
            return BatchTestResult::Failed;
        }
    };

    let status = resp.status().as_u16();
    let latency = start.elapsed().as_millis() as u64;
    let resp_headers = resp.headers().clone();

    // 刷新用量 header
    crate::proxy::handler::update_usage_from_headers(acc, &resp_headers);

    match status {
        200 => {
            acc.report_success(latency);
            state.scheduler.recompute_health(acc);

            let usage_7d = acc.usage_7d_pct_100.load(Ordering::Relaxed);
            let usage_5h = acc.usage_5h_pct_100.load(Ordering::Relaxed);
            let was_cooldown = acc.is_in_cooldown();
            let resets_at = acc.resets_at.load(Ordering::Relaxed);

            // 持久化用量到数据库
            let db = state.db();
            let aid = acc.db_id;
            let u7d = usage_7d as f64 / 100.0;
            let u5h = usage_5h as f64 / 100.0;
            tokio::spawn(async move {
                let _ = queries::persist_account_usage(&db, aid, u7d, u5h).await;
            });

            // 用量 ≥ 100% — 标记为限流，即使 200 也不该继续调度
            if usage_7d >= 10000 || usage_5h >= 10000 {
                // 从 header 的 reset-after-seconds 计算冷却时间，fallback 7 天
                let resets_at_cur = acc.resets_at.load(Ordering::Relaxed);
                if resets_at_cur > 0 {
                    let cooldown = (resets_at_cur - chrono::Utc::now().timestamp()).max(60);
                    state.scheduler.mark_cooldown(acc, "rate_limited", cooldown);
                } else {
                    state.scheduler.mark_cooldown(acc, "rate_limited", 7 * 24 * 3600);
                }

                warn!(
                    account_id = acc.db_id, email = %email, latency,
                    usage_7d = u7d, usage_5h = u5h,
                    "批量测试 200 但用量已满 — 标记限流"
                );
                return BatchTestResult::RateLimited;
            }

            // 之前限流且用量已恢复 → 恢复调度
            if (was_cooldown || resets_at > 0) && usage_7d < 10000 && usage_5h < 10000 {
                acc.resets_at.store(0, Ordering::Relaxed);
                acc.usage_7d_pct_100.store(0, Ordering::Relaxed);
                acc.usage_5h_pct_100.store(0, Ordering::Relaxed);
                state.scheduler.try_recover(acc);

                let db = state.db();
                let aid = acc.db_id;
                tokio::spawn(async move {
                    let _ = queries::clear_account_usage_state(&db, aid).await;
                });

                info!(
                    account_id = acc.db_id, email = %email, latency,
                    "批量测试 200 — 账号已恢复调度"
                );
                return BatchTestResult::Recovered;
            }

            info!(
                account_id = acc.db_id, email = %email, latency,
                usage_7d = u7d,
                "批量测试 200"
            );
            BatchTestResult::Success
        }
        429 => {
            acc.report_failure(scheduler::FailureType::RateLimited);

            // 解析 resets_at（仅首次）
            let body = resp.text().await.unwrap_or_default();
            if acc.resets_at.load(Ordering::Relaxed) == 0 {
                if let Ok(body_json) = serde_json::from_str::<serde_json::Value>(&body) {
                    if let Some(ts) = body_json.pointer("/error/resets_at").and_then(|v| v.as_i64()) {
                        acc.resets_at.store(ts, Ordering::Relaxed);
                        let db = state.db();
                        let aid = acc.db_id;
                        tokio::spawn(async move {
                            let _ = queries::update_account_resets_at(&db, aid, ts).await;
                        });
                    }
                }
            }

            // 设置冷却
            let cooldown = crate::proxy::handler::parse_rate_limit_cooldown(
                &resp_headers, &body, acc,
            );
            state.scheduler.mark_cooldown(acc, "rate_limited", cooldown);

            warn!(
                account_id = acc.db_id, email = %email, cooldown,
                "批量测试 429"
            );
            BatchTestResult::RateLimited
        }
        401 => {
            acc.report_failure(scheduler::FailureType::Unauthorized);
            state.scheduler.mark_banned(acc);

            warn!(account_id = acc.db_id, email = %email, "批量测试 401");
            BatchTestResult::Banned
        }
        _ => {
            acc.report_failure(scheduler::FailureType::Other);
            state.scheduler.recompute_health(acc);

            let body = resp.text().await.unwrap_or_default();
            let body_short: String = body.chars().take(200).collect();
            warn!(account_id = acc.db_id, email = %email, status, body = %body_short, "批量测试失败");
            BatchTestResult::Failed
        }
    }
}

// ─── 使用统计 & 图表 ───

/// GET /api/admin/usage/stats → UsageStats
pub async fn usage_stats(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    match queries::get_usage_stats_full(&state.db()).await {
        Ok(stats) => Json(json!(stats)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// GET /api/admin/usage/chart-data → ChartAggregation
#[derive(Deserialize)]
pub struct ChartQuery {
    pub start: Option<String>,
    pub end: Option<String>,
    pub bucket_minutes: Option<i64>,
    // 兼容旧的 range 参数
    pub range: Option<String>,
}

pub async fn chart_data(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<ChartQuery>,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    // 前端发送 start/end/bucket_minutes 或 range
    let (range_minutes, bucket_minutes) = if let Some(bkt) = q.bucket_minutes {
        // 从 start/end 计算 range，或默认 1h
        let range = if let (Some(start), Some(end)) = (&q.start, &q.end) {
            // 尝试解析 ISO 日期计算差值
            if let (Ok(s), Ok(e)) = (
                chrono::DateTime::parse_from_rfc3339(start),
                chrono::DateTime::parse_from_rfc3339(end),
            ) {
                (e - s).num_minutes().max(1)
            } else {
                60
            }
        } else {
            60
        };
        (range, bkt)
    } else if let Some(range) = &q.range {
        match range.as_str() {
            "1h" => (60, 5),
            "6h" => (360, 15),
            "24h" => (1440, 30),
            "7d" => (10080, 360),
            "30d" => (43200, 1440),
            _ => (60, 5),
        }
    } else {
        (60, 5)
    };

    match queries::query_chart_data(&state.db(), range_minutes, bucket_minutes).await {
        Ok(data) => Json(json!(data)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// GET /api/admin/usage/logs → { logs: UsageLog[], total: number }
#[derive(Deserialize)]
pub struct LogsQuery {
    #[serde(default = "default_page")]
    pub page: i64,
    #[serde(default = "default_page_size")]
    pub page_size: i64,
    pub model: Option<String>,
    pub email: Option<String>,
    pub endpoint: Option<String>,
    pub stream: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub range: Option<String>,
}
fn default_page() -> i64 {
    1
}
fn default_page_size() -> i64 {
    20
}

pub async fn usage_logs(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<LogsQuery>,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    match queries::query_usage_logs_filtered(
        &state.db(),
        q.page,
        q.page_size,
        q.model.as_deref(),
        q.email.as_deref(),
        q.endpoint.as_deref(),
        q.stream.as_deref(),
        q.start.as_deref(),
        q.end.as_deref(),
    )
    .await
    {
        Ok((logs, total)) => Json(json!({
            "logs": logs,
            "total": total,
        }))
        .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// DELETE /api/admin/usage/logs
pub async fn clear_usage_logs(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    match queries::clear_usage_logs(&state.db()).await {
        Ok(_) => Json(json!({"message": "ok"})).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ─── 系统运维 ───

/// GET /api/admin/ops/overview → OpsOverviewResponse
pub async fn ops_overview(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    let accounts = state.scheduler.all_accounts();
    let total_active: i64 = accounts
        .iter()
        .map(|a| a.active_requests.load(Ordering::Relaxed))
        .sum();
    let total_requests: u64 = accounts
        .iter()
        .map(|a| a.total_requests.load(Ordering::Relaxed))
        .sum();

    let uptime = state.start_time.elapsed().as_secs();

    // 流量统计（RPM、TPM、today_tokens、error_rate）
    let usage = queries::get_usage_stats_full(&state.db()).await.ok();
    let today_requests = usage.as_ref().map(|u| u.today_requests).unwrap_or(0);
    let today_tokens = usage.as_ref().map(|u| u.today_tokens).unwrap_or(0);
    let rpm = usage.as_ref().map(|u| u.rpm as f64).unwrap_or(0.0);
    let tpm = usage.as_ref().map(|u| u.tpm as f64).unwrap_or(0.0);
    let error_rate = usage.as_ref().map(|u| u.error_rate).unwrap_or(0.0);

    // CPU & 内存
    let (cpu_percent, mem_percent, mem_used, mem_total, process_mem) = get_sys_metrics();

    // PostgreSQL 连接池
    let db = state.db();
    let pool_size = db.size() as i64;
    let pool_idle = db.num_idle() as i64;
    let pool_in_use = pool_size - pool_idle;
    let pool_max = {
        let s = state.db_settings_cache.read().unwrap();
        if s.pg_max_conns > 0 { s.pg_max_conns as i64 } else { state.config.db_pool_size as i64 }
    };
    let pg_usage = if pool_max > 0 { pool_in_use as f64 / pool_max as f64 * 100.0 } else { 0.0 };

    // in-process 缓存（内存缓存，始终健康）
    let cache_size = state.token_cache.len() as i64;

    // RPM 限额
    let settings = state.db_settings_cache.read().unwrap();
    let rpm_limit = settings.global_rpm as i64;

    Json(json!({
        "updated_at": chrono::Utc::now().to_rfc3339(),
        "uptime_seconds": uptime,
        "database_driver": "postgres",
        "database_label": "PostgreSQL",
        "cache_driver": "memory",
        "cache_label": "in-process",
        "cpu": { "percent": cpu_percent, "cores": num_cpus() },
        "memory": { "percent": mem_percent, "used_bytes": mem_used, "total_bytes": mem_total, "process_bytes": process_mem },
        "runtime": {
            "goroutines": tokio::runtime::Handle::current().metrics().num_alive_tasks(),
            "available_accounts": state.scheduler.available_count(),
            "total_accounts": accounts.len(),
        },
        "requests": {
            "active": total_active,
            "total": total_requests,
        },
        "postgres": {
            "healthy": true,
            "open": pool_size, "in_use": pool_in_use, "idle": pool_idle,
            "max_open": pool_max,
            "wait_count": 0, "usage_percent": pg_usage,
        },
        "redis": {
            "healthy": true,
            "total_conns": cache_size, "idle_conns": 0, "stale_conns": 0,
            "pool_size": cache_size, "usage_percent": 0.0,
        },
        "traffic": {
            "qps": rpm / 60.0, "qps_peak": 0.0,
            "tps": tpm / 60.0, "tps_peak": 0.0,
            "rpm": rpm, "tpm": tpm,
            "error_rate": error_rate,
            "today_requests": today_requests,
            "today_tokens": today_tokens,
            "rpm_limit": rpm_limit,
        },
    }))
    .into_response()
}

/// 获取 CPU 和内存指标
fn get_sys_metrics() -> (f64, f64, u64, u64, u64) {
    use sysinfo::{Pid, ProcessesToUpdate, System};
    let mut sys = System::new();
    sys.refresh_memory();
    sys.refresh_cpu_usage();

    let mem_total = sys.total_memory();
    let mem_used = sys.used_memory();
    let mem_percent = if mem_total > 0 {
        mem_used as f64 / mem_total as f64 * 100.0
    } else {
        0.0
    };

    let cpu_percent = sys.global_cpu_usage() as f64;

    // 获取本进程内存占用（RSS）
    let pid = Pid::from_u32(std::process::id());
    sys.refresh_processes(ProcessesToUpdate::Some(&[pid]), false);
    let process_mem = sys.process(pid).map(|p| p.memory()).unwrap_or(0);

    (cpu_percent, mem_percent, mem_used, mem_total, process_mem)
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

// ─── 系统设置 ───

/// GET /api/admin/settings → SystemSettings
pub async fn get_settings(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    match queries::get_system_settings(&state.db()).await {
        Ok(s) => {
            // 前端需要更多字段
            let admin_auth_source = if state.config.admin_secret.is_some() {
                "env"
            } else if !s.admin_secret.is_empty() {
                "database"
            } else {
                "disabled"
            };

            Json(json!({
                "max_concurrency": s.max_concurrency,
                "global_rpm": s.global_rpm,
                "test_model": s.test_model,
                "test_concurrency": s.test_concurrency,
                "proxy_url": s.proxy_url,
                "admin_secret": s.admin_secret,
                "admin_auth_source": admin_auth_source,
                "auto_clean_unauthorized": s.auto_clean_unauthorized,
                "auto_clean_rate_limited": s.auto_clean_rate_limited,
                "auto_clean_full_usage": s.auto_clean_full_usage,
                "auto_clean_error": s.auto_clean_error,
                "auto_clean_expired": s.auto_clean_expired,
                "fast_scheduler_enabled": s.fast_scheduler_enabled,
                "max_retries": s.max_retries,
                "proxy_pool_enabled": false,
                "allow_remote_migration": false,
                "pg_max_conns": state.config.db_pool_size,
                "redis_pool_size": 0,
                "database_driver": "postgres",
                "database_label": "PostgreSQL",
                "cache_driver": "memory",
                "cache_label": "in-process",
            }))
            .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// PUT /api/admin/settings
pub async fn update_settings(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(input): Json<serde_json::Value>,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    // 加载当前设置并合并前端发来的字段
    let mut settings = match queries::get_system_settings(&state.db()).await {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response()
        }
    };

    if let Some(v) = input.get("max_concurrency").and_then(|v| v.as_i64()) {
        settings.max_concurrency = v as i32;
    }
    if let Some(v) = input.get("global_rpm").and_then(|v| v.as_i64()) {
        settings.global_rpm = v as i32;
    }
    if let Some(v) = input.get("test_model").and_then(|v| v.as_str()) {
        settings.test_model = v.to_string();
    }
    if let Some(v) = input.get("test_concurrency").and_then(|v| v.as_i64()) {
        settings.test_concurrency = v as i32;
    }
    if let Some(v) = input.get("max_retries").and_then(|v| v.as_i64()) {
        settings.max_retries = v as i32;
    }
    if let Some(v) = input.get("admin_secret").and_then(|v| v.as_str()) {
        settings.admin_secret = v.to_string();
    }
    if let Some(v) = input.get("auto_clean_unauthorized").and_then(|v| v.as_bool()) {
        settings.auto_clean_unauthorized = v;
    }
    if let Some(v) = input.get("auto_clean_rate_limited").and_then(|v| v.as_bool()) {
        settings.auto_clean_rate_limited = v;
    }
    if let Some(v) = input.get("auto_clean_full_usage").and_then(|v| v.as_bool()) {
        settings.auto_clean_full_usage = v;
    }
    if let Some(v) = input.get("auto_clean_error").and_then(|v| v.as_bool()) {
        settings.auto_clean_error = v;
    }
    if let Some(v) = input.get("auto_clean_expired").and_then(|v| v.as_bool()) {
        settings.auto_clean_expired = v;
    }
    if let Some(v) = input.get("fast_scheduler_enabled").and_then(|v| v.as_bool()) {
        settings.fast_scheduler_enabled = v;
    }

    if let Err(e) = queries::update_system_settings(&state.db(), &settings).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response();
    }

    // 更新运行时
    state
        .scheduler
        .max_concurrency
        .store(settings.max_concurrency as i64, Ordering::Relaxed);
    state.rate_limiter.set_rpm(settings.global_rpm as i64);

    // 动态修改 PG 连接池大小
    let old_max = state.db().size() as i32;
    let new_max = settings.pg_max_conns.clamp(5, 500);
    if new_max != old_max as i32 {
        match crate::db::init(&state.config.database_url, new_max as u32).await {
            Ok(new_pool) => {
                state.replace_db(new_pool);
                tracing::info!(old = old_max, new = new_max, "PG 连接池已动态调整");
            }
            Err(e) => {
                tracing::error!(error = %e, "PG 连接池替换失败");
            }
        }
    }

    *state.db_settings_cache.write().unwrap() = settings;
    state.scheduler.recompute_all();

    // 返回最新设置（调用 get_settings 逻辑）
    get_settings(State(state), headers).await.into_response()
}

// ─── API Keys ───

/// GET /api/admin/keys → { keys: APIKeyRow[] }
pub async fn list_keys(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    match queries::list_api_keys(&state.db()).await {
        Ok(keys) => Json(json!({"keys": keys})).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
pub struct CreateKeyRequest {
    pub name: String,
    pub key: Option<String>,
}

/// POST /api/admin/keys → { id, key, name }
pub async fn create_key(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<CreateKeyRequest>,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    let key = req
        .key
        .filter(|k| !k.is_empty())
        .unwrap_or_else(|| format!("sk-{}", uuid::Uuid::new_v4().to_string().replace('-', "")));

    match queries::insert_api_key(&state.db(), &req.name, &key).await {
        Ok(id) => (
            StatusCode::CREATED,
            Json(json!({"id": id, "key": key, "name": req.name})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// DELETE /api/admin/keys/{id}
pub async fn delete_key(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    match queries::delete_api_key(&state.db(), id).await {
        Ok(_) => Json(json!({"message": "ok"})).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ─── 模型列表 ───

/// GET /api/admin/models → { models: string[] }
pub async fn list_models(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    let models: Vec<&str> = crate::proxy::SUPPORTED_MODELS.to_vec();
    Json(json!({"models": models})).into_response()
}

// ─── 自动清理 ───

/// POST /api/admin/accounts/clean-banned
pub async fn clean_banned(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    let accounts = state.scheduler.all_accounts();
    let mut cleaned = 0;
    for acc in &accounts {
        if acc.health_tier.load(Ordering::Relaxed) == scheduler::TIER_BANNED {
            let _ = queries::delete_account(&state.db(), acc.db_id).await;
            state.scheduler.remove_account(acc.db_id);
            queries::insert_account_event(&state.db(), acc.db_id, "deleted", "clean_banned").await;
            cleaned += 1;
        }
    }
    Json(json!({"message": "ok", "cleaned": cleaned})).into_response()
}

/// POST /api/admin/accounts/clean-rate-limited
pub async fn clean_rate_limited(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    let accounts = state.scheduler.all_accounts();
    let mut cleaned = 0;
    for acc in &accounts {
        if acc.is_in_cooldown() {
            let _ = queries::delete_account(&state.db(), acc.db_id).await;
            state.scheduler.remove_account(acc.db_id);
            queries::insert_account_event(&state.db(), acc.db_id, "deleted", "clean_rate_limited").await;
            cleaned += 1;
        }
    }
    Json(json!({"message": "ok", "cleaned": cleaned})).into_response()
}

/// POST /api/admin/accounts/clean-error
pub async fn clean_error(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    let accounts = state.scheduler.all_accounts();
    let mut cleaned = 0;
    for acc in &accounts {
        let tier = acc.health_tier.load(Ordering::Relaxed);
        if tier == scheduler::TIER_BANNED || tier == scheduler::TIER_RISKY {
            let _ = queries::delete_account(&state.db(), acc.db_id).await;
            state.scheduler.remove_account(acc.db_id);
            queries::insert_account_event(&state.db(), acc.db_id, "deleted", "clean_error").await;
            cleaned += 1;
        }
    }
    Json(json!({"message": "ok", "cleaned": cleaned})).into_response()
}

// ─── 文件导入 ───

/// POST /api/admin/accounts/import
/// 支持 format: txt (默认, RT), json, at_txt
pub async fn import_accounts(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    mut multipart: axum::extract::Multipart,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, "unauthorized".to_string()).into_response();
    }

    let mut format = "txt".to_string();
    let mut proxy_url = String::new();
    let mut file_data: Vec<u8> = Vec::new();

    // 解析 multipart 字段
    while let Ok(Some(field)) = multipart.next_field().await {
        let name = field.name().unwrap_or("").to_string();
        match name.as_str() {
            "format" => {
                format = field.text().await.unwrap_or_else(|_| "txt".to_string());
            }
            "proxy_url" => {
                proxy_url = field.text().await.unwrap_or_default();
            }
            "file" => {
                file_data = field.bytes().await.unwrap_or_default().to_vec();
            }
            _ => {}
        }
    }

    if file_data.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "请上传文件（字段名: file）"})).to_string(),
        )
            .into_response();
    }

    if file_data.len() > 2 * 1024 * 1024 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "文件大小不能超过 2MB"})).to_string(),
        )
            .into_response();
    }

    match format.as_str() {
        "at_txt" => import_at_txt(state, file_data, proxy_url).await,
        "json" => import_json(state, file_data, proxy_url).await,
        _ => import_rt_txt(state, file_data, proxy_url).await,
    }
}

/// SSE 进度事件
fn sse_event(
    event_type: &str,
    current: usize,
    total: usize,
    success: usize,
    duplicate: usize,
    failed: usize,
) -> String {
    format!(
        "data: {}\n\n",
        json!({
            "type": event_type,
            "current": current,
            "total": total,
            "success": success,
            "duplicate": duplicate,
            "failed": failed,
        })
    )
}

/// AT TXT 文件导入 — 不走刷新路径
async fn import_at_txt(
    state: Arc<AppState>,
    file_data: Vec<u8>,
    proxy_url: String,
) -> axum::response::Response {
    let content = String::from_utf8_lossy(&file_data);

    // 按行分割，去 BOM，文件内去重
    let mut seen = std::collections::HashSet::new();
    let mut tokens: Vec<String> = Vec::new();
    for line in content.lines() {
        let t = line.trim().trim_start_matches('\u{feff}');
        if !t.is_empty() && seen.insert(t.to_string()) {
            tokens.push(t.to_string());
        }
    }

    if tokens.is_empty() {
        return Json(json!({"error": "文件中未找到有效的 Access Token"})).into_response();
    }

    // 数据库去重
    let existing = queries::get_all_access_tokens(&state.db()).await.unwrap_or_default();
    let mut new_tokens: Vec<String> = Vec::new();
    let mut duplicate_count = 0usize;
    for at in &tokens {
        if existing.contains(at) {
            duplicate_count += 1;
        } else {
            new_tokens.push(at.clone());
        }
    }
    let total = tokens.len();

    if new_tokens.is_empty() {
        return Json(json!({
            "message": format!("所有 {} 个 AT 已存在，无需导入", total),
            "success": 0, "duplicate": duplicate_count, "failed": 0, "total": total,
        }))
        .into_response();
    }

    // SSE 流式响应
    let (tx, rx) = tokio::sync::mpsc::channel::<String>(64);
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);

    tokio::spawn(async move {
        let sem = Arc::new(tokio::sync::Semaphore::new(20));
        let success = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let failed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let current = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut handles = Vec::new();

        for (idx, at) in new_tokens.into_iter().enumerate() {
            let sem = sem.clone();
            let state = state.clone();
            let proxy_url = proxy_url.clone();
            let success = success.clone();
            let failed = failed.clone();
            let current = current.clone();

            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();

                let info = token::parse_id_token(&at).unwrap_or_default();
                let expires_at = if info.expires_at > 0 {
                    chrono::DateTime::from_timestamp(info.expires_at, 0)
                        .unwrap_or_else(|| chrono::Utc::now() + chrono::Duration::hours(1))
                } else {
                    chrono::Utc::now() + chrono::Duration::hours(1)
                };

                let creds = Credentials {
                    access_token: at.clone(),
                    email: info.email.clone(),
                    account_id: info.chatgpt_account_id.clone(),
                    plan_type: info.chatgpt_plan_type.clone(),
                    expires_at: expires_at.to_rfc3339(),
                    ..Default::default()
                };

                let name = if info.email.is_empty() {
                    format!("at-import-{}", idx + 1)
                } else {
                    info.email.clone()
                };

                match queries::insert_at_account(&state.db(), &name, &creds, &proxy_url).await {
                    Ok(id) => {
                        let account = Arc::new(Account::new(id));
                        *account.email.write() = info.email;
                        *account.plan_type.write() = info.chatgpt_plan_type;
                        *account.proxy_url.write() = proxy_url;
                        *account.access_token.write() = at;
                        *account.codex_account_id.write() = info.chatgpt_account_id;
                        *account.expires_at.write() = expires_at;
                        state.scheduler.add_account(account);
                        queries::insert_account_event(&state.db(), id, "added", "import_at").await;
                        success.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        failed.fetch_add(1, Ordering::Relaxed);
                    }
                }
                current.fetch_add(1, Ordering::Relaxed);
            }));
        }

        // 进度推送
        let tx2 = tx.clone();
        let success2 = success.clone();
        let failed2 = failed.clone();
        let current2 = current.clone();
        let progress_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(200));
            loop {
                interval.tick().await;
                let cur = current2.load(Ordering::Relaxed) + duplicate_count;
                let suc = success2.load(Ordering::Relaxed);
                let fai = failed2.load(Ordering::Relaxed);
                let _ = tx2
                    .send(sse_event("progress", cur, total, suc, duplicate_count, fai))
                    .await;
                if cur >= total {
                    break;
                }
            }
        });

        for h in handles {
            let _ = h.await;
        }
        progress_handle.abort();

        let suc = success.load(Ordering::Relaxed);
        let fai = failed.load(Ordering::Relaxed);
        let _ = tx
            .send(sse_event("complete", total, total, suc, duplicate_count, fai))
            .await;
    });

    axum::response::Response::builder()
        .header("Content-Type", "text/event-stream")
        .header("Cache-Control", "no-cache")
        .header("Connection", "keep-alive")
        .header("X-Accel-Buffering", "no")
        .body(axum::body::Body::from_stream(stream.map(Ok::<_, std::convert::Infallible>)))
        .unwrap()
}

/// RT TXT 文件导入
async fn import_rt_txt(
    state: Arc<AppState>,
    file_data: Vec<u8>,
    proxy_url: String,
) -> axum::response::Response {
    let content = String::from_utf8_lossy(&file_data);

    let mut seen = std::collections::HashSet::new();
    let mut tokens: Vec<String> = Vec::new();
    for line in content.lines() {
        let t = line.trim().trim_start_matches('\u{feff}');
        if !t.is_empty() && seen.insert(t.to_string()) {
            tokens.push(t.to_string());
        }
    }

    if tokens.is_empty() {
        return Json(json!({"error": "文件中未找到有效的 Refresh Token"})).into_response();
    }

    // 数据库去重
    let existing = queries::get_all_refresh_tokens(&state.db()).await.unwrap_or_default();
    let mut new_tokens: Vec<String> = Vec::new();
    let mut duplicate_count = 0usize;
    for rt in &tokens {
        if existing.contains(rt) {
            duplicate_count += 1;
        } else {
            new_tokens.push(rt.clone());
        }
    }
    let total = tokens.len();

    if new_tokens.is_empty() {
        return Json(json!({
            "message": format!("所有 {} 个 RT 已存在，无需导入", total),
            "success": 0, "duplicate": duplicate_count, "failed": 0, "total": total,
        }))
        .into_response();
    }

    let (tx, rx) = tokio::sync::mpsc::channel::<String>(64);
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);

    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let sem = Arc::new(tokio::sync::Semaphore::new(10));
        let success = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let failed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let current = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut handles = Vec::new();

        for rt in new_tokens {
            let client = client.clone();
            let sem = sem.clone();
            let state = state.clone();
            let proxy_url = proxy_url.clone();
            let success = success.clone();
            let failed = failed.clone();
            let current = current.clone();

            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();

                match token::refresh::refresh_with_retry(&client, &rt).await {
                    Ok(token_resp) => {
                        let info = token::parse_id_token(&token_resp.id_token).unwrap_or_default();
                        let expires_at = chrono::Utc::now()
                            + chrono::Duration::seconds(token_resp.expires_in);

                        let creds = Credentials {
                            refresh_token: if token_resp.refresh_token.is_empty() {
                                rt.clone()
                            } else {
                                token_resp.refresh_token
                            },
                            access_token: token_resp.access_token.clone(),
                            id_token: token_resp.id_token,
                            expires_at: expires_at.to_rfc3339(),
                            email: info.email.clone(),
                            account_id: info.chatgpt_account_id.clone(),
                            plan_type: info.chatgpt_plan_type.clone(),
                            ..Default::default()
                        };

                        let name = if info.email.is_empty() { rt[..8.min(rt.len())].to_string() } else { info.email.clone() };
                        if let Ok(id) = queries::insert_account(&state.db(), &name, &creds, &proxy_url).await {
                            let account = Arc::new(Account::new(id));
                            *account.email.write() = info.email;
                            *account.plan_type.write() = info.chatgpt_plan_type;
                            *account.proxy_url.write() = proxy_url;
                            *account.access_token.write() = token_resp.access_token;
                            *account.refresh_token.write() = creds.refresh_token;
                            *account.expires_at.write() = expires_at;
                            state.scheduler.add_account(account);
                            queries::insert_account_event(&state.db(), id, "added", "import_rt").await;
                            success.fetch_add(1, Ordering::Relaxed);
                        } else {
                            failed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(_) => {
                        failed.fetch_add(1, Ordering::Relaxed);
                    }
                }
                current.fetch_add(1, Ordering::Relaxed);
            }));
        }

        // 进度推送
        let tx2 = tx.clone();
        let success2 = success.clone();
        let failed2 = failed.clone();
        let current2 = current.clone();
        let progress_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(200));
            loop {
                interval.tick().await;
                let cur = current2.load(Ordering::Relaxed) + duplicate_count;
                let suc = success2.load(Ordering::Relaxed);
                let fai = failed2.load(Ordering::Relaxed);
                let _ = tx2
                    .send(sse_event("progress", cur, total, suc, duplicate_count, fai))
                    .await;
                if cur >= total {
                    break;
                }
            }
        });

        for h in handles {
            let _ = h.await;
        }
        progress_handle.abort();

        let suc = success.load(Ordering::Relaxed);
        let fai = failed.load(Ordering::Relaxed);
        let _ = tx
            .send(sse_event("complete", total, total, suc, duplicate_count, fai))
            .await;
    });

    axum::response::Response::builder()
        .header("Content-Type", "text/event-stream")
        .header("Cache-Control", "no-cache")
        .header("Connection", "keep-alive")
        .header("X-Accel-Buffering", "no")
        .body(axum::body::Body::from_stream(stream.map(Ok::<_, std::convert::Infallible>)))
        .unwrap()
}

/// JSON 文件导入
async fn import_json(
    state: Arc<AppState>,
    file_data: Vec<u8>,
    proxy_url: String,
) -> axum::response::Response {
    let content = String::from_utf8_lossy(&file_data);
    let content = content.trim_start_matches('\u{feff}');

    #[derive(Deserialize)]
    struct JsonEntry {
        #[serde(default)]
        refresh_token: String,
        #[serde(default)]
        access_token: String,
        #[serde(default)]
        email: String,
    }

    let entries: Vec<JsonEntry> = if let Ok(arr) = serde_json::from_str::<Vec<JsonEntry>>(content) {
        arr
    } else if let Ok(single) = serde_json::from_str::<JsonEntry>(content) {
        vec![single]
    } else {
        return Json(json!({"error": "不是有效的 JSON 格式"})).into_response();
    };

    // 按 token 类型分流：有 refresh_token 走 RT 导入，否则有 access_token 走 AT 导入
    let mut rt_tokens: Vec<String> = Vec::new();
    let mut at_tokens: Vec<String> = Vec::new();

    for e in entries {
        let rt = e.refresh_token.trim().to_string();
        let at = e.access_token.trim().to_string();
        if !rt.is_empty() {
            rt_tokens.push(rt);
        } else if !at.is_empty() {
            at_tokens.push(at);
        }
    }

    if rt_tokens.is_empty() && at_tokens.is_empty() {
        return Json(json!({"error": "JSON 文件中未找到有效的 refresh_token 或 access_token"})).into_response();
    }

    // 优先处理 RT（需要刷新验证），AT-only 直接导入
    if !rt_tokens.is_empty() && at_tokens.is_empty() {
        // 全部是 RT
        import_rt_txt(state, rt_tokens.join("\n").into_bytes(), proxy_url).await
    } else if rt_tokens.is_empty() && !at_tokens.is_empty() {
        // 全部是 AT-only
        import_at_txt(state, at_tokens.join("\n").into_bytes(), proxy_url).await
    } else {
        // 混合模式：先导入 RT，再导入 AT
        // 为简化 SSE 流处理，合并为两批串行执行
        // RT 优先（数量通常较少且需要网络验证）
        import_rt_txt(state.clone(), rt_tokens.join("\n").into_bytes(), proxy_url.clone()).await;
        import_at_txt(state, at_tokens.join("\n").into_bytes(), proxy_url).await
    }
}

// ─── 账号事件趋势 ───

/// GET /api/admin/accounts/event-trend?start=...&end=...&bucket_minutes=60
pub async fn account_event_trend(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    let start = params.get("start").cloned().unwrap_or_default();
    let end = params.get("end").cloned().unwrap_or_default();
    let bucket_minutes: i64 = params
        .get("bucket_minutes")
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);

    match queries::get_account_event_trend(&state.db(), &start, &end, bucket_minutes).await {
        Ok(points) => Json(json!({"trend": points})).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}
