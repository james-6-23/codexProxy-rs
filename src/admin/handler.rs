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

    let today_requests = queries::count_today_requests(&state.db).await.unwrap_or(0);

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

    // 从数据库读取真实的时间字段
    let db_rows = queries::list_active_accounts(&state.db).await.unwrap_or_default();
    let db_map: std::collections::HashMap<i64, &crate::db::models::AccountRow> =
        db_rows.iter().map(|r| (r.id, r)).collect();

    let accounts = state.scheduler.all_accounts();
    let now = chrono::Utc::now().timestamp();
    let mut result = Vec::new();

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
        let cooldown = acc.cooldown_until.load(Ordering::Relaxed);
        let rt_empty = acc.refresh_token.read().is_empty();

        let last_401 = acc.last_unauthorized_at.load(Ordering::Relaxed);
        let last_429 = acc.last_rate_limited_at.load(Ordering::Relaxed);
        let last_timeout = acc.last_timeout_at.load(Ordering::Relaxed);
        let last_5xx = acc.last_server_error_at.load(Ordering::Relaxed);
        let fail_streak = acc.failure_streak.load(Ordering::Relaxed);
        let success_streak = acc.success_streak.load(Ordering::Relaxed);

        let unauthorized_penalty = if last_401 > 0 {
            50.0 * (1.0 - (now - last_401) as f64 / 86400.0).max(0.0)
        } else {
            0.0
        };
        let rate_limit_penalty = if last_429 > 0 {
            22.0 * (1.0 - (now - last_429) as f64 / 3600.0).max(0.0)
        } else {
            0.0
        };
        let timeout_penalty = if last_timeout > 0 {
            18.0 * (1.0 - (now - last_timeout) as f64 / 900.0).max(0.0)
        } else {
            0.0
        };
        let server_penalty = if last_5xx > 0 {
            12.0 * (1.0 - (now - last_5xx) as f64 / 900.0).max(0.0)
        } else {
            0.0
        };
        let failure_penalty = (fail_streak as f64 * 6.0).min(24.0);
        let success_bonus = (success_streak as f64 * 2.0).min(12.0);

        let usage_penalty_7d = if usage_7d >= 100.0 {
            if plan == "free" { 40.0 } else { 20.0 }
        } else if usage_7d >= 70.0 {
            8.0
        } else {
            0.0
        };

        let latency_penalty = if latency >= 20000.0 {
            15.0
        } else if latency >= 10000.0 {
            8.0
        } else if latency >= 5000.0 {
            4.0
        } else {
            0.0
        };

        let status = if tier == scheduler::TIER_BANNED {
            "error"
        } else if acc.is_in_cooldown() {
            "cooldown"
        } else {
            "active"
        };

        let success_requests = (total as f64 * success_rate) as u64;
        let error_requests = total - success_requests;

        let last_success = acc.last_success_at.load(Ordering::Relaxed);

        // 从数据库读取真实的创建/更新时间
        let db_row = db_map.get(&acc.db_id);
        let created_at = db_row.map(|r| &r.created_at);
        let updated_at = db_row.map(|r| &r.updated_at);

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
            "scheduler_breakdown": {
                "unauthorized_penalty": unauthorized_penalty,
                "rate_limit_penalty": rate_limit_penalty,
                "timeout_penalty": timeout_penalty,
                "server_penalty": server_penalty,
                "failure_penalty": failure_penalty,
                "success_bonus": success_bonus,
                "usage_penalty_7d": usage_penalty_7d,
                "latency_penalty": latency_penalty,
            },
            "active_requests": active,
            "total_requests": total,
            "success_requests": success_requests,
            "error_requests": error_requests,
            "usage_percent_7d": usage_7d,
            "usage_percent_5h": usage_5h,
            "proxy_url": acc.proxy_url.read().clone(),
            "last_used_at": ts_to_rfc3339(last_success),
            "last_unauthorized_at": ts_to_rfc3339(last_401),
            "last_rate_limited_at": ts_to_rfc3339(last_429),
            "last_timeout_at": ts_to_rfc3339(last_timeout),
            "last_server_error_at": ts_to_rfc3339(last_5xx),
            "created_at": created_at,
            "updated_at": updated_at,
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

            match queries::insert_account(&state.db, &name, &creds, &req.proxy_url).await {
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

            match queries::insert_at_account(&state.db, &name, &creds, &proxy_url).await {
                Ok(id) => {
                    let account = Arc::new(Account::new(id));
                    *account.email.write() = info.email;
                    *account.plan_type.write() = info.chatgpt_plan_type;
                    *account.proxy_url.write() = proxy_url;
                    *account.access_token.write() = at;
                    *account.codex_account_id.write() = info.chatgpt_account_id;
                    *account.expires_at.write() = expires_at;
                    state.scheduler.add_account(account);
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

                    match queries::insert_account(&state.db, &info.email, &creds, &proxy_url).await {
                        Ok(id) => {
                            let account = Arc::new(Account::new(id));
                            *account.email.write() = info.email.clone();
                            *account.plan_type.write() = info.chatgpt_plan_type;
                            *account.proxy_url.write() = proxy_url;
                            *account.access_token.write() = token_resp.access_token;
                            *account.refresh_token.write() = creds.refresh_token;
                            *account.expires_at.write() = expires_at;
                            state.scheduler.add_account(account);
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

    if let Err(e) = queries::delete_account(&state.db, id).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response();
    }

    state.scheduler.remove_account(id);
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
        if let Ok(n) = queries::batch_delete_accounts(&state.db, &req.ids).await {
            deleted = n;
        }
        for id in &req.ids {
            state.scheduler.remove_account(*id);
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

/// GET /api/admin/accounts/{id}/usage → AccountUsageDetail
pub async fn account_usage(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    if let Err(code) = verify_admin(&state, &headers) {
        return (code, Json(json!({"error": "unauthorized"}))).into_response();
    }

    match queries::get_account_usage(&state.db, id).await {
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

                let data = line.strip_prefix("data: ").unwrap_or(&line);
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
    let mut success = 0usize;
    let mut failed = 0usize;
    let mut banned = 0usize;
    let mut rate_limited = 0usize;

    for acc in &accounts {
        let tier = acc.health_tier.load(Ordering::Relaxed);
        match tier {
            scheduler::TIER_BANNED => banned += 1,
            _ if acc.is_in_cooldown() => rate_limited += 1,
            _ if acc.is_available() => success += 1,
            _ => failed += 1,
        }
    }

    Json(json!({
        "total": total,
        "success": success,
        "failed": failed,
        "banned": banned,
        "rate_limited": rate_limited,
    }))
    .into_response()
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

    match queries::get_usage_stats_full(&state.db).await {
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

    match queries::query_chart_data(&state.db, range_minutes, bucket_minutes).await {
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

    let range_minutes = q.range.as_deref().map(|r| match r {
        "1h" => 60,
        "6h" => 360,
        "24h" => 1440,
        "7d" => 10080,
        "30d" => 43200,
        _ => 60,
    });

    match queries::query_usage_logs(
        &state.db,
        q.page,
        q.page_size,
        q.model.as_deref(),
        range_minutes,
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

    match queries::clear_usage_logs(&state.db).await {
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
    let usage = queries::get_usage_stats_full(&state.db).await.ok();
    let today_requests = usage.as_ref().map(|u| u.today_requests).unwrap_or(0);
    let today_tokens = usage.as_ref().map(|u| u.today_tokens).unwrap_or(0);
    let rpm = usage.as_ref().map(|u| u.rpm as f64).unwrap_or(0.0);
    let tpm = usage.as_ref().map(|u| u.tpm as f64).unwrap_or(0.0);
    let error_rate = usage.as_ref().map(|u| u.error_rate).unwrap_or(0.0);

    // CPU & 内存
    let (cpu_percent, mem_percent, mem_used, mem_total, process_mem) = get_sys_metrics();

    // PostgreSQL 连接池
    let pool_size = state.db.size() as i64;
    let pool_idle = state.db.num_idle() as i64;
    let pool_in_use = pool_size - pool_idle;
    let pool_max = state.config.db_pool_size as i64;
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

    match queries::get_system_settings(&state.db).await {
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
    let mut settings = match queries::get_system_settings(&state.db).await {
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

    if let Err(e) = queries::update_system_settings(&state.db, &settings).await {
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

    match queries::list_api_keys(&state.db).await {
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

    match queries::insert_api_key(&state.db, &req.name, &key).await {
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

    match queries::delete_api_key(&state.db, id).await {
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
            let _ = queries::delete_account(&state.db, acc.db_id).await;
            state.scheduler.remove_account(acc.db_id);
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
            let _ = queries::delete_account(&state.db, acc.db_id).await;
            state.scheduler.remove_account(acc.db_id);
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
            let _ = queries::delete_account(&state.db, acc.db_id).await;
            state.scheduler.remove_account(acc.db_id);
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
    let existing = queries::get_all_access_tokens(&state.db).await.unwrap_or_default();
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

                match queries::insert_at_account(&state.db, &name, &creds, &proxy_url).await {
                    Ok(id) => {
                        let account = Arc::new(Account::new(id));
                        *account.email.write() = info.email;
                        *account.plan_type.write() = info.chatgpt_plan_type;
                        *account.proxy_url.write() = proxy_url;
                        *account.access_token.write() = at;
                        *account.codex_account_id.write() = info.chatgpt_account_id;
                        *account.expires_at.write() = expires_at;
                        state.scheduler.add_account(account);
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
    let existing = queries::get_all_refresh_tokens(&state.db).await.unwrap_or_default();
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
                        if let Ok(id) = queries::insert_account(&state.db, &name, &creds, &proxy_url).await {
                            let account = Arc::new(Account::new(id));
                            *account.email.write() = info.email;
                            *account.plan_type.write() = info.chatgpt_plan_type;
                            *account.proxy_url.write() = proxy_url;
                            *account.access_token.write() = token_resp.access_token;
                            *account.refresh_token.write() = creds.refresh_token;
                            *account.expires_at.write() = expires_at;
                            state.scheduler.add_account(account);
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
        refresh_token: String,
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

    let tokens: Vec<String> = entries
        .into_iter()
        .map(|e| e.refresh_token.trim().to_string())
        .filter(|t| !t.is_empty())
        .collect();

    if tokens.is_empty() {
        return Json(json!({"error": "JSON 文件中未找到有效的 refresh_token"})).into_response();
    }

    import_rt_txt(state, tokens.join("\n").into_bytes(), proxy_url).await
}
