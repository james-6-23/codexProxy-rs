//! API Key 鉴权中间件
//!
//! 行为参照 Go 版 `codex2api/proxy/handler.go::authMiddleware`：
//! - 默认 fail-closed：未配置任何 API Key 时直接拒绝（503）。
//! - 仅当显式设置 `CODEX_ALLOW_ANONYMOUS=true` 时在无密钥情况下放行。
//! - Token 来源（按顺序）：
//!   1. `Authorization: Bearer <token>`
//!   2. `x-api-key: <token>` (Anthropic SDK)
//!   3. `anthropic-auth-token: <token>` (Claude Code)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use serde::Serialize;
use tokio::sync::RwLock;
use tracing::warn;

use crate::db::DbPool;
use crate::db::models::ApiKey;
use crate::db::queries;
use crate::state::AppState;

/// API Key 缓存 TTL（与 Go 版 5 分钟一致）
const KEY_CACHE_TTL: Duration = Duration::from_secs(300);

/// 进程内 API Key 缓存
pub struct ApiKeyCache {
    pool: DbPool,
    inner: RwLock<CacheInner>,
}

struct CacheInner {
    keys: HashMap<String, ApiKey>,
    refreshed_at: Option<Instant>,
}

impl ApiKeyCache {
    pub fn new(pool: DbPool) -> Self {
        Self {
            pool,
            inner: RwLock::new(CacheInner {
                keys: HashMap::new(),
                refreshed_at: None,
            }),
        }
    }

    /// 若缓存过期则从数据库刷新
    async fn refresh_if_needed(&self) {
        // 先持读锁判断是否需要刷新
        let need_refresh = {
            let g = self.inner.read().await;
            match g.refreshed_at {
                Some(t) => t.elapsed() >= KEY_CACHE_TTL,
                None => true,
            }
        };
        if !need_refresh {
            return;
        }

        // 升级为写锁刷新
        let new_keys = match queries::list_api_keys(&self.pool).await {
            Ok(keys) => keys
                .into_iter()
                .map(|k| (k.key.clone(), k))
                .collect::<HashMap<_, _>>(),
            Err(e) => {
                warn!(error = %e, "刷新 API Key 缓存失败，沿用旧缓存");
                let mut g = self.inner.write().await;
                // 即使失败也更新时间戳避免风暴重试
                g.refreshed_at = Some(Instant::now());
                return;
            }
        };

        let mut g = self.inner.write().await;
        g.keys = new_keys;
        g.refreshed_at = Some(Instant::now());
    }

    /// 是否存在任意 API Key
    pub async fn has_any(&self) -> bool {
        self.refresh_if_needed().await;
        let g = self.inner.read().await;
        !g.keys.is_empty()
    }

    /// 解析 token，命中返回对应的 ApiKey
    pub async fn resolve(&self, token: &str) -> Option<ApiKey> {
        self.refresh_if_needed().await;
        let g = self.inner.read().await;
        g.keys.get(token).cloned()
    }

    /// 主动失效缓存：清空内容并把 `refreshed_at` 重置为 None，
    /// 下次 `has_any` / `resolve` 时会立即从 DB 拉取最新数据。
    /// admin 端创建/删除 API Key 后调用，使变更立即生效（不必等 5 分钟 TTL）。
    pub async fn invalidate(&self) {
        let mut g = self.inner.write().await;
        g.keys.clear();
        g.refreshed_at = None;
    }
}

// ─── 中间件 ───

/// 从请求头提取调用方提供的 API Key（支持多种来源）
fn extract_api_key(headers: &HeaderMap) -> Option<String> {
    // 1. Authorization: Bearer <token>
    if let Some(v) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
        let trimmed = v.trim();
        if let Some(rest) = trimmed
            .strip_prefix("Bearer ")
            .or_else(|| trimmed.strip_prefix("bearer "))
        {
            let token = rest.trim();
            if !token.is_empty() {
                return Some(token.to_string());
            }
        } else if !trimmed.is_empty() {
            // 不带 Bearer 前缀也接受（部分客户端直接放 token）
            return Some(trimmed.to_string());
        }
    }
    // 2. x-api-key
    if let Some(v) = headers.get("x-api-key").and_then(|v| v.to_str().ok()) {
        let token = v.trim();
        if !token.is_empty() {
            return Some(token.to_string());
        }
    }
    // 3. anthropic-auth-token
    if let Some(v) = headers
        .get("anthropic-auth-token")
        .and_then(|v| v.to_str().ok())
    {
        let token = v.trim();
        if !token.is_empty() {
            return Some(token.to_string());
        }
    }
    None
}

/// 脱敏：仅保留头尾 4 位
fn mask_key(key: &str) -> String {
    let len = key.chars().count();
    if len <= 8 {
        return "*".repeat(len);
    }
    let head: String = key.chars().take(4).collect();
    let tail: String = key.chars().skip(len - 4).collect();
    format!("{}***{}", head, tail)
}

/// 注入 request extensions 中的 API Key 元信息（供下游 handler 读取）
#[derive(Debug, Clone)]
pub struct AuthedApiKey {
    pub id: i64,
    pub name: String,
    pub key_masked: String,
}

/// axum 中间件 — 仅挂在 /v1/* 与 /backend-api/codex/* 等代理路由上
pub async fn require_api_key(
    State(state): State<Arc<AppState>>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    let path = req.uri().path().to_string();

    // 1. 没配置任何 key —— fail-closed 检查
    if !state.api_keys.has_any().await {
        if state.config.allow_anonymous_v1 {
            return next.run(req).await;
        }
        warn!(target: "security", path = %path, "auth blocked: no api keys configured");
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "Service is not configured: no API key has been created yet. Please add at least one API key in the admin dashboard, or set CODEX_ALLOW_ANONYMOUS=true to disable this check.",
            "service_unavailable",
        );
    }

    // 2. 提取 token
    let headers = req.headers().clone();
    let token = match extract_api_key(&headers) {
        Some(t) => t,
        None => {
            warn!(target: "security", path = %path, "auth blocked: missing api key");
            return error_response(
                StatusCode::UNAUTHORIZED,
                "Missing API key. Provide it via Authorization: Bearer <token>, x-api-key, or anthropic-auth-token header.",
                "missing_api_key",
            );
        }
    };

    // 3. 校验
    let row = match state.api_keys.resolve(&token).await {
        Some(r) => r,
        None => {
            warn!(
                target: "security",
                path = %path,
                key = %mask_key(&token),
                "auth failed: invalid api key"
            );
            return error_response(
                StatusCode::UNAUTHORIZED,
                "Invalid API key.",
                "invalid_api_key",
            );
        }
    };

    // 4. 注入 extensions 供下游 handler 使用
    req.extensions_mut().insert(AuthedApiKey {
        id: row.id,
        name: row.name.trim().to_string(),
        key_masked: mask_key(&row.key),
    });

    next.run(req).await
}

// ─── 辅助：OpenAI 风格错误响应 ───

#[derive(Serialize)]
struct ErrorEnvelope<'a> {
    error: ErrorDetail<'a>,
}

#[derive(Serialize)]
struct ErrorDetail<'a> {
    message: &'a str,
    #[serde(rename = "type")]
    err_type: &'static str,
    code: &'a str,
}

fn error_response(status: StatusCode, message: &str, code: &str) -> Response {
    let body = ErrorEnvelope {
        error: ErrorDetail {
            message,
            err_type: if status == StatusCode::SERVICE_UNAVAILABLE {
                "server_error"
            } else {
                "invalid_request_error"
            },
            code,
        },
    };
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap_or_default()))
        .unwrap()
}
