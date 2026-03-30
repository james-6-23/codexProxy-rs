use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures::StreamExt;
use serde_json::Value;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, warn};

use crate::proxy::translator;
use crate::scheduler::{Account, FailureType};
use crate::state::AppState;

/// POST /v1/chat/completions
pub async fn chat_completions(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    proxy_request(state, headers, body, "/v1/chat/completions", true).await
}

/// POST /v1/responses
pub async fn responses(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    proxy_request(state, headers, body, "/v1/responses", false).await
}

/// GET /v1/models
pub async fn list_models() -> impl IntoResponse {
    let models: Vec<Value> = super::SUPPORTED_MODELS
        .iter()
        .map(|m| {
            serde_json::json!({
                "id": m,
                "object": "model",
                "owned_by": "openai",
            })
        })
        .collect();

    axum::Json(serde_json::json!({
        "object": "list",
        "data": models,
    }))
}

/// 核心代理逻辑
async fn proxy_request(
    state: Arc<AppState>,
    _headers: HeaderMap,
    body: Bytes,
    endpoint: &str,
    translate: bool,
) -> Response {
    let start = Instant::now();
    let max_retries = state.settings.read().await.max_retries;

    // 解析请求体
    let body_json: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return error_response(StatusCode::BAD_REQUEST, &format!("无效 JSON: {}", e));
        }
    };

    let model = body_json
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let is_stream = body_json
        .get("stream")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // 全局限流检查
    if !state.rate_limiter.allow() {
        return error_response(StatusCode::TOO_MANY_REQUESTS, "全局速率限制");
    }

    let mut exclude_set: HashSet<i64> = HashSet::new();
    let mut last_error = String::new();

    for attempt in 0..=max_retries {
        // 选择账号
        let account = match state
            .scheduler
            .wait_for_available(&exclude_set, Duration::from_secs(30))
            .await
        {
            Some(acc) => acc,
            None => {
                return error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "无可用账号，请稍后重试",
                );
            }
        };

        let account_email = account.email.read().clone();
        let access_token = account.access_token.read().clone();
        let proxy_url = account.proxy_url.read().clone();

        // 构建上游请求
        let upstream_body = if translate {
            translator::translate_chat_to_responses(&body_json)
        } else {
            body_json.clone()
        };

        let upstream_url = format!("{}/v1/responses", super::UPSTREAM_BASE);

        // 构建 HTTP 客户端（支持代理）
        let client = build_client(&proxy_url, &state.config.proxy_url);

        let req = client
            .post(&upstream_url)
            .bearer_auth(&access_token)
            .header("Content-Type", "application/json")
            .json(&upstream_body)
            .timeout(Duration::from_secs(120));

        let request_start = Instant::now();

        match req.send().await {
            Ok(resp) => {
                let status = resp.status();
                let resp_headers = resp.headers().clone();
                let latency_ms = request_start.elapsed().as_millis() as u64;

                if status.is_success() {
                    // 成功
                    account.report_success(latency_ms);
                    account.release();
                    state.scheduler.recompute_health(&account);
                    state.scheduler.notify_available();

                    // 异步记录日志
                    let log_state = state.clone();
                    let log_model = model.clone();
                    let log_endpoint = endpoint.to_string();
                    let log_email = account_email.clone();
                    let account_id = account.db_id;
                    let duration = start.elapsed().as_millis() as i64;

                    tokio::spawn(async move {
                        log_usage(
                            &log_state,
                            account_id,
                            &log_endpoint,
                            &log_model,
                            status.as_u16() as i64,
                            duration,
                            is_stream,
                            &log_email,
                        )
                        .await;
                    });

                    // 流式 / 非流式转发
                    if is_stream {
                        return stream_response(resp, translate).await;
                    } else {
                        let resp_body = resp.bytes().await.unwrap_or_default();
                        let final_body = if translate {
                            match translator::translate_response_to_chat(&resp_body) {
                                Ok(translated) => translated,
                                Err(_) => resp_body.to_vec(),
                            }
                        } else {
                            resp_body.to_vec()
                        };

                        return Response::builder()
                            .status(StatusCode::OK)
                            .header("Content-Type", "application/json")
                            .body(Body::from(final_body))
                            .unwrap();
                    }
                }

                // 处理错误状态码
                account.release();
                let error_body = resp.text().await.unwrap_or_default();

                match status.as_u16() {
                    401 => {
                        account.report_failure(FailureType::Unauthorized);
                        state.scheduler.mark_banned(&account);
                        exclude_set.insert(account.db_id);
                        last_error = format!("401 Unauthorized: {}", error_body);
                        warn!(account_id = account.db_id, "账号 401，标记为 banned");
                    }
                    429 => {
                        account.report_failure(FailureType::RateLimited);
                        // 解析冷却时间
                        let cooldown = parse_rate_limit_cooldown(&resp_headers, &account);
                        state.scheduler.mark_cooldown(&account, "rate_limited", cooldown);
                        exclude_set.insert(account.db_id);
                        last_error = format!("429 Rate Limited: {}", error_body);
                        warn!(account_id = account.db_id, cooldown_secs = cooldown, "账号 429 限流");
                    }
                    500..=599 => {
                        account.report_failure(FailureType::ServerError);
                        state.scheduler.recompute_health(&account);
                        exclude_set.insert(account.db_id);
                        last_error = format!("{} Server Error: {}", status, error_body);
                    }
                    _ => {
                        // 4xx 客户端错误，不重试
                        account.report_failure(FailureType::Other);
                        state.scheduler.recompute_health(&account);
                        return error_response(status, &error_body);
                    }
                }

                state.scheduler.notify_available();
            }
            Err(e) => {
                account.release();

                if e.is_timeout() {
                    account.report_failure(FailureType::Timeout);
                    state.scheduler.recompute_health(&account);
                    exclude_set.insert(account.db_id);
                    last_error = format!("Timeout: {}", e);
                    warn!(account_id = account.db_id, "请求超时");
                } else {
                    account.report_failure(FailureType::Other);
                    state.scheduler.recompute_health(&account);
                    exclude_set.insert(account.db_id);
                    last_error = format!("Transport error: {}", e);
                    error!(account_id = account.db_id, error = %e, "传输错误");
                }

                state.scheduler.notify_available();
            }
        }
    }

    // 所有重试耗尽
    error_response(
        StatusCode::BAD_GATEWAY,
        &format!("所有重试已耗尽。最后错误: {}", last_error),
    )
}

/// 构建 HTTP 客户端
fn build_client(account_proxy: &str, global_proxy: &Option<String>) -> reqwest::Client {
    let mut builder = reqwest::Client::builder()
        .pool_max_idle_per_host(5)
        .pool_idle_timeout(Duration::from_secs(60))
        .connect_timeout(Duration::from_secs(10));

    // 账号代理优先，其次全局代理
    let proxy_url = if !account_proxy.is_empty() {
        Some(account_proxy)
    } else {
        global_proxy.as_deref()
    };

    if let Some(url) = proxy_url {
        if let Ok(proxy) = reqwest::Proxy::all(url) {
            builder = builder.proxy(proxy);
        }
    }

    builder.build().unwrap_or_else(|_| reqwest::Client::new())
}

/// 流式响应转发
async fn stream_response(resp: reqwest::Response, translate: bool) -> Response {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(32);

    tokio::spawn(async move {
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(data) => {
                    if translate {
                        // 逐 chunk 翻译 SSE 事件
                        if let Ok(translated) = translator::translate_sse_chunk(&data) {
                            let _ = tx.send(Ok(Bytes::from(translated))).await;
                        } else {
                            let _ = tx.send(Ok(data)).await;
                        }
                    } else {
                        let _ = tx.send(Ok(data)).await;
                    }
                }
                Err(e) => {
                    let _ = tx
                        .send(Err(std::io::Error::new(std::io::ErrorKind::Other, e)))
                        .await;
                    break;
                }
            }
        }
    });

    let stream = ReceiverStream::new(rx);
    let body = Body::from_stream(stream);

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/event-stream")
        .header("Cache-Control", "no-cache")
        .header("Connection", "keep-alive")
        .body(body)
        .unwrap()
}

/// 解析 429 响应的冷却时间
fn parse_rate_limit_cooldown(headers: &HeaderMap, account: &Account) -> i64 {
    // 尝试从 header 获取 reset 时间
    if let Some(val) = headers.get("x-ratelimit-reset-requests") {
        if let Ok(s) = val.to_str() {
            if let Ok(ts) = s.parse::<i64>() {
                let now = chrono::Utc::now().timestamp();
                return (ts - now).max(60);
            }
        }
    }

    // 按 plan_type 推断
    let plan = account.plan_type.read();
    match plan.as_str() {
        "free" => 7 * 24 * 3600, // 7 天
        _ => 3600,                // 1 小时
    }
}

/// 错误响应
fn error_response(status: StatusCode, message: &str) -> Response {
    let body = serde_json::json!({
        "error": {
            "message": message,
            "type": "proxy_error",
            "code": status.as_u16(),
        }
    });
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

/// 异步记录使用日志
async fn log_usage(
    state: &AppState,
    account_id: i64,
    endpoint: &str,
    model: &str,
    status_code: i64,
    duration_ms: i64,
    stream: bool,
    email: &str,
) {
    use crate::db::models::UsageLog;
    let log = UsageLog {
        id: 0,
        account_id,
        endpoint: endpoint.to_string(),
        model: model.to_string(),
        prompt_tokens: 0,
        completion_tokens: 0,
        total_tokens: 0,
        input_tokens: 0,
        output_tokens: 0,
        reasoning_tokens: 0,
        cached_tokens: 0,
        first_token_ms: 0,
        reasoning_effort: String::new(),
        status_code,
        duration_ms,
        stream,
        service_tier: String::new(),
        account_email: email.to_string(),
        created_at: String::new(),
    };
    let _ = state.log_sender.send(log).await;
}
