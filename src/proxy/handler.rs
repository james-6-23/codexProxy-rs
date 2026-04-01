use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use serde::Serialize;
use serde_json::Value;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info, warn};

use crate::proxy::translator::{self, StreamTranslator, UsageInfo};
use crate::scheduler::FailureType;
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
    #[derive(Serialize)]
    struct ModelList<'a> {
        object: &'static str,
        data: Vec<ModelEntry<'a>>,
    }
    #[derive(Serialize)]
    struct ModelEntry<'a> {
        id: &'a str,
        object: &'static str,
        owned_by: &'static str,
    }

    let data: Vec<ModelEntry> = super::SUPPORTED_MODELS
        .iter()
        .map(|m| ModelEntry { id: m, object: "model", owned_by: "openai" })
        .collect();

    axum::Json(ModelList { object: "list", data })
}

/// 核心代理逻辑
async fn proxy_request(
    state: Arc<AppState>,
    headers: HeaderMap,
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
    // 提取 reasoning_effort（兼容 Responses API 的 reasoning.effort 和 Chat API 的 reasoning_effort）
    let reasoning_effort = body_json
        .get("reasoning")
        .and_then(|r| r.get("effort"))
        .and_then(|v| v.as_str())
        .or_else(|| body_json.get("reasoning_effort").and_then(|v| v.as_str()))
        .unwrap_or("")
        .to_string();

    // 全局限流
    if !state.rate_limiter.allow() {
        return error_response(StatusCode::TOO_MANY_REQUESTS, "全局速率限制");
    }

    let mut exclude_set: HashSet<i64> = HashSet::new();
    let mut last_error = String::new();

    for _attempt in 0..=max_retries {
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
        let codex_account_id = account.codex_account_id.read().clone();
        let account_id_str = account.db_id.to_string();

        info!(
            endpoint,
            model = %model,
            account_id = account.db_id,
            email = %account_email,
            attempt = _attempt + 1,
            "→ 转发请求"
        );

        // ── 构建上游请求体 ──

        let mut upstream_body = if translate {
            translator::translate_chat_to_responses(&body_json)
        } else {
            body_json.clone()
        };

        // 必需字段
        if upstream_body.get("instructions").is_none() {
            upstream_body["instructions"] = Value::String(String::new());
        }
        upstream_body["stream"] = Value::Bool(true);
        upstream_body["store"] = Value::Bool(false);
        if upstream_body.get("include").is_none() {
            upstream_body["include"] =
                serde_json::json!(["reasoning.encrypted_content"]);
        }

        // 清理 Codex 不支持的字段
        translator::strip_unsupported_fields(&mut upstream_body);

        // 自动将字符串 input 包装为数组格式（Codex 要求 input 为 list）
        if let Some(input) = upstream_body.get("input") {
            if input.is_string() {
                let text = input.as_str().unwrap_or("").to_string();
                upstream_body["input"] = serde_json::json!([{
                    "role": "user",
                    "content": text,
                }]);
            }
        }

        // Session / prompt cache
        let session_id = resolve_session_id(&body_json, &headers, &account_id_str);
        if !session_id.is_empty() && upstream_body.get("prompt_cache_key").is_none() {
            upstream_body["prompt_cache_key"] = Value::String(session_id.clone());
        }

        // ── 构建 HTTP 请求 ──

        let upstream_url = format!("{}/responses", super::UPSTREAM_BASE);
        let ua = crate::proxy::useragent::ua_for_account(&account_id_str);
        let version = crate::proxy::useragent::version_from_ua(ua);
        let (stainless_os, stainless_arch) = crate::proxy::useragent::platform_from_ua(ua);
        let client = get_or_create_client(&state, &proxy_url);

        let mut req = client
            .post(&upstream_url)
            .header("Authorization", format!("Bearer {}", access_token))
            .header("Content-Type", "application/json")
            .header("Accept", "text/event-stream")
            .header("User-Agent", ua)
            .header("Version", version)
            .header("Originator", super::ORIGINATOR)
            .header("Connection", "Keep-Alive")
            .header("X-Stainless-Package-Version", version)
            .header("X-Stainless-Runtime-Version", version)
            .header("X-Stainless-Os", stainless_os)
            .header("X-Stainless-Arch", stainless_arch)
            .json(&upstream_body)
            .timeout(Duration::from_secs(600));

        if !codex_account_id.is_empty() {
            req = req.header("Chatgpt-Account-Id", &codex_account_id);
        }
        if !session_id.is_empty() {
            req = req
                .header("Session_id", &session_id)
                .header("Conversation_id", &session_id);
        }

        // ── 执行请求 ──

        let request_start = Instant::now();

        match req.send().await {
            Ok(resp) => {
                let status = resp.status();
                let resp_headers = resp.headers().clone();
                let latency_ms = request_start.elapsed().as_millis() as u64;

                if status.is_success() {
                    account.report_success(latency_ms);

                    // 从上游响应 header 提取用量百分比并更新到 Account
                    update_usage_from_headers(&account, &resp_headers);

                    account.release();
                    state.scheduler.recompute_health(&account);
                    state.scheduler.notify_available();

                    let msg = format!("{} ← 上游请求成功", status.as_u16());
                    info!(
                        endpoint,
                        model = %model,
                        account_id = account.db_id,
                        email = %account_email,
                        latency_ms,
                        "{msg}"
                    );

                    if is_stream || translate {
                        // Peek 第一个 chunk — 在返回 SSE 响应之前验证上游是否真正开始输出
                        let mut stream = resp.bytes_stream();
                        match peek_first_chunk(&mut stream).await {
                            PeekResult::Data(first_chunk) => {
                                // 成功拿到第一个 chunk，构建 SSE 响应
                                return stream_response_with_tracking(
                                    first_chunk,
                                    stream,
                                    translate,
                                    state.clone(),
                                    account.db_id,
                                    endpoint,
                                    &model,
                                    &account_email,
                                    &reasoning_effort,
                                    start,
                                )
                                .await;
                            }
                            PeekResult::UpstreamError(err_text) => {
                                // 首字节前上游返回了 SSE 错误事件 → bootstrap retry
                                warn!(
                                    account_id = account.db_id,
                                    "首 chunk 为错误事件，bootstrap retry: {}",
                                    err_text.chars().take(200).collect::<String>()
                                );
                                exclude_set.insert(account.db_id);
                                last_error = err_text;
                                state.scheduler.notify_available();
                                continue;
                            }
                            PeekResult::Empty => {
                                // 空流 — 上游立即关闭
                                warn!(account_id = account.db_id, "上游返回空流");
                                exclude_set.insert(account.db_id);
                                last_error = "上游返回空流".to_string();
                                state.scheduler.notify_available();
                                continue;
                            }
                            PeekResult::NetworkError(e) => {
                                // 网络层错误 → bootstrap retry
                                warn!(account_id = account.db_id, error = %e, "peek 阶段网络错误");
                                exclude_set.insert(account.db_id);
                                last_error = format!("peek 网络错误: {}", e);
                                state.scheduler.notify_available();
                                continue;
                            }
                        }
                    } else {
                        // sync 模式（Codex 仍返回 SSE，需读取流提取完整响应 + usage）
                        return collect_sync_response(
                            resp,
                            state.clone(),
                            account.db_id,
                            endpoint,
                            &model,
                            &account_email,
                            &reasoning_effort,
                            start,
                        )
                        .await;
                    }
                }

                // ── 错误状态码 ──
                // 错误响应也可能携带用量 header（尤其 429）
                update_usage_from_headers(&account, &resp_headers);
                account.release();
                let error_body = resp.text().await.unwrap_or_default();
                let duration = request_start.elapsed().as_millis() as i64;
                let status_u16 = status.as_u16();

                // 输出上游错误日志（401 → ERROR 红色，其余 → WARN 黄色）
                let err_body_short: String = error_body.chars().take(500).collect();
                if status_u16 == 401 {
                    error!(
                        endpoint,
                        model = %model,
                        account_id = account.db_id,
                        email = %account_email,
                        attempt = _attempt + 1,
                        body = %err_body_short,
                        "401 ← 上游返回错误"
                    );
                } else {
                    warn!(
                        endpoint,
                        model = %model,
                        account_id = account.db_id,
                        email = %account_email,
                        attempt = _attempt + 1,
                        body = %err_body_short,
                        "{status_u16} ← 上游返回错误"
                    );
                }

                // 记录错误请求日志（直接 send 到 log channel，无需 spawn）
                send_usage_log(
                    &state, account.db_id, endpoint, &model,
                    status_u16 as i64, duration, is_stream, &account_email,
                    &UsageInfo { input_tokens: 0, output_tokens: 0, reasoning_tokens: 0, cached_tokens: 0, total_tokens: 0 },
                    0, &reasoning_effort, "",
                ).await;

                match status_u16 {
                    401 => {
                        account.report_failure(FailureType::Unauthorized);
                        // 检查是否开启自动清理 401 账号
                        let auto_clean = state.db_settings_cache.read()
                            .map(|s| s.auto_clean_unauthorized)
                            .unwrap_or(false);
                        if auto_clean {
                            warn!(account_id = account.db_id, "账号 401，自动清理");
                            let db = state.db();
                            let aid = account.db_id;
                            tokio::spawn(async move {
                                let _ = crate::db::queries::delete_account(&db, aid).await;
                            });
                            state.scheduler.remove_account(account.db_id);
                        } else {
                            state.scheduler.mark_banned(&account);
                            warn!(account_id = account.db_id, "账号 401 banned");
                        }
                        exclude_set.insert(account.db_id);
                        last_error = format!("401: {}", error_body);
                    }
                    429 => {
                        account.report_failure(FailureType::RateLimited);
                        // 首次 429 时记录 resets_at（上游用量重置时间）
                        if account.resets_at.load(std::sync::atomic::Ordering::Relaxed) == 0 {
                            if let Ok(body_json) = serde_json::from_str::<Value>(&error_body) {
                                if let Some(ts) = body_json.pointer("/error/resets_at").and_then(|v| v.as_i64()) {
                                    account.resets_at.store(ts, std::sync::atomic::Ordering::Relaxed);
                                    // 异步持久化到数据库
                                    let db = state.db();
                                    let aid = account.db_id;
                                    tokio::spawn(async move {
                                        let _ = crate::db::queries::update_account_resets_at(&db, aid, ts).await;
                                    });
                                    info!(account_id = account.db_id, resets_at = ts, "记录用量重置时间");
                                }
                            }
                        }
                        let auto_clean = state.db_settings_cache.read()
                            .map(|s| s.auto_clean_rate_limited)
                            .unwrap_or(false);
                        if auto_clean {
                            warn!(account_id = account.db_id, "账号 429，自动清理");
                            let db = state.db();
                            let aid = account.db_id;
                            tokio::spawn(async move {
                                let _ = crate::db::queries::delete_account(&db, aid).await;
                            });
                            state.scheduler.remove_account(account.db_id);
                        } else {
                            let cooldown =
                                parse_rate_limit_cooldown(&resp_headers, &error_body, &account);
                            state
                                .scheduler
                                .mark_cooldown(&account, "rate_limited", cooldown);
                            warn!(account_id = account.db_id, cooldown, "账号 429");
                        }
                        exclude_set.insert(account.db_id);
                        last_error = format!("429: {}", error_body);
                    }
                    500..=599 => {
                        account.report_failure(FailureType::ServerError);
                        state.scheduler.recompute_health(&account);
                        exclude_set.insert(account.db_id);
                        last_error = format!("{}: {}", status, error_body);
                    }
                    _ => {
                        // 4xx 客户端错误不重试
                        account.report_failure(FailureType::Other);
                        state.scheduler.recompute_health(&account);
                        return error_response(status, &error_body);
                    }
                }

                state.scheduler.notify_available();
            }
            Err(e) => {
                account.release();
                let failure = if e.is_timeout() {
                    FailureType::Timeout
                } else {
                    FailureType::Other
                };
                account.report_failure(failure);
                state.scheduler.recompute_health(&account);
                exclude_set.insert(account.db_id);
                last_error = format!("{}", e);

                // 记录网络/超时错误日志（直接 send，无需 spawn）
                let duration = request_start.elapsed().as_millis() as i64;
                send_usage_log(
                    &state, account.db_id, endpoint, &model,
                    499, duration, is_stream, &account_email,
                    &UsageInfo { input_tokens: 0, output_tokens: 0, reasoning_tokens: 0, cached_tokens: 0, total_tokens: 0 },
                    0, &reasoning_effort, "",
                ).await;

                if e.is_timeout() {
                    warn!(account_id = account.db_id, "超时");
                } else {
                    error!(account_id = account.db_id, error = %e, "传输错误");
                }
                state.scheduler.notify_available();
            }
        }
    }

    error_response(
        StatusCode::BAD_GATEWAY,
        &format!("重试耗尽: {}", last_error),
    )
}

/// Peek 首 chunk 的结果
enum PeekResult {
    /// 成功读到有效数据
    Data(Bytes),
    /// 上游 SSE 流中包含错误事件（response.failed 等）
    UpstreamError(String),
    /// 流直接结束（空流）
    Empty,
    /// 网络层错误
    NetworkError(String),
}

/// 从上游流中 peek 第一个 chunk，判断是否为有效数据
///
/// 参照 Go 项目 openai_responses_handlers.go:197-244
/// 在返回 SSE 响应之前验证上游是否真正开始输出数据
async fn peek_first_chunk(
    stream: &mut (impl Stream<Item = Result<Bytes, reqwest::Error>> + Unpin),
) -> PeekResult {
    // 读取第一个 chunk（可能需要多个 chunk 才能凑齐一个完整 SSE 事件）
    let mut buf = Vec::new();

    while let Some(result) = stream.next().await {
        match result {
            Ok(data) => {
                buf.extend_from_slice(&data);
                let text = String::from_utf8_lossy(&buf);

                // 检查是否包含完整的 SSE data 行
                let mut has_data_line = false;
                for line in text.lines() {
                    if let Some(json_str) = line.strip_prefix("data: ") {
                        if json_str == "[DONE]" {
                            continue;
                        }
                        has_data_line = true;
                        if let Some(error_msg) = translator::parse_sse_error(json_str) {
                            return PeekResult::UpstreamError(error_msg);
                        }
                    }
                }

                // 有完整 data 行且无错误 → 返回；否则继续读取
                if has_data_line {
                    return PeekResult::Data(Bytes::from(buf));
                }
            }
            Err(e) => {
                return PeekResult::NetworkError(e.to_string());
            }
        }
    }

    PeekResult::Empty
}

/// 流式响应转发（带 TTFT 追踪 + usage 提取 + 客户端断连检测 + 心跳）
///
/// 接收已 peek 过的 first_chunk 和剩余 stream
async fn stream_response_with_tracking(
    first_chunk: Bytes,
    remaining_stream: impl Stream<Item = Result<Bytes, reqwest::Error>> + Send + Unpin + 'static,
    translate: bool,
    state: Arc<AppState>,
    account_id: i64,
    endpoint: &str,
    model: &str,
    email: &str,
    reasoning_effort: &str,
    request_start: Instant,
) -> Response {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(256);

    let endpoint = endpoint.to_string();
    let model = model.to_string();
    let email = email.to_string();
    let effort = reasoning_effort.to_string();

    tokio::spawn(async move {
        let mut translator = StreamTranslator::new();
        let mut first_token_time: Option<Instant> = None;
        let mut wrote_any_body = false;
        let mut client_gone = false;

        // 处理已 peek 过的第一个 chunk
        let process_chunk = |translator: &mut StreamTranslator,
                             data: &Bytes,
                             translate: bool,
                             first_token_time: &mut Option<Instant>|
         -> Result<Vec<u8>, Bytes> {
            if translate {
                match translator.translate_chunk(data) {
                    Ok(translated) => {
                        if translator.first_delta_received && first_token_time.is_none() {
                            *first_token_time = Some(Instant::now());
                        }
                        Ok(translated)
                    }
                    Err(_) => Err(data.clone()),
                }
            } else {
                translator.track_raw_chunk(data);
                if translator.first_delta_received && first_token_time.is_none() {
                    *first_token_time = Some(Instant::now());
                }
                Err(data.clone()) // passthrough
            }
        };

        // 发送第一个 chunk（TTFT 从此刻更准确）
        match process_chunk(&mut translator, &first_chunk, translate, &mut first_token_time) {
            Ok(translated) => {
                if !translated.is_empty() {
                    if tx.send(Ok(Bytes::from(translated))).await.is_err() {
                        client_gone = true;
                    }
                    wrote_any_body = true;
                }
            }
            Err(raw) => {
                if tx.send(Ok(raw)).await.is_err() {
                    client_gone = true;
                }
                wrote_any_body = true;
            }
        }

        // 处理剩余流（带心跳）
        if !client_gone {
            let mut stream = remaining_stream;
            let mut keepalive_timer = tokio::time::interval(Duration::from_secs(15));
            keepalive_timer.tick().await; // 消耗首次立即触发

            loop {
                tokio::select! {
                    chunk = stream.next() => {
                        match chunk {
                            Some(Ok(data)) => {
                                match process_chunk(&mut translator, &data, translate, &mut first_token_time) {
                                    Ok(translated) => {
                                        if !translated.is_empty() {
                                            if tx.send(Ok(Bytes::from(translated))).await.is_err() {
                                                client_gone = true;
                                                break;
                                            }
                                            wrote_any_body = true;
                                        }
                                    }
                                    Err(raw) => {
                                        if tx.send(Ok(raw)).await.is_err() {
                                            client_gone = true;
                                            break;
                                        }
                                        wrote_any_body = true;
                                    }
                                }
                            }
                            Some(Err(e)) => {
                                translator.stream_broken = true;
                                let err_msg = e.to_string();
                                if wrote_any_body {
                                    let _ = tx
                                        .send(Err(std::io::Error::new(std::io::ErrorKind::Other, e)))
                                        .await;
                                }
                                warn!("上游流异常中断: {err_msg}");
                                break;
                            }
                            None => break, // 流结束
                        }
                    }
                    _ = keepalive_timer.tick() => {
                        // SSE 心跳注释行，防止 thinking 阶段客户端超时
                        if tx.send(Ok(Bytes::from_static(b": keep-alive\n\n"))).await.is_err() {
                            client_gone = true;
                            break;
                        }
                    }
                }
            }
        }

        // 冲刷 pending 缓冲（处理流末尾卡在缓冲里的 response.completed）
        translator.flush_pending();

        // 流正常结束但未收到 completed 事件 → 标记为断流
        if !translator.completed && !client_gone {
            translator.stream_broken = true;
        }

        // 计算指标
        let first_token_ms = first_token_time
            .map(|t| t.duration_since(request_start).as_millis() as i64)
            .unwrap_or(0);

        let duration = request_start.elapsed().as_millis() as i64;

        let (usage, log_status) = if client_gone {
            // 客户端断连 → 499
            let u = translator.usage.clone().unwrap_or_else(|| translator.estimate_tokens_on_break());
            (u, 499)
        } else if translator.completed && translator.usage.is_some() {
            // 完整完成 → 200
            (translator.usage.clone().unwrap(), 200)
        } else {
            // 断流（上游中断 / 未收到 completed）→ 206
            (translator.estimate_tokens_on_break(), 206)
        };

        let service_tier = translator.service_tier.clone();

        // 参照 Go 的 usage_helpers.go:72-74：全 0 token 且非失败 → 跳过无意义记录
        let is_empty_usage = usage.input_tokens == 0
            && usage.output_tokens == 0
            && usage.reasoning_tokens == 0
            && usage.cached_tokens == 0;

        if is_empty_usage && log_status == 206 {
            warn!("断流且无 token 数据，跳过 usage 记录");
        } else {
            send_usage_log(
                &state, account_id, &endpoint, &model,
                log_status as i64, duration, true, &email,
                &usage, first_token_ms, &effort, &service_tier,
            )
            .await;
        }
    });

    let stream = ReceiverStream::new(rx);
    let body = Body::from_stream(stream);

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/event-stream")
        .header("Cache-Control", "no-cache")
        .header("Connection", "keep-alive")
        .header("X-Accel-Buffering", "no")
        .body(body)
        .unwrap()
}

/// sync 模式 — 读取 SSE 流收集完整响应，一次性返回 JSON
async fn collect_sync_response(
    resp: reqwest::Response,
    state: Arc<AppState>,
    account_id: i64,
    endpoint: &str,
    model: &str,
    email: &str,
    reasoning_effort: &str,
    request_start: Instant,
) -> Response {
    let mut translator = StreamTranslator::new();
    let mut stream = resp.bytes_stream();
    let mut first_token_time: Option<Instant> = None;
    let mut last_completed_data: Option<Vec<u8>> = None;

    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(data) => {
                translator.track_raw_chunk(&data);
                if translator.first_delta_received && first_token_time.is_none() {
                    first_token_time = Some(Instant::now());
                }
                // 保存包含 response.completed 的 SSE 事件
                if translator.completed {
                    last_completed_data = Some(data.to_vec());
                    break;
                }
            }
            Err(_) => break,
        }
    }

    // 冲刷 pending 缓冲
    translator.flush_pending();

    let first_token_ms = first_token_time
        .map(|t| t.duration_since(request_start).as_millis() as i64)
        .unwrap_or(0);
    let duration = request_start.elapsed().as_millis() as i64;

    let usage = translator.usage.clone().unwrap_or_else(|| translator.estimate_tokens_on_break());
    let service_tier = translator.service_tier.clone();

    let endpoint = endpoint.to_string();
    let model = model.to_string();
    let email = email.to_string();
    let effort = reasoning_effort.to_string();

    tokio::spawn({
        let state = state.clone();
        let endpoint = endpoint.clone();
        let model = model.clone();
        let email = email.clone();
        let effort = effort.clone();
        let usage = usage.clone();
        async move {
            send_usage_log(
                &state, account_id, &endpoint, &model,
                200, duration, false, &email,
                &usage, first_token_ms, &effort, &service_tier,
            ).await;
        }
    });

    // 从 completed 事件中提取完整响应 JSON 返回给客户端
    let body_bytes = if let Some(raw) = last_completed_data {
        // SSE 行格式: "data: {...}\n\n"，提取 JSON 中的 response 字段
        let text = String::from_utf8_lossy(&raw);
        let mut result = Vec::new();
        for line in text.lines() {
            if let Some(json_str) = line.strip_prefix("data: ") {
                if let Ok(event) = serde_json::from_str::<Value>(json_str) {
                    if let Some(resp_obj) = event.get("response") {
                        result = serde_json::to_vec(resp_obj).unwrap_or_default();
                        break;
                    }
                }
            }
        }
        if result.is_empty() {
            // 回退：直接用 pending 中缓存的最后完整行
            b"{}".to_vec()
        } else {
            result
        }
    } else {
        b"{}".to_vec()
    };

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Body::from(body_bytes))
        .unwrap()
}

// ─── 辅助函数 ───

/// 获取或创建 HTTP Client（按 proxy_url 池化复用，避免重复 TLS 握手）
pub(crate) fn get_or_create_client(state: &AppState, account_proxy: &str) -> reqwest::Client {
    let proxy_key = if !account_proxy.is_empty() {
        account_proxy.to_string()
    } else {
        state.config.proxy_url.clone().unwrap_or_default()
    };

    // 命中缓存 → 直接复用（reqwest::Client 内部 Arc，clone 极轻量）
    if let Some(client) = state.http_clients.get(&proxy_key) {
        return client.clone();
    }

    // 创建新 Client，优化连接池参数
    let mut builder = reqwest::Client::builder()
        .pool_max_idle_per_host(20)
        .pool_idle_timeout(Duration::from_secs(300))
        .connect_timeout(Duration::from_secs(10))
        .tcp_keepalive(Duration::from_secs(60))
        .tcp_nodelay(true);

    if !proxy_key.is_empty() {
        if let Ok(proxy) = reqwest::Proxy::all(&proxy_key) {
            builder = builder.proxy(proxy);
        }
    }

    let client = builder.build().unwrap_or_else(|_| reqwest::Client::new());
    state.http_clients.insert(proxy_key, client.clone());
    client
}

/// 解析 429 冷却时间 — 按 plan 和响应 header/body 智能判断
pub(crate) fn parse_rate_limit_cooldown(
    headers: &HeaderMap,
    error_body: &str,
    account: &crate::scheduler::Account,
) -> i64 {
    // 尝试从 header 获取 reset
    if let Some(val) = headers.get("x-ratelimit-reset-requests") {
        if let Ok(s) = val.to_str() {
            if let Ok(ts) = s.parse::<i64>() {
                let now = chrono::Utc::now().timestamp();
                return (ts - now).max(60);
            }
        }
    }

    // 尝试从响应体解析 resets_at
    if let Ok(body) = serde_json::from_str::<Value>(error_body) {
        // resets_in_seconds
        if let Some(secs) = body.get("resets_in_seconds").and_then(|v| v.as_i64()) {
            return secs.max(60);
        }
        // resets_at ISO 时间
        if let Some(at) = body.get("resets_at").and_then(|v| v.as_str()) {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(at) {
                let now = chrono::Utc::now().timestamp();
                return (dt.timestamp() - now).max(60);
            }
        }
    }

    // 检查 dual-window headers
    let primary = headers
        .get("x-codex-primary-used-percent")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let primary_window_min = headers
        .get("x-codex-primary-window-minutes")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(300.0); // 默认 5h
    let secondary = headers
        .get("x-codex-secondary-used-percent")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);
    let secondary_window_min = headers
        .get("x-codex-secondary-window-minutes")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(10080.0); // 默认 7d

    let window_to_cooldown = |min: f64| -> i64 {
        if min >= 1440.0 { 7 * 24 * 3600 }
        else if min >= 60.0 { 5 * 3600 }
        else { 1800 }
    };

    if primary >= 100.0 && secondary >= 100.0 {
        return window_to_cooldown(primary_window_min.max(secondary_window_min));
    }
    if primary >= 100.0 {
        return window_to_cooldown(primary_window_min);
    }
    if secondary >= 100.0 {
        return window_to_cooldown(secondary_window_min);
    }

    // fallback: 按 plan_type
    let plan = account.plan_type.read();
    match plan.as_str() {
        "free" => 7 * 24 * 3600,
        _ => 3600,
    }
}

/// 解析会话连续性 key（参考 codex2api ResolveContinuity）
/// 优先级：prompt_cache_key > 下游 API Key > 账号 ID
fn resolve_session_id(body: &Value, downstream_headers: &HeaderMap, account_id: &str) -> String {
    // 1. 最高优先级：请求体中的 prompt_cache_key
    if let Some(key) = body.get("prompt_cache_key").and_then(|v| v.as_str()) {
        if !key.is_empty() {
            return key.to_string();
        }
    }

    // 2. 下游 Authorization header 中的 API Key（client_principal）
    if let Some(auth) = downstream_headers.get("Authorization").and_then(|v| v.to_str().ok()) {
        let api_key = auth.strip_prefix("Bearer ").unwrap_or(auth).trim();
        if !api_key.is_empty() {
            let seed = format!("codex2api:prompt-cache:{}", api_key);
            return uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, seed.as_bytes()).to_string();
        }
    }

    // 3. 兜底：基于账号 ID 生成确定性 UUID
    let seed = format!("codex2api:prompt-cache:auth:{}", account_id);
    uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, seed.as_bytes()).to_string()
}

/// 从上游响应 header 提取用量百分比并更新到 Account
///
/// 上游每次响应都会返回：
/// - `x-codex-primary-used-percent` → 5h 窗口用量百分比
/// - `x-codex-secondary-used-percent` → 7d 窗口用量百分比
pub(crate) fn update_usage_from_headers(account: &crate::scheduler::Account, headers: &HeaderMap) {
    if let Some(primary) = headers
        .get("x-codex-primary-used-percent")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<f64>().ok())
    {
        // 5h 窗口 → usage_5h_pct_100（×100 存储）
        account
            .usage_5h_pct_100
            .store((primary * 100.0) as i64, std::sync::atomic::Ordering::Relaxed);
    }
    if let Some(secondary) = headers
        .get("x-codex-secondary-used-percent")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<f64>().ok())
    {
        // 7d 窗口 → usage_7d_pct_100（×100 存储）
        account
            .usage_7d_pct_100
            .store((secondary * 100.0) as i64, std::sync::atomic::Ordering::Relaxed);
    }
}

fn error_response(status: StatusCode, message: &str) -> Response {
    #[derive(Serialize)]
    struct ErrorResp<'a> {
        error: ErrorBody<'a>,
    }
    #[derive(Serialize)]
    struct ErrorBody<'a> {
        message: &'a str,
        #[serde(rename = "type")]
        error_type: &'static str,
        code: u16,
    }

    let body = ErrorResp {
        error: ErrorBody {
            message,
            error_type: "proxy_error",
            code: status.as_u16(),
        },
    };
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

/// 发送使用日志到异步写入通道
async fn send_usage_log(
    state: &AppState,
    account_id: i64,
    endpoint: &str,
    model: &str,
    status_code: i64,
    duration_ms: i64,
    stream: bool,
    email: &str,
    usage: &UsageInfo,
    first_token_ms: i64,
    reasoning_effort: &str,
    service_tier: &str,
) {
    use crate::db::models::UsageLog;
    let log = UsageLog {
        id: 0,
        account_id,
        endpoint: endpoint.to_string(),
        model: model.to_string(),
        prompt_tokens: usage.input_tokens,
        completion_tokens: usage.output_tokens,
        total_tokens: usage.total_tokens,
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        reasoning_tokens: usage.reasoning_tokens,
        cached_tokens: usage.cached_tokens,
        first_token_ms,
        reasoning_effort: reasoning_effort.to_string(),
        status_code,
        duration_ms,
        stream,
        service_tier: service_tier.to_string(),
        account_email: email.to_string(),
        created_at: String::new(),
    };
    let _ = state.log_sender.send(log).await;
}
