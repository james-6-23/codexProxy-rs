use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

// ─── Codex 不支持的字段（传了会 400）───

const UNSUPPORTED_FIELDS: &[&str] = &[
    "temperature", "top_p", "frequency_penalty", "presence_penalty",
    "logprobs", "top_logprobs", "n", "seed", "stop", "user",
    "logit_bias", "response_format", "stream_options",
    "truncation", "context_management",
    "max_output_tokens", "max_tokens", "max_completion_tokens",
];

// ─── Tool schema 中需要移除的 JSON Schema 关键字 ───

const UNSUPPORTED_SCHEMA_KEYS: &[&str] = &[
    "uniqueItems", "minItems", "maxItems", "minimum", "maximum",
    "exclusiveMinimum", "exclusiveMaximum", "pattern", "format",
    "minLength", "maxLength", "multipleOf", "const", "examples",
    "default", "nullable", "$schema", "$id", "$ref",
    "additionalItems", "patternProperties", "dependencies",
    "if", "then", "else", "allOf", "anyOf", "oneOf", "not",
];

// ─── 输出侧类型化结构体（零堆分配序列化）───

#[derive(Serialize)]
struct ChatChunk<'a> {
    id: &'a str,
    object: &'static str,
    choices: [ChunkChoice<'a>; 1],
    #[serde(skip_serializing_if = "Option::is_none")]
    usage: Option<ChunkUsage>,
}

#[derive(Serialize)]
struct ChunkChoice<'a> {
    index: u32,
    delta: ChunkDelta<'a>,
    finish_reason: Option<&'a str>,
}

#[derive(Serialize)]
struct ChunkDelta<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<[ToolCallChunk<'a>; 1]>,
}

#[derive(Serialize)]
struct ToolCallChunk<'a> {
    index: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<&'a str>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    call_type: Option<&'static str>,
    function: ToolCallFunc<'a>,
}

#[derive(Serialize)]
struct ToolCallFunc<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
    arguments: &'a str,
}

#[derive(Serialize)]
struct ChunkUsage {
    prompt_tokens: i64,
    completion_tokens: i64,
    total_tokens: i64,
    reasoning_tokens: i64,
    cached_tokens: i64,
}

// ─── 输入侧类型化结构体（零拷贝反序列化）───

/// SSE 事件通用结构 — 所有字段 Option，按 event_type 取用
#[derive(Deserialize)]
struct SseEvent<'a> {
    #[serde(rename = "type")]
    event_type: &'a str,
    #[serde(default)]
    response_id: Option<&'a str>,
    #[serde(default)]
    delta: Option<&'a str>,
    #[serde(default)]
    item_id: Option<&'a str>,
    #[serde(default, borrow)]
    item: Option<SseItem<'a>>,
    #[serde(default, borrow)]
    response: Option<SseResponse<'a>>,
}

#[derive(Deserialize)]
struct SseItem<'a> {
    #[serde(default)]
    id: Option<&'a str>,
    #[serde(rename = "type", default)]
    item_type: Option<&'a str>,
    #[serde(default)]
    call_id: Option<&'a str>,
    #[serde(default)]
    name: Option<&'a str>,
}

#[derive(Deserialize)]
struct SseResponse<'a> {
    #[serde(default)]
    id: Option<&'a str>,
    #[serde(default)]
    service_tier: Option<&'a str>,
    #[serde(default)]
    usage: Option<UsageRaw>,
    #[serde(default, borrow)]
    status_details: Option<StatusDetailsRaw<'a>>,
}

#[derive(Deserialize)]
struct UsageRaw {
    #[serde(default)]
    input_tokens: Option<i64>,
    #[serde(default)]
    output_tokens: Option<i64>,
    #[serde(default)]
    output_tokens_details: Option<OutputTokensDetails>,
    #[serde(default)]
    input_tokens_details: Option<InputTokensDetails>,
}

#[derive(Deserialize)]
struct OutputTokensDetails {
    #[serde(default)]
    reasoning_tokens: Option<i64>,
}

#[derive(Deserialize)]
struct InputTokensDetails {
    #[serde(default)]
    cached_tokens: Option<i64>,
}

#[derive(Deserialize)]
struct StatusDetailsRaw<'a> {
    #[serde(default, borrow)]
    error: Option<ErrorDetailRaw<'a>>,
}

#[derive(Deserialize)]
struct ErrorDetailRaw<'a> {
    #[serde(default)]
    message: Option<&'a str>,
}

// ─── 请求翻译 ───

/// 将 OpenAI Chat Completions 格式翻译为 Codex Responses 格式
pub fn translate_chat_to_responses(chat_body: &Value) -> Value {
    let mut body = json!({});

    // model（必需）
    if let Some(model) = chat_body.get("model") {
        body["model"] = model.clone();
    }

    // messages → input
    if let Some(messages) = chat_body.get("messages").and_then(|v| v.as_array()) {
        body["input"] = convert_messages_to_input(messages);
    } else if let Some(input) = chat_body.get("input") {
        // 如果 input 是字符串，自动包装为数组
        if input.is_string() {
            body["input"] = json!([{
                "role": "user",
                "content": input,
            }]);
        } else {
            body["input"] = input.clone();
        }
    }

    // stream + store（Codex 强制要求）
    body["stream"] = Value::Bool(true);
    body["store"] = Value::Bool(false);

    // include（获取 reasoning 内容必需）
    body["include"] = json!(["reasoning.encrypted_content"]);

    // reasoning_effort → reasoning.effort（带合法值钳制）
    if let Some(effort) = chat_body.get("reasoning_effort").and_then(|v| v.as_str()) {
        let clamped = clamp_reasoning_effort(effort);
        body["reasoning"] = json!({ "effort": clamped });
    }

    // tools（需要净化 schema）
    if let Some(tools) = chat_body.get("tools").and_then(|v| v.as_array()) {
        body["tools"] = Value::Array(sanitize_tools(tools));
    }

    // service_tier（兼容两种字段名）
    if let Some(tier) = chat_body.get("service_tier").or_else(|| chat_body.get("serviceTier")) {
        if let Some(s) = tier.as_str() {
            let valid = match s {
                "auto" | "default" | "flex" | "priority" | "scale" => s,
                _ => "auto",
            };
            body["service_tier"] = Value::String(valid.to_string());
        }
    }

    body
}

/// reasoning_effort 合法值钳制
fn clamp_reasoning_effort(effort: &str) -> &str {
    match effort {
        "low" | "medium" | "high" => effort,
        "xhigh" => "xhigh",
        "none" | "min" => "low",
        "max" => "high",
        _ => "medium",
    }
}

/// 将 messages 数组转换为 Codex input 格式
fn convert_messages_to_input(messages: &[Value]) -> Value {
    let mut input = Vec::new();

    for msg in messages {
        let role = msg.get("role").and_then(|v| v.as_str()).unwrap_or("user");

        match role {
            "system" | "developer" => {
                let mut item = json!({"role": "developer"});
                if let Some(c) = msg.get("content") {
                    item["content"] = c.clone();
                }
                input.push(item);
            }
            "tool" => {
                input.push(json!({
                    "type": "function_call_output",
                    "call_id": msg.get("tool_call_id").unwrap_or(&Value::Null),
                    "output": msg.get("content").and_then(|v| v.as_str()).unwrap_or(""),
                }));
            }
            "assistant" => {
                if let Some(tool_calls) = msg.get("tool_calls").and_then(|v| v.as_array()) {
                    for tc in tool_calls {
                        let func = tc.get("function").unwrap_or(&Value::Null);
                        input.push(json!({
                            "type": "function_call",
                            "call_id": tc.get("id").unwrap_or(&Value::Null),
                            "name": func.get("name").unwrap_or(&Value::Null),
                            "arguments": func.get("arguments").and_then(|v| v.as_str()).unwrap_or("{}"),
                        }));
                    }
                }
                if let Some(content) = msg.get("content") {
                    if !content.is_null() {
                        let mut item = json!({"role": "assistant"});
                        item["content"] = content.clone();
                        input.push(item);
                    }
                }
            }
            _ => {
                let mut item = json!({"role": role});
                if let Some(c) = msg.get("content") {
                    item["content"] = c.clone();
                }
                input.push(item);
            }
        }
    }

    Value::Array(input)
}

/// 净化 tools — 移除不支持的 JSON Schema 关键字，补全缺失的 description
fn sanitize_tools(tools: &[Value]) -> Vec<Value> {
    tools
        .iter()
        .map(|tool| {
            let mut t = tool.clone();
            if let Some(func) = t.get_mut("function").and_then(|v| v.as_object_mut()) {
                // 确保有 description
                if !func.contains_key("description") || func["description"].is_null() {
                    let name = func.get("name").and_then(|v| v.as_str()).unwrap_or("tool");
                    func.insert(
                        "description".to_string(),
                        Value::String(format!("Execute {}", name)),
                    );
                }
                // 净化 parameters schema
                if let Some(params) = func.get_mut("parameters") {
                    strip_unsupported_schema_keys(params);
                }
            }
            t
        })
        .collect()
}

/// 递归移除 JSON Schema 中不支持的关键字
fn strip_unsupported_schema_keys(schema: &mut Value) {
    if let Some(obj) = schema.as_object_mut() {
        for key in UNSUPPORTED_SCHEMA_KEYS {
            obj.remove(*key);
        }
        if let Some(props) = obj.get_mut("properties").and_then(|v| v.as_object_mut()) {
            for (_, prop_val) in props.iter_mut() {
                strip_unsupported_schema_keys(prop_val);
            }
        }
        if let Some(items) = obj.get_mut("items") {
            strip_unsupported_schema_keys(items);
        }
    }
}

/// 清理请求体中 Codex 不支持的字段
pub fn strip_unsupported_fields(body: &mut Value) {
    if let Some(obj) = body.as_object_mut() {
        for field in UNSUPPORTED_FIELDS {
            obj.remove(*field);
        }
        obj.remove("previous_response_id");
        obj.remove("prompt_cache_retention");
        obj.remove("safety_identifier");
        obj.remove("disable_response_storage");
    }
}

// ─── 响应翻译 ───

/// 从 Codex 响应中提取 usage 详细信息
#[derive(Clone)]
pub struct UsageInfo {
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub reasoning_tokens: i64,
    pub cached_tokens: i64,
    pub total_tokens: i64,
}

/// 从 Codex 响应 JSON 提取 usage（供 translate_response_to_chat 使用）
pub fn extract_usage(resp: &Value) -> UsageInfo {
    let usage = resp.get("usage").unwrap_or(&Value::Null);
    let input = usage.get("input_tokens").and_then(|v| v.as_i64()).unwrap_or(0);
    let output = usage.get("output_tokens").and_then(|v| v.as_i64()).unwrap_or(0);

    let reasoning = usage
        .get("output_tokens_details")
        .and_then(|d| d.get("reasoning_tokens"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    let cached = usage
        .get("input_tokens_details")
        .and_then(|d| d.get("cached_tokens"))
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    UsageInfo {
        input_tokens: input,
        output_tokens: output,
        reasoning_tokens: reasoning,
        cached_tokens: cached,
        total_tokens: input + output,
    }
}

/// 从类型化 UsageRaw 提取 UsageInfo（零拷贝路径）
fn extract_usage_from_raw(raw: &Option<UsageRaw>) -> UsageInfo {
    match raw {
        Some(u) => {
            let input = u.input_tokens.unwrap_or(0);
            let output = u.output_tokens.unwrap_or(0);
            let reasoning = u.output_tokens_details.as_ref()
                .and_then(|d| d.reasoning_tokens).unwrap_or(0);
            let cached = u.input_tokens_details.as_ref()
                .and_then(|d| d.cached_tokens).unwrap_or(0);
            UsageInfo {
                input_tokens: input,
                output_tokens: output,
                reasoning_tokens: reasoning,
                cached_tokens: cached,
                total_tokens: input + output,
            }
        }
        None => UsageInfo {
            input_tokens: 0, output_tokens: 0, reasoning_tokens: 0,
            cached_tokens: 0, total_tokens: 0,
        },
    }
}

/// 将 Codex Responses 响应翻译为 OpenAI Chat Completions 格式
pub fn translate_response_to_chat(body: &[u8]) -> Result<(Vec<u8>, UsageInfo), anyhow::Error> {
    let resp: Value = serde_json::from_slice(body)?;

    let mut content = String::new();
    let mut tool_calls = Vec::new();

    if let Some(output) = resp.get("output").and_then(|v| v.as_array()) {
        for (idx, item) in output.iter().enumerate() {
            let item_type = item.get("type").and_then(|v| v.as_str()).unwrap_or("");
            match item_type {
                "message" => {
                    if let Some(c) = item.get("content").and_then(|v| v.as_array()) {
                        for part in c {
                            if let Some(text) = part.get("text").and_then(|v| v.as_str()) {
                                content.push_str(text);
                            }
                        }
                    }
                }
                "function_call" => {
                    tool_calls.push(json!({
                        "id": item.get("call_id").unwrap_or(&Value::Null),
                        "type": "function",
                        "index": idx,
                        "function": {
                            "name": item.get("name").unwrap_or(&Value::Null),
                            "arguments": item.get("arguments").unwrap_or(&Value::Null),
                        }
                    }));
                }
                _ => {}
            }
        }
    }

    let usage_info = extract_usage(&resp);

    let mut message = json!({
        "role": "assistant",
        "content": if content.is_empty() { Value::Null } else { Value::String(content) },
    });
    if !tool_calls.is_empty() {
        message["tool_calls"] = Value::Array(tool_calls);
    }

    let service_tier = resp.get("service_tier").and_then(|v| v.as_str()).unwrap_or("");

    let chat_resp = json!({
        "id": resp.get("id").unwrap_or(&Value::Null),
        "object": "chat.completion",
        "created": chrono::Utc::now().timestamp(),
        "model": resp.get("model").unwrap_or(&Value::Null),
        "service_tier": service_tier,
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": "stop",
        }],
        "usage": {
            "prompt_tokens": usage_info.input_tokens,
            "completion_tokens": usage_info.output_tokens,
            "total_tokens": usage_info.total_tokens,
            "reasoning_tokens": usage_info.reasoning_tokens,
            "cached_tokens": usage_info.cached_tokens,
        },
    });

    Ok((serde_json::to_vec(&chat_resp)?, usage_info))
}

// ─── 流式翻译（有状态）───

/// 流式翻译器 — 维护 tool_call 索引映射等状态
pub struct StreamTranslator {
    /// tool_call item_id → OpenAI 格式的 index
    tool_call_indices: std::collections::HashMap<String, usize>,
    next_tool_index: usize,
    /// 是否已收到第一个 delta（用于 TTFT 追踪）
    pub first_delta_received: bool,
    /// 是否收到了终止事件
    pub completed: bool,
    /// 上游流是否异常中断（网络错误、未收到 completed 就结束）
    pub stream_broken: bool,
    /// 累积 delta 字符数（用于 token 估算）
    pub delta_chars: usize,
    /// 从 response.completed 提取的 usage
    pub usage: Option<UsageInfo>,
    /// service_tier
    pub service_tier: String,
    /// 跨 TCP chunk 的行缓冲（SSE 事件可能跨 chunk 拆分）
    pending: String,
}

impl StreamTranslator {
    pub fn new() -> Self {
        Self {
            tool_call_indices: std::collections::HashMap::new(),
            next_tool_index: 0,
            first_delta_received: false,
            completed: false,
            stream_broken: false,
            delta_chars: 0,
            usage: None,
            service_tier: String::new(),
            pending: String::new(),
        }
    }

    /// 从 pending 缓冲中提取完整的行，返回完整行列表
    fn drain_lines(&mut self, new_data: &str) -> Vec<String> {
        self.pending.push_str(new_data);
        let mut lines = Vec::new();
        let mut start = 0;

        while let Some(rel_pos) = self.pending[start..].find('\n') {
            let end = start + rel_pos;
            let line = self.pending[start..end].trim_end_matches('\r').to_string();
            lines.push(line);
            start = end + 1;
        }

        if start > 0 {
            self.pending.drain(..start);
        }

        lines
    }

    /// 从 SSE 事件 JSON 更新内部状态（delta 字符数、usage、completed）
    fn update_state_from_event(&mut self, json_str: &str) {
        if let Ok(event) = serde_json::from_str::<SseEvent>(json_str) {
            match event.event_type {
                "response.output_text.delta" => {
                    self.first_delta_received = true;
                    self.delta_chars += event.delta.map(|s| s.len()).unwrap_or(0);
                }
                "response.completed" => {
                    self.completed = true;
                    if let Some(ref resp) = event.response {
                        self.usage = Some(extract_usage_from_raw(&resp.usage));
                        self.service_tier = resp.service_tier.unwrap_or("").to_string();
                    }
                }
                "response.failed" => {
                    self.completed = true;
                }
                _ => {}
            }
        }
    }

    /// 流结束后冲刷 pending 缓冲中残留的数据
    pub fn flush_pending(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let remaining = std::mem::take(&mut self.pending);
        for line in remaining.lines() {
            let line = line.trim();
            if let Some(json_str) = line.strip_prefix("data: ")
                && json_str != "[DONE]"
            {
                self.update_state_from_event(json_str);
            }
        }
    }

    /// 翻译一个 SSE chunk，返回翻译后的 bytes
    pub fn translate_chunk(&mut self, data: &[u8]) -> Result<Vec<u8>, anyhow::Error> {
        let text = std::str::from_utf8(data)?;
        let lines = self.drain_lines(text);
        let mut output = Vec::with_capacity(256);

        for line in &lines {
            if let Some(json_str) = line.strip_prefix("data: ") {
                if json_str == "[DONE]" {
                    output.extend_from_slice(b"data: [DONE]\n\n");
                    continue;
                }

                match serde_json::from_str::<SseEvent>(json_str) {
                    Ok(event) => {
                        let rid = event.response_id.unwrap_or("");

                        match event.event_type {
                            // 文本 delta
                            "response.output_text.delta" => {
                                let delta_text = event.delta.unwrap_or("");
                                self.first_delta_received = true;
                                self.delta_chars += delta_text.len();

                                let chunk = ChatChunk {
                                    id: rid,
                                    object: "chat.completion.chunk",
                                    choices: [ChunkChoice {
                                        index: 0,
                                        delta: ChunkDelta {
                                            content: Some(delta_text),
                                            tool_calls: None,
                                        },
                                        finish_reason: None,
                                    }],
                                    usage: None,
                                };
                                output.extend_from_slice(b"data: ");
                                serde_json::to_writer(&mut output, &chunk)?;
                                output.extend_from_slice(b"\n\n");
                            }

                            // function_call 参数增量
                            "response.function_call_arguments.delta" => {
                                let item_id = event.item_id.unwrap_or("");
                                let delta_args = event.delta.unwrap_or("");
                                self.first_delta_received = true;

                                let idx = *self.tool_call_indices
                                    .entry(item_id.to_string())
                                    .or_insert_with(|| {
                                        let i = self.next_tool_index;
                                        self.next_tool_index += 1;
                                        i
                                    });

                                let chunk = ChatChunk {
                                    id: rid,
                                    object: "chat.completion.chunk",
                                    choices: [ChunkChoice {
                                        index: 0,
                                        delta: ChunkDelta {
                                            content: None,
                                            tool_calls: Some([ToolCallChunk {
                                                index: idx,
                                                id: None,
                                                call_type: None,
                                                function: ToolCallFunc {
                                                    name: None,
                                                    arguments: delta_args,
                                                },
                                            }]),
                                        },
                                        finish_reason: None,
                                    }],
                                    usage: None,
                                };
                                output.extend_from_slice(b"data: ");
                                serde_json::to_writer(&mut output, &chunk)?;
                                output.extend_from_slice(b"\n\n");
                            }

                            // function_call 创建（发送 name）
                            "response.output_item.added" => {
                                if let Some(ref item) = event.item
                                    && item.item_type == Some("function_call")
                                {
                                        let item_id = item.id.unwrap_or("");
                                        let name = item.name.unwrap_or("");
                                        let call_id = item.call_id.unwrap_or("");

                                        let idx = *self.tool_call_indices
                                            .entry(item_id.to_string())
                                            .or_insert_with(|| {
                                                let i = self.next_tool_index;
                                                self.next_tool_index += 1;
                                                i
                                            });

                                        let chunk = ChatChunk {
                                            id: rid,
                                            object: "chat.completion.chunk",
                                            choices: [ChunkChoice {
                                                index: 0,
                                                delta: ChunkDelta {
                                                    content: None,
                                                    tool_calls: Some([ToolCallChunk {
                                                        index: idx,
                                                        id: Some(call_id),
                                                        call_type: Some("function"),
                                                        function: ToolCallFunc {
                                                            name: Some(name),
                                                            arguments: "",
                                                        },
                                                    }]),
                                                },
                                                finish_reason: None,
                                            }],
                                            usage: None,
                                        };
                                        output.extend_from_slice(b"data: ");
                                        serde_json::to_writer(&mut output, &chunk)?;
                                        output.extend_from_slice(b"\n\n");
                                }
                            }

                            // 完成事件 — 提取 usage 和 service_tier
                            "response.completed" => {
                                self.completed = true;

                                if let Some(ref resp) = event.response {
                                    self.usage = Some(extract_usage_from_raw(&resp.usage));
                                    self.service_tier = resp.service_tier.unwrap_or("").to_string();
                                }

                                let usage_json = self.usage.as_ref().map(|u| ChunkUsage {
                                    prompt_tokens: u.input_tokens,
                                    completion_tokens: u.output_tokens,
                                    total_tokens: u.total_tokens,
                                    reasoning_tokens: u.reasoning_tokens,
                                    cached_tokens: u.cached_tokens,
                                }).unwrap_or(ChunkUsage {
                                    prompt_tokens: 0, completion_tokens: 0, total_tokens: 0,
                                    reasoning_tokens: 0, cached_tokens: 0,
                                });

                                let final_rid = event.response.as_ref()
                                    .and_then(|r| r.id)
                                    .unwrap_or(rid);

                                let chunk = ChatChunk {
                                    id: final_rid,
                                    object: "chat.completion.chunk",
                                    choices: [ChunkChoice {
                                        index: 0,
                                        delta: ChunkDelta {
                                            content: None,
                                            tool_calls: None,
                                        },
                                        finish_reason: Some("stop"),
                                    }],
                                    usage: Some(usage_json),
                                };
                                output.extend_from_slice(b"data: ");
                                serde_json::to_writer(&mut output, &chunk)?;
                                output.extend_from_slice(b"\n\n");
                                output.extend_from_slice(b"data: [DONE]\n\n");
                            }

                            // 其他事件透传
                            _ => {
                                output.extend_from_slice(line.as_bytes());
                                output.extend_from_slice(b"\n\n");
                            }
                        }
                    }
                    // JSON 解析失败 → 透传
                    Err(_) => {
                        output.extend_from_slice(line.as_bytes());
                        output.extend_from_slice(b"\n\n");
                    }
                }
            } else if !line.is_empty() {
                output.extend_from_slice(line.as_bytes());
                output.push(b'\n');
            }
        }

        Ok(output)
    }

    /// 在 passthrough 模式下解析 SSE 事件，提取 usage / TTFT / delta 字符数
    pub fn track_raw_chunk(&mut self, data: &[u8]) {
        let text = match std::str::from_utf8(data) {
            Ok(t) => t,
            Err(_) => return,
        };

        let lines = self.drain_lines(text);

        for line in &lines {
            if let Some(json_str) = line.strip_prefix("data: ")
                && json_str != "[DONE]"
            {
                self.update_state_from_event(json_str);
            }
        }
    }

    /// 如果流中断（未收到 completed），估算 token
    pub fn estimate_tokens_on_break(&self) -> UsageInfo {
        let estimated_output = (self.delta_chars / 3).max(1) as i64;
        UsageInfo {
            input_tokens: 0,
            output_tokens: estimated_output,
            reasoning_tokens: 0,
            cached_tokens: 0,
            total_tokens: estimated_output,
        }
    }
}

// ─── 工具函数 ───

/// 尝试解析 SSE 事件，如果是 response.failed 返回错误信息
pub fn parse_sse_error(json_str: &str) -> Option<String> {
    let event: SseEvent = serde_json::from_str(json_str).ok()?;
    if event.event_type == "response.failed" {
        let msg = event.response.as_ref()
            .and_then(|r| r.status_details.as_ref())
            .and_then(|d| d.error.as_ref())
            .and_then(|e| e.message)
            .unwrap_or("unknown upstream error");
        Some(msg.to_string())
    } else {
        None
    }
}
