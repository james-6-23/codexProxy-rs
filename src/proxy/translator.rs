use serde_json::{json, Value};

// ─── Codex 不支持的字段（传了会 400）───

const UNSUPPORTED_FIELDS: &[&str] = &[
    "temperature", "top_p", "frequency_penalty", "presence_penalty",
    "logprobs", "top_logprobs", "n", "seed", "stop", "user",
    "logit_bias", "response_format", "stream_options",
    "truncation", "context_management",
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

    // max_tokens / max_completion_tokens → max_output_tokens
    if let Some(max) = chat_body.get("max_completion_tokens").or_else(|| chat_body.get("max_tokens")) {
        body["max_output_tokens"] = max.clone();
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
                // system → developer
                let mut item = json!({"role": "developer"});
                if let Some(c) = msg.get("content") {
                    item["content"] = c.clone();
                }
                input.push(item);
            }
            "tool" => {
                // tool 响应 → function_call_output
                input.push(json!({
                    "type": "function_call_output",
                    "call_id": msg.get("tool_call_id").unwrap_or(&Value::Null),
                    "output": msg.get("content").and_then(|v| v.as_str()).unwrap_or(""),
                }));
            }
            "assistant" => {
                // assistant + tool_calls → function_call items
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
                // assistant 文本内容
                if let Some(content) = msg.get("content") {
                    if !content.is_null() {
                        let mut item = json!({"role": "assistant"});
                        item["content"] = content.clone();
                        input.push(item);
                    }
                }
            }
            _ => {
                // user 等角色直接映射
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
        // 递归处理 properties
        if let Some(props) = obj.get_mut("properties").and_then(|v| v.as_object_mut()) {
            for (_, prop_val) in props.iter_mut() {
                strip_unsupported_schema_keys(prop_val);
            }
        }
        // 递归处理 items
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
        // 清理可能导致上游报错的字段
        obj.remove("previous_response_id");
        obj.remove("prompt_cache_retention");
        obj.remove("safety_identifier");
        obj.remove("disable_response_storage");
    }
}

// ─── 响应翻译 ───

/// 从 Codex 响应中提取 usage 详细信息
pub struct UsageInfo {
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub reasoning_tokens: i64,
    pub cached_tokens: i64,
    pub total_tokens: i64,
}

/// 从 Codex 响应 JSON 提取 usage
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
            delta_chars: 0,
            usage: None,
            service_tier: String::new(),
            pending: String::new(),
        }
    }

    /// 从 pending 缓冲中提取完整的行，返回完整行列表
    /// 不完整的尾部数据保留在 pending 中
    fn drain_lines(&mut self, new_data: &str) -> Vec<String> {
        self.pending.push_str(new_data);
        let mut lines = Vec::new();

        loop {
            let Some(pos) = self.pending.find('\n') else {
                break;
            };
            let line = self.pending[..pos].trim_end_matches('\r').to_string();
            self.pending = self.pending[pos + 1..].to_string();
            lines.push(line);
        }

        lines
    }

    /// 翻译一个 SSE chunk，返回翻译后的 bytes
    pub fn translate_chunk(&mut self, data: &[u8]) -> Result<Vec<u8>, anyhow::Error> {
        let text = std::str::from_utf8(data)?;
        let lines = self.drain_lines(text);
        let mut output = Vec::new();

        for line in &lines {
            if let Some(json_str) = line.strip_prefix("data: ") {
                if json_str == "[DONE]" {
                    output.extend_from_slice(b"data: [DONE]\n\n");
                    continue;
                }

                if let Ok(event) = serde_json::from_str::<Value>(json_str) {
                    let event_type = event.get("type").and_then(|v| v.as_str()).unwrap_or("");
                    let response_id = event.get("response_id").unwrap_or(&Value::Null);

                    match event_type {
                        // 文本 delta
                        "response.output_text.delta" => {
                            let delta_text = event.get("delta").and_then(|v| v.as_str()).unwrap_or("");
                            self.first_delta_received = true;
                            self.delta_chars += delta_text.len();

                            let chunk = json!({
                                "id": response_id,
                                "object": "chat.completion.chunk",
                                "choices": [{
                                    "index": 0,
                                    "delta": { "content": delta_text },
                                    "finish_reason": Value::Null,
                                }],
                            });
                            output.extend_from_slice(
                                format!("data: {}\n\n", serde_json::to_string(&chunk)?).as_bytes(),
                            );
                        }

                        // function_call 参数增量
                        "response.function_call_arguments.delta" => {
                            let item_id = event.get("item_id").and_then(|v| v.as_str()).unwrap_or("");
                            let delta_args = event.get("delta").and_then(|v| v.as_str()).unwrap_or("");
                            self.first_delta_received = true;

                            let idx = *self.tool_call_indices.entry(item_id.to_string()).or_insert_with(|| {
                                let i = self.next_tool_index;
                                self.next_tool_index += 1;
                                i
                            });

                            let chunk = json!({
                                "id": response_id,
                                "object": "chat.completion.chunk",
                                "choices": [{
                                    "index": 0,
                                    "delta": {
                                        "tool_calls": [{
                                            "index": idx,
                                            "function": { "arguments": delta_args },
                                        }]
                                    },
                                    "finish_reason": Value::Null,
                                }],
                            });
                            output.extend_from_slice(
                                format!("data: {}\n\n", serde_json::to_string(&chunk)?).as_bytes(),
                            );
                        }

                        // function_call 创建（发送 name）
                        "response.output_item.added" => {
                            if let Some(item) = event.get("item") {
                                let item_type = item.get("type").and_then(|v| v.as_str()).unwrap_or("");
                                if item_type == "function_call" {
                                    let item_id = item.get("id").and_then(|v| v.as_str()).unwrap_or("");
                                    let name = item.get("name").and_then(|v| v.as_str()).unwrap_or("");
                                    let call_id = item.get("call_id").and_then(|v| v.as_str()).unwrap_or("");

                                    let idx = *self.tool_call_indices.entry(item_id.to_string()).or_insert_with(|| {
                                        let i = self.next_tool_index;
                                        self.next_tool_index += 1;
                                        i
                                    });

                                    let chunk = json!({
                                        "id": response_id,
                                        "object": "chat.completion.chunk",
                                        "choices": [{
                                            "index": 0,
                                            "delta": {
                                                "tool_calls": [{
                                                    "index": idx,
                                                    "id": call_id,
                                                    "type": "function",
                                                    "function": { "name": name, "arguments": "" },
                                                }]
                                            },
                                            "finish_reason": Value::Null,
                                        }],
                                    });
                                    output.extend_from_slice(
                                        format!("data: {}\n\n", serde_json::to_string(&chunk)?).as_bytes(),
                                    );
                                }
                            }
                        }

                        // 完成事件 — 提取 usage 和 service_tier
                        "response.completed" => {
                            self.completed = true;

                            if let Some(resp) = event.get("response") {
                                self.usage = Some(extract_usage(resp));
                                self.service_tier = resp
                                    .get("service_tier")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();
                            }

                            let usage_json = if let Some(ref u) = self.usage {
                                json!({
                                    "prompt_tokens": u.input_tokens,
                                    "completion_tokens": u.output_tokens,
                                    "total_tokens": u.total_tokens,
                                    "reasoning_tokens": u.reasoning_tokens,
                                    "cached_tokens": u.cached_tokens,
                                })
                            } else {
                                json!({})
                            };

                            let rid = event
                                .get("response")
                                .and_then(|r| r.get("id"))
                                .or(Some(response_id))
                                .unwrap_or(&Value::Null);

                            let chunk = json!({
                                "id": rid,
                                "object": "chat.completion.chunk",
                                "choices": [{
                                    "index": 0,
                                    "delta": {},
                                    "finish_reason": "stop",
                                }],
                                "usage": usage_json,
                            });
                            output.extend_from_slice(
                                format!("data: {}\n\n", serde_json::to_string(&chunk)?).as_bytes(),
                            );
                            output.extend_from_slice(b"data: [DONE]\n\n");
                        }

                        // 其他事件透传
                        _ => {
                            output.extend_from_slice(line.as_bytes());
                            output.extend_from_slice(b"\n\n");
                        }
                    }
                } else {
                    output.extend_from_slice(line.as_bytes());
                    output.extend_from_slice(b"\n\n");
                }
            } else if !line.is_empty() {
                output.extend_from_slice(line.as_bytes());
                output.push(b'\n');
            }
        }

        Ok(output)
    }

    /// 在 passthrough 模式下解析 SSE 事件，提取 usage / TTFT / delta 字符数
    /// 不产生翻译输出，只更新内部状态
    pub fn track_raw_chunk(&mut self, data: &[u8]) {
        let text = match std::str::from_utf8(data) {
            Ok(t) => t,
            Err(_) => return,
        };

        let lines = self.drain_lines(text);

        for line in &lines {
            let json_str = match line.strip_prefix("data: ") {
                Some(s) if s != "[DONE]" => s,
                _ => continue,
            };

            if let Ok(event) = serde_json::from_str::<Value>(json_str) {
                let event_type = event.get("type").and_then(|v| v.as_str()).unwrap_or("");
                match event_type {
                    "response.output_text.delta" => {
                        self.first_delta_received = true;
                        self.delta_chars += event
                            .get("delta")
                            .and_then(|v| v.as_str())
                            .map(|s| s.len())
                            .unwrap_or(0);
                    }
                    "response.completed" => {
                        self.completed = true;
                        if let Some(resp) = event.get("response") {
                            self.usage = Some(extract_usage(resp));
                            self.service_tier = resp
                                .get("service_tier")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string();
                        }
                    }
                    "response.failed" => {
                        self.completed = true;
                    }
                    _ => {}
                }
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
