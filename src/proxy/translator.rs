use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

// ─── Codex 不支持的字段（传了会 400）───

const UNSUPPORTED_FIELDS: &[&str] = &[
    "temperature", "top_p", "frequency_penalty", "presence_penalty",
    "logprobs", "top_logprobs", "n", "seed", "stop", "user",
    "logit_bias", "response_format", "stream_options",
    "truncation", "context_management",
    "max_output_tokens", "max_tokens", "max_completion_tokens",
    "metadata", "verbosity",
];

/// 上游接受的最大工具数量（对齐 Go maxTools）
const MAX_TOOLS: usize = 128;

// ─── Tool schema 中需要移除的 JSON Schema 关键字 ───
//
// 与 Go codex2api 对齐：只移除上游会拒绝的数值/字符串/数组验证约束。
// 保留 allOf/anyOf/oneOf/$ref/$defs/additionalProperties 等结构化关键字，
// 但会递归进入其中清理。
const UNSUPPORTED_SCHEMA_KEYS: &[&str] = &[
    "uniqueItems", "minItems", "maxItems", "minimum", "maximum",
    "exclusiveMinimum", "exclusiveMaximum", "multipleOf",
    "pattern", "minLength", "maxLength", "format",
    "minProperties", "maxProperties",
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
        // 如果 input 是字符串，自动包装为 Codex 单条 user message
        if let Some(s) = input.as_str() {
            body["input"] = json!([{
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": s}],
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
        if let Some(normalized) = normalize_reasoning_effort(effort) {
            body["reasoning"] = json!({ "effort": normalized });
        }
    }

    // tools（需要净化 schema）
    if let Some(tools) = chat_body.get("tools").and_then(|v| v.as_array()) {
        let cleaned = sanitize_tools(tools);
        if !cleaned.is_empty() {
            body["tools"] = Value::Array(cleaned);
        }
    }

    // service_tier（兼容两种字段名；只有 fast/priority 会显式转发上游）
    let tier_raw = chat_body
        .get("service_tier")
        .or_else(|| chat_body.get("serviceTier"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();
    if is_allowed_service_tier(tier_raw) {
        if let Some(mapped) = upstream_service_tier(tier_raw) {
            body["service_tier"] = Value::String(mapped.to_string());
        }
    }

    body
}

/// reasoning_effort 合法值钳制（对齐 Go normalizeReasoningEffort）
/// - 已知值（low/medium/high/xhigh）保留
/// - max → xhigh
/// - 其余非空值（包括 none/min/unknown）→ high（Go 行为：fallback 到 high）
/// - 空字符串 → 返回 None（不注入 reasoning 字段）
fn normalize_reasoning_effort(effort: &str) -> Option<&'static str> {
    let trimmed = effort.trim().to_ascii_lowercase();
    if trimmed.is_empty() {
        return None;
    }
    Some(match trimmed.as_str() {
        "low" => "low",
        "medium" => "medium",
        "high" => "high",
        "xhigh" => "xhigh",
        "max" => "xhigh",
        _ => "high",
    })
}

/// 检查 service_tier 是否在允许集合内（含 "fast" 别名）
fn is_allowed_service_tier(tier: &str) -> bool {
    matches!(
        tier,
        "auto" | "default" | "flex" | "priority" | "scale" | "fast"
    )
}

/// 将客户端 service_tier 映射为上游接受的值。
/// Codex 上游当前只接受 "priority"；auto/default/flex/scale 都不显式转发。
fn upstream_service_tier(tier: &str) -> Option<&'static str> {
    match tier {
        "fast" | "priority" => Some("priority"),
        _ => None,
    }
}

/// 将 messages 数组转换为 Codex input 格式
///
/// 对齐 Go convertMessagesToInputSlice：
/// - system/developer → {type:"message", role:"developer", content:[{type:"input_text", text}]}
/// - user/其他 → {type:"message", role:"user", content:[{type:"input_text", text}]}
/// - assistant 纯文本 → {type:"message", role:"assistant", content:[{type:"output_text", text}]}
/// - assistant + tool_calls → 文本部分为 output_text message，然后每个 tool_call 转 function_call 项
/// - tool → {type:"function_call_output", call_id, output: content_string}
fn convert_messages_to_input(messages: &[Value]) -> Value {
    let mut input: Vec<Value> = Vec::with_capacity(messages.len());

    for msg in messages {
        let role = msg.get("role").and_then(|v| v.as_str()).unwrap_or("user");
        let content = msg.get("content");

        match role {
            "tool" => {
                let call_id = msg.get("tool_call_id").cloned().unwrap_or(Value::Null);
                let output = raw_content_to_string(content);
                input.push(json!({
                    "type": "function_call_output",
                    "call_id": call_id,
                    "output": output,
                }));
            }
            "assistant" => {
                let tool_calls = msg.get("tool_calls").and_then(|v| v.as_array());
                if let Some(tcs) = tool_calls {
                    // 1) 若有 content，先发一条 assistant message
                    let text = raw_content_to_string(content);
                    if !text.is_empty() {
                        input.push(json!({
                            "type": "message",
                            "role": "assistant",
                            "content": [{"type": "output_text", "text": text}],
                        }));
                    }
                    // 2) 每个 tool_call 转 function_call 项
                    for tc in tcs {
                        let func = tc.get("function").unwrap_or(&Value::Null);
                        let args = func
                            .get("arguments")
                            .and_then(|v| v.as_str())
                            .unwrap_or("{}")
                            .to_string();
                        input.push(json!({
                            "type": "function_call",
                            "call_id": tc.get("id").cloned().unwrap_or(Value::Null),
                            "name": func.get("name").cloned().unwrap_or(Value::Null),
                            "arguments": args,
                        }));
                    }
                } else {
                    input.push(json!({
                        "type": "message",
                        "role": "assistant",
                        "content": build_content_parts(content, "assistant"),
                    }));
                }
            }
            "system" | "developer" => {
                input.push(json!({
                    "type": "message",
                    "role": "developer",
                    "content": build_content_parts(content, "system"),
                }));
            }
            _ => {
                input.push(json!({
                    "type": "message",
                    "role": "user",
                    "content": build_content_parts(content, "user"),
                }));
            }
        }
    }

    Value::Array(input)
}

/// 将 content 字段提取为字符串：null → ""，string → 原值，array → 合并所有 text 项
fn raw_content_to_string(content: Option<&Value>) -> String {
    match content {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => s.clone(),
        Some(Value::Array(parts)) => {
            let mut buf = String::new();
            for p in parts {
                if let Some(t) = p.get("text").and_then(|v| v.as_str()) {
                    buf.push_str(t);
                }
            }
            buf
        }
        Some(other) => other.to_string(),
    }
}

/// 构造 Codex content parts：根据 role 选 input_text / output_text
/// 跳过图片等非文本类型（按 scope 要求）。但允许已经规范化的 input_text/output_text 透传。
fn build_content_parts(content: Option<&Value>, role: &str) -> Value {
    let text_type = if role == "assistant" { "output_text" } else { "input_text" };

    match content {
        None | Some(Value::Null) => Value::Array(Vec::new()),
        Some(Value::String(s)) => {
            if s.is_empty() {
                Value::Array(Vec::new())
            } else {
                json!([{"type": text_type, "text": s}])
            }
        }
        Some(Value::Array(parts)) => {
            let mut out: Vec<Value> = Vec::with_capacity(parts.len());
            for item in parts {
                let item_type = item.get("type").and_then(|v| v.as_str()).unwrap_or("");
                match item_type {
                    "text" => {
                        let text = item.get("text").and_then(|v| v.as_str()).unwrap_or("");
                        out.push(json!({"type": text_type, "text": text}));
                    }
                    "input_text" | "output_text" => {
                        // 已经是 Codex 形态，归一到当前 role 对应的类型
                        let text = item.get("text").and_then(|v| v.as_str()).unwrap_or("");
                        out.push(json!({"type": text_type, "text": text}));
                    }
                    // 跳过 image/image_url/file —— 当前 scope 不处理图像
                    _ => {}
                }
            }
            Value::Array(out)
        }
        Some(other) => {
            // 数字、bool 等 → toString
            let s = other.to_string();
            json!([{"type": text_type, "text": s}])
        }
    }
}

/// 净化 tools — 移除不支持的 JSON Schema 关键字，补全缺失的 description，
/// 并将 OpenAI {type:"function", function:{name,description,parameters,strict}} 提升为
/// Codex {type:"function", name, description, parameters, strict}。
fn sanitize_tools(tools: &[Value]) -> Vec<Value> {
    let limit = MAX_TOOLS.min(tools.len());
    tools[..limit]
        .iter()
        .map(|tool| {
            let mut t = tool.clone();
            let tool_type = t.get("type").and_then(|v| v.as_str()).unwrap_or("").trim().to_string();

            // OpenAI 嵌套形式 → 上提到顶层
            if tool_type == "function" && t.get("function").is_some() {
                if let Some(func) = t.get("function").cloned() {
                    if let Some(func_obj) = func.as_object() {
                        let mut item = serde_json::Map::new();
                        item.insert("type".to_string(), Value::String("function".into()));
                        if let Some(name) = func_obj.get("name") {
                            item.insert("name".to_string(), name.clone());
                        }
                        if let Some(desc) = func_obj.get("description") {
                            if !desc.is_null() {
                                item.insert("description".to_string(), desc.clone());
                            }
                        }
                        if let Some(params) = func_obj.get("parameters") {
                            if !params.is_null() {
                                item.insert("parameters".to_string(), params.clone());
                            }
                        }
                        if let Some(strict) = func_obj.get("strict") {
                            item.insert("strict".to_string(), strict.clone());
                        }
                        t = Value::Object(item);
                    }
                }
            }

            // function 工具：补全 description + 规范 parameters
            if tool_type == "function" {
                if let Some(obj) = t.as_object_mut() {
                    if obj
                        .get("description")
                        .map(|v| v.is_null() || v.as_str().map(|s| s.is_empty()).unwrap_or(false))
                        .unwrap_or(true)
                    {
                        let name = obj.get("name").and_then(|v| v.as_str()).unwrap_or("tool");
                        obj.insert(
                            "description".to_string(),
                            Value::String(format!("Execute {}", name)),
                        );
                    }
                    normalize_function_tool_parameters(obj);
                }
            } else {
                // 非 function 工具（如 tool_search）：仅清理 parameters schema
                if let Some(params) = t.get_mut("parameters") {
                    sanitize_schema_for_upstream(params);
                }
                // 默认描述（对齐 Go 的 tool_search 默认值）
                if tool_type == "tool_search" {
                    if let Some(obj) = t.as_object_mut() {
                        if obj
                            .get("description")
                            .map(|v| v.is_null() || v.as_str().map(|s| s.is_empty()).unwrap_or(false))
                            .unwrap_or(true)
                        {
                            obj.insert(
                                "description".to_string(),
                                Value::String(
                                    "Search through available tools to find the most relevant one for the task."
                                        .into(),
                                ),
                            );
                        }
                    }
                }
            }
            t
        })
        .collect()
}

fn normalize_function_tool_parameters(tool: &mut serde_json::Map<String, Value>) {
    let need_default = !matches!(tool.get("parameters"), Some(Value::Object(_)));
    if need_default {
        tool.insert("parameters".to_string(), default_function_parameters_schema());
        return;
    }
    if let Some(params) = tool.get_mut("parameters") {
        sanitize_schema_for_upstream(params);
        ensure_function_parameters_root_object(params);
    }
}

/// 递归移除 JSON Schema 中不支持的验证关键字（对齐 Go stripUnsupportedSchemaKeys）
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
        // 递归进入组合关键字（保留这些关键字本身，但清理子 schema）
        for key in ["allOf", "anyOf", "oneOf"] {
            if let Some(arr) = obj.get_mut(key).and_then(|v| v.as_array_mut()) {
                for item in arr.iter_mut() {
                    strip_unsupported_schema_keys(item);
                }
            }
        }
        if let Some(add_props) = obj.get_mut("additionalProperties") {
            if add_props.is_object() {
                strip_unsupported_schema_keys(add_props);
            }
        }
        if let Some(defs) = obj.get_mut("$defs").and_then(|v| v.as_object_mut()) {
            for (_, v) in defs.iter_mut() {
                strip_unsupported_schema_keys(v);
            }
        }
    }
}

/// 规范化 schema.required 字段：必须是字符串数组，去除空白项，若无项则删除。
fn normalize_schema_required_fields(schema: &mut Value) {
    if let Some(obj) = schema.as_object_mut() {
        let drop_required = match obj.get("required") {
            Some(Value::Array(arr)) => {
                let cleaned: Vec<Value> = arr
                    .iter()
                    .filter_map(|item| match item.as_str() {
                        Some(s) if !s.trim().is_empty() => Some(Value::String(s.to_string())),
                        _ => None,
                    })
                    .collect();
                if cleaned.is_empty() {
                    true
                } else {
                    obj.insert("required".to_string(), Value::Array(cleaned));
                    false
                }
            }
            Some(_) => true,
            None => false,
        };
        if drop_required {
            obj.remove("required");
        }

        if let Some(props) = obj.get_mut("properties").and_then(|v| v.as_object_mut()) {
            for (_, v) in props.iter_mut() {
                normalize_schema_required_fields(v);
            }
        }
        if let Some(items) = obj.get_mut("items") {
            normalize_schema_required_fields(items);
        }
        for key in ["allOf", "anyOf", "oneOf"] {
            if let Some(arr) = obj.get_mut(key).and_then(|v| v.as_array_mut()) {
                for item in arr.iter_mut() {
                    normalize_schema_required_fields(item);
                }
            }
        }
        if let Some(add_props) = obj.get_mut("additionalProperties") {
            if add_props.is_object() {
                normalize_schema_required_fields(add_props);
            }
        }
        if let Some(defs) = obj.get_mut("$defs").and_then(|v| v.as_object_mut()) {
            for (_, v) in defs.iter_mut() {
                normalize_schema_required_fields(v);
            }
        }
    }
}

/// 递归为缺失 items 的 array schema 补上空 schema，
/// 兼容上游对 array 必须声明 items 的校验。
fn ensure_array_items(schema: &mut Value) {
    if let Some(obj) = schema.as_object_mut() {
        if schema_declares_array(obj) && !obj.contains_key("items") {
            obj.insert("items".to_string(), Value::Object(serde_json::Map::new()));
        }
        if let Some(props) = obj.get_mut("properties").and_then(|v| v.as_object_mut()) {
            for (_, v) in props.iter_mut() {
                ensure_array_items(v);
            }
        }
        if let Some(items) = obj.get_mut("items") {
            ensure_array_items(items);
        }
        for key in ["allOf", "anyOf", "oneOf"] {
            if let Some(arr) = obj.get_mut(key).and_then(|v| v.as_array_mut()) {
                for item in arr.iter_mut() {
                    ensure_array_items(item);
                }
            }
        }
        if let Some(add_props) = obj.get_mut("additionalProperties") {
            if add_props.is_object() {
                ensure_array_items(add_props);
            }
        }
        if let Some(defs) = obj.get_mut("$defs").and_then(|v| v.as_object_mut()) {
            for (_, v) in defs.iter_mut() {
                ensure_array_items(v);
            }
        }
    }
}

fn schema_declares_array(obj: &serde_json::Map<String, Value>) -> bool {
    match obj.get("type") {
        Some(Value::String(s)) => s == "array",
        Some(Value::Array(arr)) => arr.iter().any(|v| v.as_str() == Some("array")),
        _ => false,
    }
}

/// 上游接受的完整 schema 净化：移除验证约束 + 规范 required + 补全 items
fn sanitize_schema_for_upstream(schema: &mut Value) {
    strip_unsupported_schema_keys(schema);
    normalize_schema_required_fields(schema);
    ensure_array_items(schema);
}

/// 确保 function tool 的 parameters 顶层是 {type:"object", properties:{}}（缺失则补全）
fn ensure_function_parameters_root_object(schema: &mut Value) {
    if let Some(obj) = schema.as_object_mut() {
        let need_type = !matches!(obj.get("type"), Some(Value::String(s)) if s.trim() == "object");
        if need_type {
            obj.insert("type".to_string(), Value::String("object".into()));
        }
        let need_props = !matches!(obj.get("properties"), Some(Value::Object(_)));
        if need_props {
            obj.insert("properties".to_string(), Value::Object(serde_json::Map::new()));
        }
    }
}

fn default_function_parameters_schema() -> Value {
    json!({
        "type": "object",
        "properties": {},
    })
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
        for item in output.iter() {
            let item_type = item.get("type").and_then(|v| v.as_str()).unwrap_or("");
            match item_type {
                "message" => {
                    if let Some(parts) = item.get("content").and_then(|v| v.as_array()) {
                        for part in parts {
                            let pt = part.get("type").and_then(|v| v.as_str()).unwrap_or("");
                            // 接受 output_text / text / input_text 多种命名
                            if matches!(pt, "output_text" | "text" | "input_text")
                                || pt.is_empty()
                            {
                                if let Some(text) = part.get("text").and_then(|v| v.as_str()) {
                                    content.push_str(text);
                                }
                            }
                        }
                    }
                }
                "function_call" => {
                    let idx = tool_calls.len();
                    tool_calls.push(json!({
                        "id": item.get("call_id").cloned().unwrap_or(Value::Null),
                        "type": "function",
                        "index": idx,
                        "function": {
                            "name": item.get("name").cloned().unwrap_or(Value::Null),
                            "arguments": item.get("arguments").cloned().unwrap_or(Value::Null),
                        }
                    }));
                }
                // 推理项不直接转入 content（OpenAI Chat 没有标准字段承载）
                "reasoning" => {}
                _ => {}
            }
        }
    }

    let usage_info = extract_usage(&resp);
    let has_tool_calls = !tool_calls.is_empty();
    let finish_reason = if has_tool_calls { "tool_calls" } else { "stop" };

    let mut message = json!({
        "role": "assistant",
        "content": if content.is_empty() && has_tool_calls {
            Value::Null
        } else {
            Value::String(content)
        },
    });
    if has_tool_calls {
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
            "finish_reason": finish_reason,
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
    /// 上游显式 response.failed（区别于网络中断或正常 completed）
    /// 移植自 codex2api 提交 285f209 fix(proxy): classify response failed streams
    pub failed: bool,
    /// response.failed 事件的原始 JSON 载荷，用于提取 status_code / message
    pub failure_payload: Option<String>,
    /// 上游流是否异常中断（网络错误、未收到 completed 就结束）
    pub stream_broken: bool,
    /// 累积 delta 字符数（用于 token 估算）
    pub delta_chars: usize,
    /// 从 response.completed 提取的 usage
    pub usage: Option<UsageInfo>,
    /// service_tier
    pub service_tier: String,
    /// 是否曾出现 tool_call（影响 finish_reason）
    has_tool_calls: bool,
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
            failed: false,
            failure_payload: None,
            stream_broken: false,
            delta_chars: 0,
            usage: None,
            service_tier: String::new(),
            has_tool_calls: false,
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
                    self.failed = true;
                    if self.failure_payload.is_none() {
                        self.failure_payload = Some(json_str.to_string());
                    }
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
                                self.has_tool_calls = true;

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
                                        self.has_tool_calls = true;

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

                                let finish = if self.has_tool_calls { "tool_calls" } else { "stop" };

                                let chunk = ChatChunk {
                                    id: final_rid,
                                    object: "chat.completion.chunk",
                                    choices: [ChunkChoice {
                                        index: 0,
                                        delta: ChunkDelta {
                                            content: None,
                                            tool_calls: None,
                                        },
                                        finish_reason: Some(finish),
                                    }],
                                    usage: Some(usage_json),
                                };
                                output.extend_from_slice(b"data: ");
                                serde_json::to_writer(&mut output, &chunk)?;
                                output.extend_from_slice(b"\n\n");
                                output.extend_from_slice(b"data: [DONE]\n\n");
                            }

                            // 已知但不需要透传给 OpenAI 客户端的噪音事件（对齐 Go 行为，直接丢弃）
                            "response.created"
                            | "response.in_progress"
                            | "response.content_part.added"
                            | "response.content_part.done"
                            | "response.output_item.done"
                            | "response.output_text.done"
                            | "response.function_call_arguments.done"
                            | "response.reasoning_summary_part.added"
                            | "response.reasoning_summary_part.done"
                            | "response.reasoning_summary_text.delta"
                            | "response.reasoning_summary_text.done"
                            | "response.reasoning_text.delta"
                            | "response.reasoning_text.done"
                            | "response.reasoning.encrypted_content.delta"
                            | "response.reasoning.encrypted_content.done" => {
                                // 静默丢弃 — rs 不向 OpenAI 客户端透传 reasoning，避免协议噪音
                            }

                            // 其他事件透传
                            _ => {
                                // response.failed 单独处理：透传给客户端，但同时标记 failed
                                // 移植自 codex2api 提交 285f209：用于将失败分类为非 200 状态
                                if event.event_type == "response.failed" {
                                    self.failed = true;
                                    if self.failure_payload.is_none() {
                                        self.failure_payload = Some(json_str.to_string());
                                    }
                                }
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

    /// 将 response.failed 载荷分类为 (status_code, kind, message)
    ///
    /// 移植自 codex2api 提交 285f209：上游 SSE 返回 response.failed 时，
    /// 应根据其携带的 error.status_code / error.code / error.type 推断真实状态码，
    /// 避免被记为成功（200）。
    pub fn classify_failure(&self) -> Option<(i64, &'static str, String)> {
        let payload = self.failure_payload.as_deref()?;
        let value: serde_json::Value = serde_json::from_str(payload).ok()?;

        // 1. 直接读 status_code
        let paths = [
            "/response/status_code",
            "/response/error/status_code",
            "/response/status_details/error/status_code",
            "/status_code",
            "/error/status_code",
        ];
        let mut status_code: i64 = 0;
        for p in paths {
            if let Some(code) = value.pointer(p).and_then(|v| v.as_i64())
                && (400..=599).contains(&code)
            {
                status_code = code;
                break;
            }
        }

        // 2. 没有直接 status_code → 根据 error.code / error.type 推断
        if status_code == 0 {
            let mut tokens = String::new();
            for p in [
                "/response/error/code",
                "/response/error/type",
                "/response/status_details/error/code",
                "/response/status_details/error/type",
                "/error/code",
                "/error/type",
            ] {
                if let Some(s) = value.pointer(p).and_then(|v| v.as_str()) {
                    tokens.push(' ');
                    tokens.push_str(s);
                }
            }
            let lc = tokens.to_lowercase();
            status_code = if lc.contains("rate_limit") {
                429
            } else if lc.contains("unauthorized") || lc.contains("invalid_api_key") {
                401
            } else if lc.contains("payment") {
                402
            } else if lc.contains("forbidden") {
                403
            } else if lc.contains("invalid") || lc.contains("bad_request") {
                400
            } else {
                500
            };
        }

        // 提取 message
        let message = ["/response/error/message", "/response/status_details/error/message", "/error/message"]
            .iter()
            .find_map(|p| value.pointer(p).and_then(|v| v.as_str()))
            .map(|s| s.to_string())
            .unwrap_or_else(|| "上游返回 response.failed".to_string());

        let kind: &'static str = if status_code >= 500 { "server" } else { "client" };

        Some((status_code, kind, message))
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

// ─── 单元测试 ───

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── translate_chat_to_responses ─────────────────────────────

    #[test]
    fn chat_to_responses_basic_user_message() {
        let chat = json!({
            "model": "gpt-5.4",
            "messages": [
                {"role": "user", "content": "Hello"}
            ],
        });
        let resp = translate_chat_to_responses(&chat);
        assert_eq!(resp["model"], "gpt-5.4");
        assert_eq!(resp["stream"], true);
        assert_eq!(resp["store"], false);
        assert_eq!(resp["include"], json!(["reasoning.encrypted_content"]));
        let input = &resp["input"];
        assert!(input.is_array());
        let first = &input[0];
        assert_eq!(first["type"], "message");
        assert_eq!(first["role"], "user");
        assert_eq!(first["content"][0]["type"], "input_text");
        assert_eq!(first["content"][0]["text"], "Hello");
    }

    #[test]
    fn chat_to_responses_system_becomes_developer() {
        let chat = json!({
            "model": "gpt-5.4",
            "messages": [
                {"role": "system", "content": "Be brief."},
                {"role": "user", "content": "Hi"}
            ],
        });
        let resp = translate_chat_to_responses(&chat);
        let first = &resp["input"][0];
        assert_eq!(first["type"], "message");
        assert_eq!(first["role"], "developer");
        assert_eq!(first["content"][0]["type"], "input_text");
        assert_eq!(first["content"][0]["text"], "Be brief.");
    }

    #[test]
    fn chat_to_responses_assistant_with_tool_calls() {
        let chat = json!({
            "model": "gpt-5.4",
            "messages": [
                {"role": "user", "content": "Get weather"},
                {
                    "role": "assistant",
                    "content": "Sure, let me check.",
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": {"name": "get_weather", "arguments": "{\"city\":\"NYC\"}"}
                    }]
                },
                {"role": "tool", "tool_call_id": "call_1", "content": "75F"}
            ],
        });
        let resp = translate_chat_to_responses(&chat);
        let input = resp["input"].as_array().unwrap();
        // user
        assert_eq!(input[0]["role"], "user");
        // assistant text message
        assert_eq!(input[1]["type"], "message");
        assert_eq!(input[1]["role"], "assistant");
        assert_eq!(input[1]["content"][0]["type"], "output_text");
        assert_eq!(input[1]["content"][0]["text"], "Sure, let me check.");
        // function_call
        assert_eq!(input[2]["type"], "function_call");
        assert_eq!(input[2]["call_id"], "call_1");
        assert_eq!(input[2]["name"], "get_weather");
        assert_eq!(input[2]["arguments"], "{\"city\":\"NYC\"}");
        // tool result
        assert_eq!(input[3]["type"], "function_call_output");
        assert_eq!(input[3]["call_id"], "call_1");
        assert_eq!(input[3]["output"], "75F");
    }

    #[test]
    fn chat_to_responses_array_content_normalized() {
        let chat = json!({
            "model": "gpt-5.4",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": "Part A"},
                    {"type": "input_text", "text": "Part B"}
                ]
            }],
        });
        let resp = translate_chat_to_responses(&chat);
        let parts = resp["input"][0]["content"].as_array().unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0]["type"], "input_text");
        assert_eq!(parts[0]["text"], "Part A");
        assert_eq!(parts[1]["type"], "input_text");
        assert_eq!(parts[1]["text"], "Part B");
    }

    #[test]
    fn chat_to_responses_image_parts_skipped() {
        let chat = json!({
            "model": "gpt-5.4",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": "Describe"},
                    {"type": "image_url", "image_url": {"url": "data:..."}}
                ]
            }],
        });
        let resp = translate_chat_to_responses(&chat);
        let parts = resp["input"][0]["content"].as_array().unwrap();
        // Image silently dropped (scope: skip images)
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0]["text"], "Describe");
    }

    #[test]
    fn chat_to_responses_service_tier_mapping() {
        // priority → priority
        let chat = json!({"model": "x", "messages": [], "service_tier": "priority"});
        assert_eq!(translate_chat_to_responses(&chat)["service_tier"], "priority");
        // fast alias → priority
        let chat = json!({"model": "x", "messages": [], "service_tier": "fast"});
        assert_eq!(translate_chat_to_responses(&chat)["service_tier"], "priority");
        // auto → stripped (not forwarded to upstream)
        let chat = json!({"model": "x", "messages": [], "service_tier": "auto"});
        let resp = translate_chat_to_responses(&chat);
        assert!(resp.get("service_tier").is_none());
        // unknown → stripped
        let chat = json!({"model": "x", "messages": [], "service_tier": "weird"});
        let resp = translate_chat_to_responses(&chat);
        assert!(resp.get("service_tier").is_none());
    }

    #[test]
    fn chat_to_responses_reasoning_effort_normalization() {
        let chat = json!({"model": "x", "messages": [], "reasoning_effort": "high"});
        assert_eq!(
            translate_chat_to_responses(&chat)["reasoning"],
            json!({"effort": "high"})
        );
        let chat = json!({"model": "x", "messages": [], "reasoning_effort": "max"});
        assert_eq!(
            translate_chat_to_responses(&chat)["reasoning"],
            json!({"effort": "xhigh"})
        );
        let chat = json!({"model": "x", "messages": [], "reasoning_effort": "bogus"});
        assert_eq!(
            translate_chat_to_responses(&chat)["reasoning"],
            json!({"effort": "high"})
        );
    }

    #[test]
    fn chat_to_responses_string_input_wrapping() {
        let chat = json!({"model": "x", "input": "Plain string"});
        let resp = translate_chat_to_responses(&chat);
        let input = resp["input"].as_array().unwrap();
        assert_eq!(input.len(), 1);
        assert_eq!(input[0]["type"], "message");
        assert_eq!(input[0]["role"], "user");
        assert_eq!(input[0]["content"][0]["type"], "input_text");
        assert_eq!(input[0]["content"][0]["text"], "Plain string");
    }

    // ── tool sanitization ───────────────────────────────────────

    #[test]
    fn tools_openai_nested_flattened_to_codex() {
        let chat = json!({
            "model": "x",
            "messages": [],
            "tools": [{
                "type": "function",
                "function": {
                    "name": "get_time",
                    "description": "Return current time",
                    "parameters": {"type": "object", "properties": {"tz": {"type": "string"}}}
                }
            }]
        });
        let resp = translate_chat_to_responses(&chat);
        let tool = &resp["tools"][0];
        assert_eq!(tool["type"], "function");
        assert_eq!(tool["name"], "get_time");
        assert_eq!(tool["description"], "Return current time");
        assert_eq!(tool["parameters"]["type"], "object");
        // Nested function key should NOT remain
        assert!(tool.get("function").is_none());
    }

    #[test]
    fn tools_missing_description_filled() {
        let chat = json!({
            "model": "x",
            "messages": [],
            "tools": [{
                "type": "function",
                "function": {"name": "ping"}
            }]
        });
        let resp = translate_chat_to_responses(&chat);
        let tool = &resp["tools"][0];
        assert_eq!(tool["description"], "Execute ping");
        // Default parameters object should exist
        assert_eq!(tool["parameters"]["type"], "object");
        assert!(tool["parameters"]["properties"].is_object());
    }

    #[test]
    fn tools_schema_strips_validation_keys_only() {
        let chat = json!({
            "model": "x",
            "messages": [],
            "tools": [{
                "type": "function",
                "function": {
                    "name": "search",
                    "description": "ok",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "q": {"type": "string", "minLength": 1, "pattern": "^.+$"},
                            "items": {"type": "array"} // missing items
                        },
                        "required": ["q", "", "  "],
                        "anyOf": [{"required": ["q"]}]
                    }
                }
            }]
        });
        let resp = translate_chat_to_responses(&chat);
        let params = &resp["tools"][0]["parameters"];
        let q = &params["properties"]["q"];
        // Validation keys stripped
        assert!(q.get("minLength").is_none());
        assert!(q.get("pattern").is_none());
        // String type preserved
        assert_eq!(q["type"], "string");
        // array gets items: {}
        assert_eq!(params["properties"]["items"]["items"], json!({}));
        // required cleaned
        assert_eq!(params["required"], json!(["q"]));
        // anyOf preserved (not stripped)
        assert!(params["anyOf"].is_array());
    }

    // ── translate_response_to_chat ──────────────────────────────

    #[test]
    fn response_to_chat_extracts_output_text() {
        let body = json!({
            "id": "resp_1",
            "model": "gpt-5.4",
            "output": [{
                "type": "message",
                "content": [
                    {"type": "output_text", "text": "Hello "},
                    {"type": "output_text", "text": "world"}
                ]
            }],
            "usage": {"input_tokens": 10, "output_tokens": 5}
        });
        let (bytes, usage) = translate_response_to_chat(&serde_json::to_vec(&body).unwrap()).unwrap();
        let resp: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(resp["choices"][0]["message"]["content"], "Hello world");
        assert_eq!(resp["choices"][0]["finish_reason"], "stop");
        assert_eq!(usage.input_tokens, 10);
        assert_eq!(usage.output_tokens, 5);
        assert_eq!(usage.total_tokens, 15);
    }

    #[test]
    fn response_to_chat_with_tool_calls_finish_reason() {
        let body = json!({
            "id": "resp_2",
            "model": "gpt-5.4",
            "output": [
                {
                    "type": "function_call",
                    "call_id": "call_X",
                    "name": "get_weather",
                    "arguments": "{\"city\":\"NYC\"}"
                }
            ],
            "usage": {"input_tokens": 5, "output_tokens": 2}
        });
        let (bytes, _) = translate_response_to_chat(&serde_json::to_vec(&body).unwrap()).unwrap();
        let resp: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(resp["choices"][0]["finish_reason"], "tool_calls");
        let tc = &resp["choices"][0]["message"]["tool_calls"][0];
        assert_eq!(tc["id"], "call_X");
        assert_eq!(tc["function"]["name"], "get_weather");
        // content should be null when only tool_calls
        assert!(resp["choices"][0]["message"]["content"].is_null());
    }

    // ── strip_unsupported_fields ────────────────────────────────

    #[test]
    fn strip_unsupported_fields_removes_known_keys() {
        let mut body = json!({
            "model": "x",
            "temperature": 0.7,
            "top_p": 0.9,
            "metadata": {"foo": "bar"},
            "verbosity": "high",
            "previous_response_id": "resp_x",
            "safety_identifier": "abc"
        });
        strip_unsupported_fields(&mut body);
        assert!(body.get("temperature").is_none());
        assert!(body.get("top_p").is_none());
        assert!(body.get("metadata").is_none());
        assert!(body.get("verbosity").is_none());
        assert!(body.get("previous_response_id").is_none());
        assert!(body.get("safety_identifier").is_none());
        assert_eq!(body["model"], "x");
    }

    // ── StreamTranslator ────────────────────────────────────────

    #[test]
    fn stream_translates_output_text_delta() {
        let mut t = StreamTranslator::new();
        let chunk = "data: {\"type\":\"response.output_text.delta\",\"response_id\":\"r1\",\"delta\":\"Hi\"}\n\n";
        let out = t.translate_chunk(chunk.as_bytes()).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("\"content\":\"Hi\""));
        assert!(t.first_delta_received);
        assert_eq!(t.delta_chars, 2);
    }

    #[test]
    fn stream_completed_emits_finish_and_done() {
        let mut t = StreamTranslator::new();
        let chunk = r#"data: {"type":"response.completed","response":{"id":"r1","usage":{"input_tokens":3,"output_tokens":4,"output_tokens_details":{"reasoning_tokens":1},"input_tokens_details":{"cached_tokens":2}},"service_tier":"priority"}}
"#;
        let out = t.translate_chunk(chunk.as_bytes()).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(t.completed);
        assert!(s.contains("\"finish_reason\":\"stop\""));
        assert!(s.contains("data: [DONE]"));
        let u = t.usage.as_ref().unwrap();
        assert_eq!(u.input_tokens, 3);
        assert_eq!(u.output_tokens, 4);
        assert_eq!(u.reasoning_tokens, 1);
        assert_eq!(u.cached_tokens, 2);
        assert_eq!(t.service_tier, "priority");
    }

    #[test]
    fn stream_tool_calls_set_finish_reason_tool_calls() {
        let mut t = StreamTranslator::new();
        let added = "data: {\"type\":\"response.output_item.added\",\"response_id\":\"r1\",\"item\":{\"id\":\"i1\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"f\"}}\n\n";
        t.translate_chunk(added.as_bytes()).unwrap();
        let args = "data: {\"type\":\"response.function_call_arguments.delta\",\"response_id\":\"r1\",\"item_id\":\"i1\",\"delta\":\"{}\"}\n\n";
        t.translate_chunk(args.as_bytes()).unwrap();
        let done = "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n";
        let out = t.translate_chunk(done.as_bytes()).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("\"finish_reason\":\"tool_calls\""));
    }

    #[test]
    fn stream_noisy_events_dropped() {
        let mut t = StreamTranslator::new();
        let chunk = "data: {\"type\":\"response.created\",\"response_id\":\"r1\"}\n\ndata: {\"type\":\"response.in_progress\",\"response_id\":\"r1\"}\n\n";
        let out = t.translate_chunk(chunk.as_bytes()).unwrap();
        let s = String::from_utf8(out).unwrap();
        // Noise should not be forwarded
        assert!(!s.contains("response.created"));
        assert!(!s.contains("response.in_progress"));
    }

    #[test]
    fn stream_reasoning_deltas_dropped() {
        // 移植自 codex2api 一致性：reasoning_*.delta 不应透传给 OpenAI 客户端
        let mut t = StreamTranslator::new();
        let chunk = concat!(
            "data: {\"type\":\"response.reasoning_summary_text.delta\",\"response_id\":\"r1\",\"delta\":\"思考中…\"}\n\n",
            "data: {\"type\":\"response.reasoning_text.delta\",\"response_id\":\"r1\",\"delta\":\"hidden\"}\n\n",
        );
        let out = t.translate_chunk(chunk.as_bytes()).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(!s.contains("reasoning_summary_text"), "summary delta leaked: {}", s);
        assert!(!s.contains("reasoning_text"), "reasoning text delta leaked: {}", s);
        assert!(!s.contains("思考中"), "reasoning content leaked: {}", s);
    }

    #[test]
    fn stream_chunk_split_across_boundary() {
        let mut t = StreamTranslator::new();
        let full = "data: {\"type\":\"response.output_text.delta\",\"response_id\":\"r1\",\"delta\":\"Hello\"}\n\n";
        let (a, b) = full.split_at(20);
        let _ = t.translate_chunk(a.as_bytes()).unwrap();
        let out = t.translate_chunk(b.as_bytes()).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("\"content\":\"Hello\""));
    }

    #[test]
    fn parse_sse_error_extracts_message() {
        let json_str = r#"{"type":"response.failed","response":{"status_details":{"error":{"message":"upstream broke"}}}}"#;
        assert_eq!(parse_sse_error(json_str), Some("upstream broke".into()));
        let json_str = r#"{"type":"response.completed"}"#;
        assert_eq!(parse_sse_error(json_str), None);
    }

    // ── schema sanitization unit tests ──────────────────────────

    #[test]
    fn schema_recurses_into_anyof_and_defs() {
        let mut schema = json!({
            "type": "object",
            "properties": {
                "a": {"type": "string", "pattern": "^x$"}
            },
            "anyOf": [
                {"type": "string", "minLength": 3},
                {"type": "object", "properties": {"b": {"type": "integer", "minimum": 0}}}
            ],
            "$defs": {
                "Foo": {"type": "string", "maxLength": 10}
            }
        });
        sanitize_schema_for_upstream(&mut schema);
        assert!(schema["properties"]["a"].get("pattern").is_none());
        assert!(schema["anyOf"][0].get("minLength").is_none());
        assert!(schema["anyOf"][1]["properties"]["b"].get("minimum").is_none());
        assert!(schema["$defs"]["Foo"].get("maxLength").is_none());
        // anyOf itself preserved
        assert!(schema["anyOf"].is_array());
    }

    #[test]
    fn schema_required_normalized() {
        let mut schema = json!({
            "type": "object",
            "required": ["a", "", "b", "  "],
            "properties": {"a": {"type": "string"}, "b": {"type": "string"}}
        });
        sanitize_schema_for_upstream(&mut schema);
        assert_eq!(schema["required"], json!(["a", "b"]));

        let mut schema = json!({"type": "object", "required": []});
        sanitize_schema_for_upstream(&mut schema);
        assert!(schema.get("required").is_none());
    }

    #[test]
    fn schema_ensure_items_on_array() {
        let mut schema = json!({
            "type": "object",
            "properties": {
                "list": {"type": "array"}
            }
        });
        sanitize_schema_for_upstream(&mut schema);
        assert_eq!(schema["properties"]["list"]["items"], json!({}));
    }

    #[test]
    fn schema_function_parameters_root_object_enforced() {
        // Empty parameters → default object schema
        let mut chat = json!({
            "model": "x",
            "messages": [],
            "tools": [{"type": "function", "function": {"name": "f"}}]
        });
        let resp = translate_chat_to_responses(&chat);
        let params = &resp["tools"][0]["parameters"];
        assert_eq!(params["type"], "object");
        assert!(params["properties"].is_object());

        // type missing → forced to object
        chat = json!({
            "model": "x",
            "messages": [],
            "tools": [{"type": "function", "function": {
                "name": "f",
                "parameters": {"properties": {"a": {"type": "string"}}}
            }}]
        });
        let resp = translate_chat_to_responses(&chat);
        assert_eq!(resp["tools"][0]["parameters"]["type"], "object");
    }
}

