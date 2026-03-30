use serde_json::Value;

/// 将 OpenAI Chat Completions 格式翻译为 Codex Responses 格式
pub fn translate_chat_to_responses(chat_body: &Value) -> Value {
    let mut body = serde_json::json!({});

    // model
    if let Some(model) = chat_body.get("model") {
        body["model"] = model.clone();
    }

    // messages → input
    if let Some(messages) = chat_body.get("messages").and_then(|v| v.as_array()) {
        let input = convert_messages_to_input(messages);
        body["input"] = input;
    }

    // stream → 始终 true（Codex 要求）
    body["stream"] = Value::Bool(true);
    body["store"] = Value::Bool(false);

    // reasoning_effort
    if let Some(effort) = chat_body.get("reasoning_effort") {
        body["reasoning"] = serde_json::json!({ "effort": effort });
    }

    // temperature
    if let Some(temp) = chat_body.get("temperature") {
        body["temperature"] = temp.clone();
    }

    // max_tokens → max_output_tokens
    if let Some(max) = chat_body.get("max_tokens") {
        body["max_output_tokens"] = max.clone();
    }
    if let Some(max) = chat_body.get("max_completion_tokens") {
        body["max_output_tokens"] = max.clone();
    }

    // tools
    if let Some(tools) = chat_body.get("tools") {
        body["tools"] = tools.clone();
    }

    // service_tier
    if let Some(tier) = chat_body.get("service_tier") {
        body["service_tier"] = tier.clone();
    }

    body
}

/// 将 messages 数组转换为 Codex input 格式
fn convert_messages_to_input(messages: &[Value]) -> Value {
    let mut input = Vec::new();

    for msg in messages {
        let role = msg
            .get("role")
            .and_then(|v| v.as_str())
            .unwrap_or("user");
        let content = msg.get("content");

        // system → developer
        let mapped_role = if role == "system" { "developer" } else { role };

        let mut item = serde_json::json!({
            "role": mapped_role,
        });

        if let Some(c) = content {
            item["content"] = c.clone();
        }

        // 保留 tool_call_id
        if let Some(tc_id) = msg.get("tool_call_id") {
            item["tool_call_id"] = tc_id.clone();
        }

        // 保留 tool_calls
        if let Some(tcs) = msg.get("tool_calls") {
            item["tool_calls"] = tcs.clone();
        }

        input.push(item);
    }

    Value::Array(input)
}

/// 将 Codex Responses 响应翻译为 OpenAI Chat Completions 格式
pub fn translate_response_to_chat(body: &[u8]) -> Result<Vec<u8>, anyhow::Error> {
    let resp: Value = serde_json::from_slice(body)?;

    // 提取 output 中的文本
    let mut content = String::new();
    let mut tool_calls = Vec::new();

    if let Some(output) = resp.get("output").and_then(|v| v.as_array()) {
        for item in output {
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
                    tool_calls.push(serde_json::json!({
                        "id": item.get("call_id").unwrap_or(&Value::Null),
                        "type": "function",
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

    // 构建 Chat Completions 响应
    let mut message = serde_json::json!({
        "role": "assistant",
        "content": if content.is_empty() { Value::Null } else { Value::String(content) },
    });

    if !tool_calls.is_empty() {
        message["tool_calls"] = Value::Array(tool_calls);
    }

    // 提取 usage
    let usage = resp.get("usage").cloned().unwrap_or(serde_json::json!({}));

    let chat_resp = serde_json::json!({
        "id": resp.get("id").unwrap_or(&Value::Null),
        "object": "chat.completion",
        "created": chrono::Utc::now().timestamp(),
        "model": resp.get("model").unwrap_or(&Value::Null),
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": "stop",
        }],
        "usage": usage,
    });

    Ok(serde_json::to_vec(&chat_resp)?)
}

/// 翻译 SSE chunk（Codex Responses → OpenAI Chat Completions 流格式）
pub fn translate_sse_chunk(data: &[u8]) -> Result<Vec<u8>, anyhow::Error> {
    let text = std::str::from_utf8(data)?;
    let mut output = Vec::new();

    for line in text.lines() {
        if let Some(json_str) = line.strip_prefix("data: ") {
            if json_str == "[DONE]" {
                output.extend_from_slice(b"data: [DONE]\n\n");
                continue;
            }

            if let Ok(event) = serde_json::from_str::<Value>(json_str) {
                let event_type = event.get("type").and_then(|v| v.as_str()).unwrap_or("");

                match event_type {
                    "response.output_text.delta" => {
                        let delta_text = event.get("delta").and_then(|v| v.as_str()).unwrap_or("");
                        let chunk = serde_json::json!({
                            "id": event.get("response_id").unwrap_or(&Value::Null),
                            "object": "chat.completion.chunk",
                            "choices": [{
                                "index": 0,
                                "delta": { "content": delta_text },
                                "finish_reason": null,
                            }],
                        });
                        output.extend_from_slice(
                            format!("data: {}\n\n", serde_json::to_string(&chunk)?).as_bytes(),
                        );
                    }
                    "response.completed" => {
                        // 提取 usage
                        let usage = event
                            .get("response")
                            .and_then(|r| r.get("usage"))
                            .cloned()
                            .unwrap_or(serde_json::json!({}));

                        let chunk = serde_json::json!({
                            "id": event.get("response_id")
                                .or_else(|| event.get("response").and_then(|r| r.get("id")))
                                .unwrap_or(&Value::Null),
                            "object": "chat.completion.chunk",
                            "choices": [{
                                "index": 0,
                                "delta": {},
                                "finish_reason": "stop",
                            }],
                            "usage": usage,
                        });
                        output.extend_from_slice(
                            format!("data: {}\n\n", serde_json::to_string(&chunk)?).as_bytes(),
                        );
                        output.extend_from_slice(b"data: [DONE]\n\n");
                    }
                    _ => {
                        // 透传其他事件
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
