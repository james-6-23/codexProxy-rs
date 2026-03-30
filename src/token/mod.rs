pub mod cache;
pub mod refresh;

use serde::{Deserialize, Serialize};

/// OAuth Token 刷新端点
pub const TOKEN_URL: &str = "https://auth.openai.com/oauth/token";
pub const CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
pub const REFRESH_SCOPES: &str = "openid profile email";

/// Token 刷新响应
#[derive(Debug, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    #[serde(default)]
    pub refresh_token: String,
    #[serde(default)]
    pub id_token: String,
    #[serde(default)]
    pub expires_in: i64,
}

/// 从 JWT payload 中解析的账号信息
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AccountInfo {
    pub email: String,
    pub chatgpt_account_id: String,
    pub chatgpt_plan_type: String,
}

/// 解析 JWT payload（不验证签名）
pub fn parse_id_token(id_token: &str) -> Option<AccountInfo> {
    let parts: Vec<&str> = id_token.split('.').collect();
    if parts.len() < 2 {
        return None;
    }

    // JWT payload 是 base64url 编码
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;
    let payload = URL_SAFE_NO_PAD.decode(parts[1]).ok()?;
    let json: serde_json::Value = serde_json::from_slice(&payload).ok()?;

    Some(AccountInfo {
        email: json
            .get("email")
            .or_else(|| json.pointer("/https://api.openai.com/profile/email"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        chatgpt_account_id: json
            .get("chatgpt_account_id")
            .or_else(|| json.pointer("/https://api.openai.com/auth/chatgpt_account_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        chatgpt_plan_type: json
            .get("chatgpt_plan_type")
            .or_else(|| json.pointer("/https://api.openai.com/auth/chatgpt_plan_type"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
    })
}
