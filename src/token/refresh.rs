use super::*;
use anyhow::{bail, Result};
use reqwest::Client;
use std::time::Duration;
use tracing::warn;

/// 刷新 Access Token
pub async fn refresh_access_token(
    client: &Client,
    refresh_token: &str,
) -> Result<TokenResponse> {
    let rt = refresh_token.trim();
    if rt.is_empty() {
        bail!("non_retryable: refresh_token 为空");
    }

    let resp = client
        .post(TOKEN_URL)
        .header("Accept", "application/json")
        .form(&[
            ("grant_type", "refresh_token"),
            ("client_id", CLIENT_ID),
            ("refresh_token", rt),
            ("scope", REFRESH_SCOPES),
        ])
        .timeout(Duration::from_secs(30))
        .send()
        .await?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        // 检查是否不可重试错误
        if body.contains("invalid_grant")
            || body.contains("invalid_client")
            || body.contains("unauthorized_client")
            || body.contains("access_denied")
        {
            bail!("non_retryable: {}", body);
        }
        bail!("refresh failed ({}): {}", status, body);
    }

    let token_resp: TokenResponse = resp.json().await?;
    Ok(token_resp)
}

/// 带重试的刷新（最多 3 次，指数退避）
pub async fn refresh_with_retry(
    client: &Client,
    refresh_token: &str,
) -> Result<TokenResponse> {
    let rt = refresh_token.trim();
    if rt.is_empty() {
        bail!("non_retryable: refresh_token 为空，跳过刷新");
    }

    let mut last_err = None;

    for attempt in 0..3 {
        match refresh_access_token(client, rt).await {
            Ok(resp) => return Ok(resp),
            Err(e) => {
                let msg = e.to_string();
                if msg.starts_with("non_retryable") {
                    return Err(e);
                }
                warn!(attempt = attempt + 1, error = %msg, "Token 刷新失败，准备重试");
                last_err = Some(e);
                // 指数退避：1s, 2s, 4s
                tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
            }
        }
    }

    Err(last_err.unwrap())
}
