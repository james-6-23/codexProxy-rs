use super::models::*;
use super::DbPool;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::Row;

/// 批量查询各账号的历史请求统计（启动时恢复内存计数器）
pub async fn get_account_request_counts(pool: &DbPool) -> Result<std::collections::HashMap<i64, (u64, u64)>> {
    let rows = sqlx::query(
        "SELECT account_id,
                COUNT(*)::BIGINT AS total,
                COALESCE(SUM(CASE WHEN status_code >= 400 AND status_code != 499 THEN 1 ELSE 0 END), 0)::BIGINT AS errors
         FROM usage_logs WHERE status_code != 499
         GROUP BY account_id",
    )
    .fetch_all(pool)
    .await?;

    let mut map = std::collections::HashMap::new();
    for row in rows {
        let id: i64 = row.get("account_id");
        let total: i64 = row.get("total");
        let errors: i64 = row.get("errors");
        map.insert(id, (total as u64, errors as u64));
    }
    Ok(map)
}

// ─── 账号操作 ───

pub async fn list_active_accounts(pool: &DbPool) -> Result<Vec<AccountRow>> {
    let rows = sqlx::query_as::<_, AccountRow>(
        "SELECT id, name, platform, type, credentials::TEXT, proxy_url, status,
                error_message, cooldown_reason,
                TO_CHAR(cooldown_until AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS cooldown_until,
                TO_CHAR(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,
                TO_CHAR(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at
         FROM accounts WHERE status = 'active' ORDER BY id",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn insert_account(pool: &DbPool, name: &str, creds: &Credentials, proxy_url: &str) -> Result<i64> {
    let creds_json = serde_json::to_value(creds)?;
    let row = sqlx::query("INSERT INTO accounts (name, credentials, proxy_url) VALUES ($1, $2, $3) RETURNING id")
        .bind(name)
        .bind(creds_json)
        .bind(proxy_url)
        .fetch_one(pool)
        .await?;
    Ok(row.get::<i64, _>("id"))
}

/// 插入 AT-only 账号（type='at'，无 refresh_token）
pub async fn insert_at_account(pool: &DbPool, name: &str, creds: &Credentials, proxy_url: &str) -> Result<i64> {
    let creds_json = serde_json::to_value(creds)?;
    let row = sqlx::query(
        "INSERT INTO accounts (name, type, credentials, proxy_url) VALUES ($1, 'at', $2, $3) RETURNING id",
    )
    .bind(name)
    .bind(creds_json)
    .bind(proxy_url)
    .fetch_one(pool)
    .await?;
    Ok(row.get::<i64, _>("id"))
}

/// 获取所有已有的 access_token（用于 AT 导入去重）
pub async fn get_all_access_tokens(pool: &DbPool) -> Result<std::collections::HashSet<String>> {
    let rows = sqlx::query_scalar::<_, String>(
        "SELECT credentials->>'access_token' FROM accounts WHERE status != 'deleted' AND credentials->>'access_token' != ''",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().collect())
}

/// 获取所有已有的 refresh_token（用于 RT 导入去重）
pub async fn get_all_refresh_tokens(pool: &DbPool) -> Result<std::collections::HashSet<String>> {
    let rows = sqlx::query_scalar::<_, String>(
        "SELECT credentials->>'refresh_token' FROM accounts WHERE status != 'deleted' AND credentials->>'refresh_token' != ''",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().collect())
}

/// 批量软删除账号，返回实际影响行数
pub async fn batch_delete_accounts(pool: &DbPool, ids: &[i64]) -> Result<i64> {
    if ids.is_empty() {
        return Ok(0);
    }
    // 构建 ANY($1) 参数
    let result = sqlx::query(
        "UPDATE accounts SET status = 'deleted', updated_at = NOW() WHERE id = ANY($1) AND status != 'deleted'",
    )
    .bind(ids)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() as i64)
}

pub async fn update_account_credentials(pool: &DbPool, id: i64, creds: &Credentials) -> Result<()> {
    let creds_json = serde_json::to_value(creds)?;
    // 使用 || 合并而非替换，保留已有的 usage/reset 字段
    sqlx::query("UPDATE accounts SET credentials = credentials || $1, updated_at = NOW() WHERE id = $2")
        .bind(creds_json)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_account(pool: &DbPool, id: i64) -> Result<()> {
    sqlx::query("UPDATE accounts SET status = 'deleted', updated_at = NOW() WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

// ─── 使用日志 ───

pub async fn batch_insert_usage_logs(pool: &DbPool, logs: &[UsageLog]) -> Result<()> {
    if logs.is_empty() {
        return Ok(());
    }

    // 构建批量 INSERT（PostgreSQL 支持多行 VALUES）
    let mut query = String::from(
        "INSERT INTO usage_logs (account_id, endpoint, model, prompt_tokens, completion_tokens,
         total_tokens, input_tokens, output_tokens, reasoning_tokens, cached_tokens,
         first_token_ms, reasoning_effort, status_code, duration_ms, stream, service_tier, account_email) VALUES ",
    );

    let mut params_idx = 1u32;
    for (i, _) in logs.iter().enumerate() {
        if i > 0 {
            query.push(',');
        }
        query.push('(');
        for j in 0..17 {
            if j > 0 {
                query.push(',');
            }
            query.push('$');
            query.push_str(&params_idx.to_string());
            params_idx += 1;
        }
        query.push(')');
    }

    let mut q = sqlx::query(&query);
    for log in logs {
        q = q
            .bind(log.account_id)
            .bind(&log.endpoint)
            .bind(&log.model)
            .bind(log.prompt_tokens)
            .bind(log.completion_tokens)
            .bind(log.total_tokens)
            .bind(log.input_tokens)
            .bind(log.output_tokens)
            .bind(log.reasoning_tokens)
            .bind(log.cached_tokens)
            .bind(log.first_token_ms)
            .bind(&log.reasoning_effort)
            .bind(log.status_code)
            .bind(log.duration_ms)
            .bind(log.stream)
            .bind(&log.service_tier)
            .bind(&log.account_email);
    }

    q.execute(pool).await?;
    Ok(())
}

/// 查询图表聚合数据
pub async fn query_chart_data(pool: &DbPool, range_minutes: i64, bucket_minutes: i64) -> Result<ChartData> {
    let interval = format!("{} minutes", range_minutes);
    let bucket_secs = bucket_minutes * 60;

    // 时间线聚合 — date_trunc 不支持任意分钟，用 to_timestamp + floor 方式
    let timeline = sqlx::query_as::<_, ChartBucket>(
        "SELECT
            TO_CHAR(
                TO_TIMESTAMP(FLOOR(EXTRACT(EPOCH FROM created_at) / $1) * $1) AT TIME ZONE 'Asia/Shanghai',
                'YYYY-MM-DD\"T\"HH24:MI:SS'
            ) AS bucket,
            COUNT(*)::BIGINT AS requests,
            COALESCE(AVG(duration_ms), 0)::FLOAT8 AS avg_latency,
            COALESCE(SUM(input_tokens), 0)::BIGINT AS input_tokens,
            COALESCE(SUM(output_tokens), 0)::BIGINT AS output_tokens,
            COALESCE(SUM(reasoning_tokens), 0)::BIGINT AS reasoning_tokens,
            COALESCE(SUM(cached_tokens), 0)::BIGINT AS cached_tokens,
            COALESCE(SUM(CASE WHEN status_code = 401 THEN 1 ELSE 0 END), 0)::BIGINT AS errors_401,
            COALESCE(SUM(CASE WHEN status_code >= 200 AND status_code < 300 THEN 1 ELSE 0 END), 0)::BIGINT AS success_200
         FROM usage_logs
         WHERE created_at >= NOW() - $2::INTERVAL AND status_code != 499
         GROUP BY bucket ORDER BY bucket",
    )
    .bind(bucket_secs as f64)
    .bind(&interval)
    .fetch_all(pool)
    .await?;

    // 模型排名
    let models = sqlx::query_as::<_, ModelRanking>(
        "SELECT model, COUNT(*)::BIGINT AS requests
         FROM usage_logs
         WHERE created_at >= NOW() - $1::INTERVAL AND status_code != 499 AND model != ''
         GROUP BY model ORDER BY requests DESC LIMIT 10",
    )
    .bind(&interval)
    .fetch_all(pool)
    .await?;

    Ok(ChartData { timeline, models })
}

/// 查询使用日志（分页）
/// 带完整筛选的使用日志查询
pub async fn query_usage_logs_filtered(
    pool: &DbPool,
    page: i64,
    page_size: i64,
    model: Option<&str>,
    email: Option<&str>,
    endpoint: Option<&str>,
    stream: Option<&str>,
    start: Option<&str>,
    end: Option<&str>,
) -> Result<(Vec<UsageLogRow>, i64)> {
    let offset = (page - 1) * page_size;

    let mut where_clauses = vec!["status_code != 499".to_string()];
    let mut bind_values: Vec<String> = Vec::new();
    let mut param_idx = 0u32;

    // 时间范围
    if let Some(s) = start {
        param_idx += 1;
        where_clauses.push(format!("created_at >= ${}::TIMESTAMPTZ", param_idx));
        bind_values.push(s.to_string());
    }
    if let Some(e) = end {
        param_idx += 1;
        where_clauses.push(format!("created_at <= ${}::TIMESTAMPTZ", param_idx));
        bind_values.push(e.to_string());
    }

    // 模型
    if let Some(m) = model {
        if !m.is_empty() {
            param_idx += 1;
            where_clauses.push(format!("model = ${}", param_idx));
            bind_values.push(m.to_string());
        }
    }

    // 邮箱（模糊匹配）
    if let Some(em) = email {
        if !em.is_empty() {
            param_idx += 1;
            where_clauses.push(format!("LOWER(account_email) LIKE LOWER(${})", param_idx));
            bind_values.push(format!("%{}%", em));
        }
    }

    // 端点
    if let Some(ep) = endpoint {
        if !ep.is_empty() {
            param_idx += 1;
            where_clauses.push(format!("endpoint = ${}", param_idx));
            bind_values.push(ep.to_string());
        }
    }

    // 流式/非流式
    if let Some(s) = stream {
        match s {
            "true" => where_clauses.push("stream = true".to_string()),
            "false" => where_clauses.push("stream = false".to_string()),
            _ => {}
        }
    }

    let where_sql = where_clauses.join(" AND ");

    // 总数
    let count_sql = format!("SELECT COUNT(*)::BIGINT FROM usage_logs WHERE {}", where_sql);
    let mut count_q = sqlx::query_scalar::<_, i64>(&count_sql);
    for v in &bind_values {
        count_q = count_q.bind(v);
    }
    let total = count_q.fetch_one(pool).await?;

    // 分页数据
    let data_sql = format!(
        "SELECT id, account_id, endpoint, model, prompt_tokens, completion_tokens,
                total_tokens, input_tokens, output_tokens, reasoning_tokens, cached_tokens,
                first_token_ms, reasoning_effort, status_code, duration_ms, stream,
                service_tier, account_email, created_at::TEXT
         FROM usage_logs WHERE {} ORDER BY created_at DESC LIMIT {} OFFSET {}",
        where_sql, page_size, offset
    );
    let mut data_q = sqlx::query_as::<_, UsageLogRow>(&data_sql);
    for v in &bind_values {
        data_q = data_q.bind(v);
    }
    let logs = data_q.fetch_all(pool).await?;

    Ok((logs, total))
}

// ─── 系统设置 ───

pub async fn get_system_settings(pool: &DbPool) -> Result<SystemSettings> {
    let settings = sqlx::query_as::<_, SystemSettings>(
        "SELECT max_concurrency, global_rpm, test_model, test_concurrency,
                proxy_url, admin_secret, auto_clean_unauthorized, auto_clean_rate_limited,
                auto_clean_full_usage, auto_clean_error, auto_clean_expired,
                fast_scheduler_enabled, max_retries, pg_max_conns
         FROM system_settings WHERE id = 1",
    )
    .fetch_one(pool)
    .await?;
    Ok(settings)
}

pub async fn update_system_settings(pool: &DbPool, s: &SystemSettings) -> Result<()> {
    sqlx::query(
        "UPDATE system_settings SET
            max_concurrency=$1, global_rpm=$2, test_model=$3, test_concurrency=$4,
            proxy_url=$5, admin_secret=$6, auto_clean_unauthorized=$7, auto_clean_rate_limited=$8,
            auto_clean_full_usage=$9, auto_clean_error=$10, auto_clean_expired=$11,
            fast_scheduler_enabled=$12, max_retries=$13, pg_max_conns=$14
         WHERE id = 1",
    )
    .bind(s.max_concurrency)
    .bind(s.global_rpm)
    .bind(&s.test_model)
    .bind(s.test_concurrency)
    .bind(&s.proxy_url)
    .bind(&s.admin_secret)
    .bind(s.auto_clean_unauthorized)
    .bind(s.auto_clean_rate_limited)
    .bind(s.auto_clean_full_usage)
    .bind(s.auto_clean_error)
    .bind(s.auto_clean_expired)
    .bind(s.fast_scheduler_enabled)
    .bind(s.max_retries)
    .bind(s.pg_max_conns)
    .execute(pool)
    .await?;
    Ok(())
}

// ─── 统计查询 ───

pub async fn count_today_requests(pool: &DbPool) -> Result<i64> {
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT FROM usage_logs WHERE created_at >= CURRENT_DATE AND status_code != 499",
    )
    .fetch_one(pool)
    .await?;
    Ok(count)
}

pub async fn get_usage_stats_full(pool: &DbPool) -> Result<UsageStatsFull> {
    let row = sqlx::query(
        "SELECT
            COUNT(*)::BIGINT AS total_requests,
            COALESCE(SUM(total_tokens), 0)::BIGINT AS total_tokens,
            COALESCE(SUM(prompt_tokens), 0)::BIGINT AS total_prompt_tokens,
            COALESCE(SUM(completion_tokens), 0)::BIGINT AS total_completion_tokens,
            COALESCE(SUM(cached_tokens), 0)::BIGINT AS total_cached_tokens,
            COALESCE(AVG(duration_ms), 0)::FLOAT8 AS avg_duration_ms
         FROM usage_logs WHERE status_code != 499",
    )
    .fetch_one(pool)
    .await?;

    let today = sqlx::query(
        "SELECT COUNT(*)::BIGINT AS today_requests, COALESCE(SUM(total_tokens), 0)::BIGINT AS today_tokens
         FROM usage_logs WHERE created_at >= CURRENT_DATE AND status_code != 499",
    )
    .fetch_one(pool)
    .await?;

    let minute = sqlx::query(
        "SELECT COUNT(*)::BIGINT AS rpm, COALESCE(SUM(total_tokens), 0)::BIGINT AS tpm
         FROM usage_logs WHERE created_at >= NOW() - INTERVAL '1 minute' AND status_code != 499",
    )
    .fetch_one(pool)
    .await?;

    let today_requests: i64 = today.get("today_requests");
    let error_count: i64 = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT FROM usage_logs
         WHERE created_at >= CURRENT_DATE AND status_code >= 400 AND status_code != 499",
    )
    .fetch_one(pool)
    .await?;

    let error_rate = if today_requests > 0 {
        error_count as f64 / today_requests as f64 * 100.0
    } else {
        0.0
    };

    Ok(UsageStatsFull {
        total_requests: row.get("total_requests"),
        total_tokens: row.get("total_tokens"),
        total_prompt_tokens: row.get("total_prompt_tokens"),
        total_completion_tokens: row.get("total_completion_tokens"),
        total_cached_tokens: row.get("total_cached_tokens"),
        today_requests,
        today_tokens: today.get("today_tokens"),
        rpm: minute.get("rpm"),
        tpm: minute.get("tpm"),
        avg_duration_ms: row.get("avg_duration_ms"),
        error_rate,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageStatsFull {
    pub total_requests: i64,
    pub total_tokens: i64,
    pub total_prompt_tokens: i64,
    pub total_completion_tokens: i64,
    pub total_cached_tokens: i64,
    pub today_requests: i64,
    pub today_tokens: i64,
    pub rpm: i64,
    pub tpm: i64,
    pub avg_duration_ms: f64,
    pub error_rate: f64,
}

// ─── 账号使用详情 ───

pub async fn get_account_usage(pool: &DbPool, account_id: i64) -> Result<AccountUsageDetail> {
    let row = sqlx::query(
        "SELECT COUNT(*)::BIGINT AS total_requests,
                COALESCE(SUM(total_tokens),0)::BIGINT AS total_tokens,
                COALESCE(SUM(input_tokens),0)::BIGINT AS input_tokens,
                COALESCE(SUM(output_tokens),0)::BIGINT AS output_tokens,
                COALESCE(SUM(reasoning_tokens),0)::BIGINT AS reasoning_tokens,
                COALESCE(SUM(cached_tokens),0)::BIGINT AS cached_tokens
         FROM usage_logs WHERE account_id = $1 AND status_code != 499",
    )
    .bind(account_id)
    .fetch_one(pool)
    .await?;

    let models = sqlx::query_as::<_, AccountModelStat>(
        "SELECT model, COUNT(*)::BIGINT AS requests, COALESCE(SUM(total_tokens),0)::BIGINT AS tokens
         FROM usage_logs WHERE account_id = $1 AND status_code != 499 AND model != ''
         GROUP BY model ORDER BY requests DESC LIMIT 10",
    )
    .bind(account_id)
    .fetch_all(pool)
    .await?;

    Ok(AccountUsageDetail {
        total_requests: row.get("total_requests"),
        total_tokens: row.get("total_tokens"),
        input_tokens: row.get("input_tokens"),
        output_tokens: row.get("output_tokens"),
        reasoning_tokens: row.get("reasoning_tokens"),
        cached_tokens: row.get("cached_tokens"),
        models,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountUsageDetail {
    pub total_requests: i64,
    pub total_tokens: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub reasoning_tokens: i64,
    pub cached_tokens: i64,
    pub models: Vec<AccountModelStat>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct AccountModelStat {
    pub model: String,
    pub requests: i64,
    pub tokens: i64,
}

// ─── API Keys ───

pub async fn list_api_keys(pool: &DbPool) -> Result<Vec<ApiKey>> {
    let keys = sqlx::query_as::<_, ApiKey>(
        "SELECT id, name, key, created_at::TEXT FROM api_keys ORDER BY id",
    )
    .fetch_all(pool)
    .await?;
    Ok(keys)
}

pub async fn insert_api_key(pool: &DbPool, name: &str, key: &str) -> Result<i64> {
    let row = sqlx::query("INSERT INTO api_keys (name, key) VALUES ($1, $2) RETURNING id")
        .bind(name)
        .bind(key)
        .fetch_one(pool)
        .await?;
    Ok(row.get::<i64, _>("id"))
}

pub async fn delete_api_key(pool: &DbPool, id: i64) -> Result<()> {
    sqlx::query("DELETE FROM api_keys WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

// ─── 清除日志 ───

pub async fn clear_usage_logs(pool: &DbPool) -> Result<()> {
    // 快照到 baseline 再清空
    sqlx::query(
        "UPDATE usage_stats_baseline SET
            total_requests = total_requests + (SELECT COUNT(*) FROM usage_logs WHERE status_code != 499),
            total_tokens = total_tokens + COALESCE((SELECT SUM(total_tokens) FROM usage_logs WHERE status_code != 499), 0),
            prompt_tokens = prompt_tokens + COALESCE((SELECT SUM(prompt_tokens) FROM usage_logs WHERE status_code != 499), 0),
            completion_tokens = completion_tokens + COALESCE((SELECT SUM(completion_tokens) FROM usage_logs WHERE status_code != 499), 0),
            cached_tokens = cached_tokens + COALESCE((SELECT SUM(cached_tokens) FROM usage_logs WHERE status_code != 499), 0)
         WHERE id = 1",
    )
    .execute(pool)
    .await?;

    sqlx::query("TRUNCATE usage_logs").execute(pool).await?;
    Ok(())
}

// ─── 账号事件 ───

/// 更新账号的用量重置时间（仅更新 credentials JSONB 中的 codex_7d_reset_at 字段）
pub async fn update_account_resets_at(pool: &DbPool, id: i64, resets_at: i64) -> Result<()> {
    let ts_str = if resets_at > 0 {
        resets_at.to_string()
    } else {
        String::new()
    };
    sqlx::query(
        "UPDATE accounts SET credentials = credentials || jsonb_build_object('codex_7d_reset_at', $1::TEXT), updated_at = NOW() WHERE id = $2",
    )
    .bind(&ts_str)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// 持久化账号用量百分比到 credentials JSONB
pub async fn persist_account_usage(pool: &DbPool, id: i64, usage_7d: f64, usage_5h: f64) -> Result<()> {
    sqlx::query(
        "UPDATE accounts SET credentials = credentials || jsonb_build_object('codex_7d_used_percent', $1::FLOAT8, 'codex_5h_used_percent', $2::FLOAT8), updated_at = NOW() WHERE id = $3",
    )
    .bind(usage_7d)
    .bind(usage_5h)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// 清除账号用量状态（探针恢复后调用）
pub async fn clear_account_usage_state(pool: &DbPool, id: i64) -> Result<()> {
    sqlx::query(
        "UPDATE accounts SET credentials = credentials || '{\"codex_7d_used_percent\": 0, \"codex_5h_used_percent\": 0, \"codex_7d_reset_at\": \"\", \"codex_5h_reset_at\": \"\"}'::JSONB, cooldown_until = NULL, cooldown_reason = '', updated_at = NOW() WHERE id = $1",
    )
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// 持久化冷却状态到数据库
pub async fn update_account_cooldown(pool: &DbPool, id: i64, until_ts: i64, reason: &str) -> Result<()> {
    sqlx::query(
        "UPDATE accounts SET cooldown_until = TO_TIMESTAMP($1::FLOAT8), cooldown_reason = $2, updated_at = NOW() WHERE id = $3",
    )
    .bind(until_ts as f64)
    .bind(reason)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// 清除数据库中的冷却状态
pub async fn clear_account_cooldown(pool: &DbPool, id: i64) -> Result<()> {
    sqlx::query(
        "UPDATE accounts SET cooldown_until = NULL, cooldown_reason = '', updated_at = NOW() WHERE id = $1",
    )
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// 插入账号事件（added / deleted）
pub async fn insert_account_event(pool: &DbPool, account_id: i64, event_type: &str, source: &str) {
    let _ = sqlx::query(
        "INSERT INTO account_events (account_id, event_type, source) VALUES ($1, $2, $3)",
    )
    .bind(account_id)
    .bind(event_type)
    .bind(source)
    .execute(pool)
    .await;
}

/// 查询账号增删趋势（按时间桶聚合）
pub async fn get_account_event_trend(
    pool: &DbPool,
    start: &str,
    end: &str,
    bucket_minutes: i64,
) -> Result<Vec<AccountEventPoint>> {
    let bucket_secs = bucket_minutes * 60;
    let rows = sqlx::query_as::<_, AccountEventPoint>(
        "SELECT
            TO_CHAR(
                TO_TIMESTAMP(FLOOR(EXTRACT(EPOCH FROM created_at) / $1) * $1) AT TIME ZONE 'Asia/Shanghai',
                'YYYY-MM-DD\"T\"HH24:MI:SS'
            ) AS bucket,
            COALESCE(SUM(CASE WHEN event_type = 'added' THEN 1 ELSE 0 END), 0)::BIGINT AS added,
            COALESCE(SUM(CASE WHEN event_type = 'deleted' THEN 1 ELSE 0 END), 0)::BIGINT AS deleted
         FROM account_events
         WHERE created_at >= $2::TIMESTAMPTZ AND created_at <= $3::TIMESTAMPTZ
         GROUP BY 1
         ORDER BY 1",
    )
    .bind(bucket_secs as f64)
    .bind(start)
    .bind(end)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

#[derive(Debug, sqlx::FromRow, serde::Serialize)]
pub struct AccountEventPoint {
    pub bucket: String,
    pub added: i64,
    pub deleted: i64,
}
