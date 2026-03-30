use super::models::*;
use super::DbPool;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::Row;

// ─── 账号操作 ───

pub async fn list_active_accounts(pool: &DbPool) -> Result<Vec<AccountRow>> {
    let rows = sqlx::query_as::<_, AccountRow>(
        "SELECT id, name, platform, type, credentials::TEXT, proxy_url, status,
                error_message, cooldown_reason,
                cooldown_until::TEXT, created_at::TEXT, updated_at::TEXT
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

pub async fn update_account_credentials(pool: &DbPool, id: i64, creds: &Credentials) -> Result<()> {
    let creds_json = serde_json::to_value(creds)?;
    sqlx::query("UPDATE accounts SET credentials = $1, updated_at = NOW() WHERE id = $2")
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
                TO_TIMESTAMP(FLOOR(EXTRACT(EPOCH FROM created_at) / $1) * $1),
                'YYYY-MM-DD\"T\"HH24:MI:SS'
            ) AS bucket,
            COUNT(*)::BIGINT AS requests,
            COALESCE(AVG(duration_ms), 0)::FLOAT8 AS avg_latency,
            COALESCE(SUM(input_tokens), 0)::BIGINT AS input_tokens,
            COALESCE(SUM(output_tokens), 0)::BIGINT AS output_tokens,
            COALESCE(SUM(reasoning_tokens), 0)::BIGINT AS reasoning_tokens,
            COALESCE(SUM(cached_tokens), 0)::BIGINT AS cached_tokens,
            COALESCE(SUM(CASE WHEN status_code = 401 THEN 1 ELSE 0 END), 0)::BIGINT AS errors_401
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
pub async fn query_usage_logs(
    pool: &DbPool,
    page: i64,
    page_size: i64,
    model_filter: Option<&str>,
    range_minutes: Option<i64>,
) -> Result<(Vec<UsageLogRow>, i64)> {
    let offset = (page - 1) * page_size;

    // 动态构建查询
    let mut where_clauses = vec!["status_code != 499".to_string()];
    let mut param_idx = 1u32;

    if model_filter.is_some() {
        param_idx += 1;
        where_clauses.push(format!("model = ${}", param_idx));
    }
    if range_minutes.is_some() {
        param_idx += 1;
        where_clauses.push(format!("created_at >= NOW() - (${}::TEXT || ' minutes')::INTERVAL", param_idx));
    }

    let where_sql = where_clauses.join(" AND ");

    // 总数
    let count_sql = format!("SELECT COUNT(*)::BIGINT FROM usage_logs WHERE {}", where_sql);
    let mut count_q = sqlx::query_scalar::<_, i64>(&count_sql);
    if let Some(m) = model_filter {
        count_q = count_q.bind(m);
    }
    if let Some(mins) = range_minutes {
        count_q = count_q.bind(mins.to_string());
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
    if let Some(m) = model_filter {
        data_q = data_q.bind(m);
    }
    if let Some(mins) = range_minutes {
        data_q = data_q.bind(mins.to_string());
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
                fast_scheduler_enabled, max_retries
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
            fast_scheduler_enabled=$12, max_retries=$13
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
