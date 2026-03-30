pub mod models;
pub mod queries;

use anyhow::Result;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

pub type DbPool = PgPool;

/// 初始化 PostgreSQL 连接池并建表
pub async fn init(database_url: &str, pool_size: u32) -> Result<DbPool> {
    let pool = PgPoolOptions::new()
        .max_connections(pool_size)
        .min_connections(2)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .idle_timeout(std::time::Duration::from_secs(600))
        .connect(database_url)
        .await?;

    create_tables(&pool).await?;
    Ok(pool)
}

/// 创建表结构（幂等）
/// 注意：sqlx prepared statement 不支持多条 SQL，必须逐条执行
async fn create_tables(pool: &PgPool) -> Result<()> {
    let statements = [
        "CREATE TABLE IF NOT EXISTS accounts (
            id              BIGSERIAL PRIMARY KEY,
            name            TEXT NOT NULL DEFAULT '',
            platform        TEXT NOT NULL DEFAULT 'openai',
            type            TEXT NOT NULL DEFAULT 'oauth',
            credentials     JSONB NOT NULL DEFAULT '{}',
            proxy_url       TEXT NOT NULL DEFAULT '',
            status          TEXT NOT NULL DEFAULT 'active',
            error_message   TEXT NOT NULL DEFAULT '',
            cooldown_reason TEXT NOT NULL DEFAULT '',
            cooldown_until  TIMESTAMPTZ,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
        "CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(status)",
        "CREATE INDEX IF NOT EXISTS idx_accounts_status_id ON accounts(status, id)",
        "CREATE TABLE IF NOT EXISTS usage_logs (
            id                  BIGSERIAL PRIMARY KEY,
            account_id          BIGINT NOT NULL DEFAULT 0,
            endpoint            TEXT NOT NULL DEFAULT '',
            model               TEXT NOT NULL DEFAULT '',
            prompt_tokens       INT NOT NULL DEFAULT 0,
            completion_tokens   INT NOT NULL DEFAULT 0,
            total_tokens        INT NOT NULL DEFAULT 0,
            input_tokens        INT NOT NULL DEFAULT 0,
            output_tokens       INT NOT NULL DEFAULT 0,
            reasoning_tokens    INT NOT NULL DEFAULT 0,
            cached_tokens       INT NOT NULL DEFAULT 0,
            first_token_ms      INT NOT NULL DEFAULT 0,
            reasoning_effort    TEXT NOT NULL DEFAULT '',
            status_code         INT NOT NULL DEFAULT 0,
            duration_ms         INT NOT NULL DEFAULT 0,
            stream              BOOLEAN NOT NULL DEFAULT FALSE,
            service_tier        TEXT NOT NULL DEFAULT '',
            account_email       TEXT NOT NULL DEFAULT '',
            created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
        "CREATE INDEX IF NOT EXISTS idx_usage_logs_created ON usage_logs(created_at)",
        "CREATE INDEX IF NOT EXISTS idx_usage_logs_status ON usage_logs(created_at, status_code)",
        "CREATE TABLE IF NOT EXISTS api_keys (
            id          BIGSERIAL PRIMARY KEY,
            name        TEXT NOT NULL DEFAULT '',
            key         TEXT NOT NULL UNIQUE,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
        "CREATE TABLE IF NOT EXISTS system_settings (
            id                      INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
            max_concurrency         INT NOT NULL DEFAULT 2,
            global_rpm              INT NOT NULL DEFAULT 0,
            test_model              TEXT NOT NULL DEFAULT 'o4-mini',
            test_concurrency        INT NOT NULL DEFAULT 50,
            proxy_url               TEXT NOT NULL DEFAULT '',
            admin_secret            TEXT NOT NULL DEFAULT '',
            auto_clean_unauthorized BOOLEAN NOT NULL DEFAULT FALSE,
            auto_clean_rate_limited BOOLEAN NOT NULL DEFAULT FALSE,
            auto_clean_full_usage   BOOLEAN NOT NULL DEFAULT FALSE,
            auto_clean_error        BOOLEAN NOT NULL DEFAULT FALSE,
            auto_clean_expired      BOOLEAN NOT NULL DEFAULT FALSE,
            fast_scheduler_enabled  BOOLEAN NOT NULL DEFAULT FALSE,
            max_retries             INT NOT NULL DEFAULT 2,
            pg_max_conns            INT NOT NULL DEFAULT 20
        )",
        // 兼容已有表：添加 pg_max_conns 列
        "DO $$ BEGIN
            ALTER TABLE system_settings ADD COLUMN pg_max_conns INT NOT NULL DEFAULT 20;
        EXCEPTION WHEN duplicate_column THEN NULL; END $$",
        "INSERT INTO system_settings (id) VALUES (1) ON CONFLICT DO NOTHING",
        "CREATE TABLE IF NOT EXISTS usage_stats_baseline (
            id                  INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
            total_requests      BIGINT NOT NULL DEFAULT 0,
            total_tokens        BIGINT NOT NULL DEFAULT 0,
            prompt_tokens       BIGINT NOT NULL DEFAULT 0,
            completion_tokens   BIGINT NOT NULL DEFAULT 0,
            cached_tokens       BIGINT NOT NULL DEFAULT 0
        )",
        "INSERT INTO usage_stats_baseline (id) VALUES (1) ON CONFLICT DO NOTHING",
        "CREATE TABLE IF NOT EXISTS account_events (
            id          BIGSERIAL PRIMARY KEY,
            account_id  BIGINT NOT NULL,
            event_type  TEXT NOT NULL DEFAULT '',
            source      TEXT NOT NULL DEFAULT '',
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )",
        "CREATE INDEX IF NOT EXISTS idx_account_events_created ON account_events(created_at)",
    ];

    for sql in statements {
        sqlx::query(sql).execute(pool).await?;
    }

    Ok(())
}
