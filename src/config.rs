use std::env;

/// 应用配置 — 从环境变量加载，不可变
#[derive(Debug, Clone)]
pub struct AppConfig {
    /// 服务端口
    pub port: u16,
    /// PostgreSQL 连接字符串
    pub database_url: String,
    /// 连接池大小
    pub db_pool_size: u32,
    /// 管理后台密钥（可选）
    pub admin_secret: Option<String>,
    /// 全局默认代理 URL（可选）
    pub proxy_url: Option<String>,
}

impl AppConfig {
    /// 从环境变量加载配置
    pub fn from_env() -> Self {
        // 支持两种方式：DATABASE_URL 直接指定，或拆分字段拼接
        let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| {
            let host = env::var("DATABASE_HOST").unwrap_or_else(|_| "localhost".into());
            let port = env::var("DATABASE_PORT").unwrap_or_else(|_| "5432".into());
            let user = env::var("DATABASE_USER").unwrap_or_else(|_| "codex".into());
            let pass = env::var("DATABASE_PASSWORD").unwrap_or_else(|_| "codex".into());
            let name = env::var("DATABASE_NAME").unwrap_or_else(|_| "codex2api".into());
            format!("postgres://{}:{}@{}:{}/{}", user, pass, host, port, name)
        });

        Self {
            port: env::var("CODEX_PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(8080),
            database_url,
            db_pool_size: env::var("DB_POOL_SIZE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(20),
            admin_secret: env::var("ADMIN_SECRET").ok().filter(|s| !s.is_empty()),
            proxy_url: env::var("PROXY_URL").ok().filter(|s| !s.is_empty()),
        }
    }
}
