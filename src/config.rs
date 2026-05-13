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
    /// 显式允许 /v1/* 在未配置 API Key 时无鉴权放行（默认禁止，fail-closed）
    pub allow_anonymous_v1: bool,

    // 设备指纹配置
    pub device_user_agent: Option<String>,
    pub device_package_version: Option<String>,
    pub device_runtime_version: Option<String>,
    pub device_os: Option<String>,
    pub device_arch: Option<String>,
    pub stabilize_device_profile: bool,
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
            allow_anonymous_v1: env::var("CODEX_ALLOW_ANONYMOUS")
                .ok()
                .map(|s| s.trim().eq_ignore_ascii_case("true"))
                .unwrap_or(false),

            // 设备指纹配置（可选）
            device_user_agent: env::var("CODEX_USER_AGENT").ok().filter(|s| !s.trim().is_empty()),
            device_package_version: env::var("CODEX_PACKAGE_VERSION").ok().filter(|s| !s.trim().is_empty()),
            device_runtime_version: env::var("CODEX_RUNTIME_VERSION").ok().filter(|s| !s.trim().is_empty()),
            device_os: env::var("CODEX_OS").ok().filter(|s| !s.trim().is_empty()),
            device_arch: env::var("CODEX_ARCH").ok().filter(|s| !s.trim().is_empty()),
            stabilize_device_profile: env::var("STABILIZE_DEVICE_PROFILE")
                .ok()
                .map(|s| s.trim().eq_ignore_ascii_case("true"))
                .unwrap_or(false),
        }
    }
}
