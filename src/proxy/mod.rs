pub mod handler;
pub mod ratelimit;
pub mod translator;
pub mod useragent;

/// Codex 上游 API 基地址
pub const UPSTREAM_BASE: &str = "https://chatgpt.com/backend-api/codex";

/// 发往上游的 Originator 标识
pub const ORIGINATOR: &str = "codex_cli_rs";

/// 默认客户端版本
pub const CLIENT_VERSION: &str = "0.117.0";

/// 支持的模型列表
pub const SUPPORTED_MODELS: &[&str] = &[
    "o4-mini",
    "o3",
    "o3-pro",
    "gpt-4.1",
    "gpt-4.1-mini",
    "gpt-4.1-nano",
    "codex-mini",
];
