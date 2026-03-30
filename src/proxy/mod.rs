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
    "gpt-5.4",
    "gpt-5.4-mini",
    "gpt-5",
    "gpt-5-codex",
    "gpt-5-codex-mini",
    "gpt-5.1",
    "gpt-5.1-codex",
    "gpt-5.1-codex-mini",
    "gpt-5.1-codex-max",
    "gpt-5.2",
    "gpt-5.2-codex",
    "gpt-5.3-codex",
];
