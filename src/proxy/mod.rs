pub mod handler;
pub mod ratelimit;
pub mod translator;

/// Codex 上游 API 基地址
pub const UPSTREAM_BASE: &str = "https://api.openai.com";

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
