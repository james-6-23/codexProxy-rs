use crate::config::AppConfig;

/// 版本号 + 权重（正式版权重高，alpha 权重低）
///
/// 与 Go 版 `latestCodexCLIVersion = 0.128.0` 对齐——把 0.128.0 作为主版本，
/// 同时保留少量较旧的正式版与 alpha 版以模拟真实用户分布（多版本混用更不易被识别）。
///
/// 权重含义：该版本在 UA 池中出现的条目数
/// 正式版 0.128.0: 权重 5（最新正式版，与 Go 对齐）
/// 正式版 0.127.0: 权重 3
/// 正式版 0.126.0: 权重 2
/// Alpha  0.129.0-alpha.4: 权重 1（尝鲜用户少）
/// Alpha  0.128.0-alpha.12: 权重 1
/// Alpha  0.127.0-alpha.20: 权重 1
struct VersionWeight {
    version: &'static str,
    weight: usize,
}

static VERSIONS: &[VersionWeight] = &[
    VersionWeight { version: "0.128.0", weight: 5 },
    VersionWeight { version: "0.127.0", weight: 3 },
    VersionWeight { version: "0.126.0", weight: 2 },
    VersionWeight { version: "0.129.0-alpha.4", weight: 1 },
    VersionWeight { version: "0.128.0-alpha.12", weight: 1 },
    VersionWeight { version: "0.127.0-alpha.20", weight: 1 },
];

/// 平台 + 终端组合模板（{V} 占位符替换为版本号）
static TEMPLATES: &[&str] = &[
    // macOS arm64（主流）
    "codex_cli_rs/{V} (Mac OS 15.5.0; arm64) Apple_Terminal/464",
    "codex_cli_rs/{V} (Mac OS 15.4.1; arm64) Ghostty/1.2.3",
    "codex_cli_rs/{V} (Mac OS 15.3.0; arm64) iTerm.app/3.5.10",
    "codex_cli_rs/{V} (Mac OS 15.5.0; arm64) kitty/0.40.0",
    "codex_cli_rs/{V} (Mac OS 15.2.0; arm64) WezTerm/20250101",
    "codex_cli_rs/{V} (Mac OS 15.5.0; arm64) vscode/1.100.0",
    "codex_cli_rs/{V} (Mac OS 15.4.0; arm64) tmux/3.5a",
    "codex_cli_rs/{V} (Mac OS 14.7.4; arm64) Alacritty/0.15.1",
    // macOS x86_64
    "codex_cli_rs/{V} (Mac OS 15.4.0; x86_64) Apple_Terminal/464",
    "codex_cli_rs/{V} (Mac OS 14.7.0; x86_64) iTerm.app/3.5.8",
    // Linux
    "codex_cli_rs/{V} (Ubuntu 24.04; x86_64) kitty/0.35.2",
    "codex_cli_rs/{V} (Ubuntu 24.10; x86_64) Alacritty/0.14.0",
    "codex_cli_rs/{V} (Arch Linux Rolling; x86_64) kitty/0.40.0",
    "codex_cli_rs/{V} (Fedora Linux 41; x86_64) vscode/1.100.0",
    // Windows
    "codex_cli_rs/{V} (Windows 10.0.26120; x86_64) WindowsTerminal",
    "codex_cli_rs/{V} (Windows 10.0.22631; x86_64) WindowsTerminal",
];

use std::sync::LazyLock;

/// 按权重展开的 UA 池（正式版出现次数多，alpha 少）
static UA_POOL: LazyLock<Vec<String>> = LazyLock::new(|| {
    let mut pool = Vec::new();
    for vw in VERSIONS {
        for _ in 0..vw.weight {
            for tpl in TEMPLATES {
                pool.push(tpl.replace("{V}", vw.version));
            }
        }
    }
    pool
});

/// 设备指纹配置（运行时可覆盖）
pub struct DeviceProfile {
    pub user_agent: String,
    pub package_version: String,
    pub runtime_version: String,
    pub os: String,
    pub arch: String,
}

impl DeviceProfile {
    /// 从配置和账号 ID 生成设备指纹
    pub fn from_config(config: &AppConfig, account_id: &str) -> Self {
        // 如果配置了固定 UA，直接使用（stabilize_device_profile = true）
        if config.stabilize_device_profile {
            if let Some(ref ua) = config.device_user_agent {
                let version = version_from_ua(ua);
                let (os, arch) = platform_from_ua(ua);
                return Self {
                    user_agent: ua.clone(),
                    package_version: config.device_package_version.clone().unwrap_or_else(|| version.to_string()),
                    runtime_version: config.device_runtime_version.clone().unwrap_or_else(|| version.to_string()),
                    os: config.device_os.clone().unwrap_or_else(|| os.to_string()),
                    arch: config.device_arch.clone().unwrap_or_else(|| arch.to_string()),
                };
            }
        }

        // 否则按账号 ID 确定性选择
        let ua = ua_for_account(account_id);
        let version = version_from_ua(ua);
        let (os, arch) = platform_from_ua(ua);

        Self {
            user_agent: ua.to_string(),
            package_version: config.device_package_version.clone().unwrap_or_else(|| version.to_string()),
            runtime_version: config.device_runtime_version.clone().unwrap_or_else(|| version.to_string()),
            os: config.device_os.clone().unwrap_or_else(|| os.to_string()),
            arch: config.device_arch.clone().unwrap_or_else(|| arch.to_string()),
        }
    }
}

/// 按账号 ID 确定性选择 UA（同一账号始终相同 UA）
pub fn ua_for_account(account_id: &str) -> &str {
    let pool = &*UA_POOL;
    let key = format!("codex2api:ua-profile:{}", account_id);
    let hash = fnv32a(key.as_bytes());
    let idx = (hash as usize) % pool.len();
    &pool[idx]
}

/// 从 UA 中提取版本号（如 "0.128.0"）
pub fn version_from_ua(ua: &str) -> &str {
    // 格式: codex_cli_rs/X.Y.Z[-alpha.N] (...)
    if let Some(start) = ua.find('/') {
        if let Some(end) = ua[start..].find(' ') {
            return &ua[start + 1..start + end];
        }
    }
    super::CLIENT_VERSION
}

/// 从 UA 中提取平台 OS 和 Arch（与 X-Stainless-Os / X-Stainless-Arch 保持一致）
///
/// UA 格式: `codex_cli_rs/0.128.0 (Mac OS 15.5.0; arm64) ...`
/// 返回 `("MacOS", "arm64")` / `("Linux", "x86_64")` / `("Windows", "x86_64")`
pub fn platform_from_ua(ua: &str) -> (&str, &str) {
    // 提取括号内的平台描述
    let inner = ua
        .find('(')
        .and_then(|s| ua[s + 1..].find(')').map(|e| &ua[s + 1..s + 1 + e]))
        .unwrap_or("");

    let os = if inner.contains("Mac OS") {
        "MacOS"
    } else if inner.contains("Windows") {
        "Windows"
    } else {
        "Linux"
    };

    let arch = if inner.contains("arm64") {
        "arm64"
    } else {
        "x86_64"
    };

    (os, arch)
}

/// FNV-32a 哈希（与 Go 版 hash/fnv.New32a 一致）
fn fnv32a(data: &[u8]) -> u32 {
    let mut hash: u32 = 0x811c9dc5;
    for &byte in data {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}

// ─── 官方客户端识别 ───
//
// 与 Go 版 `IsCodexOfficialClientByHeaders` 对齐——识别下游请求的 User-Agent /
// Originator 是否来自官方 / 受信任的 Codex 客户端家族（codex_cli_rs / codex_vscode /
// codex_app / codex_chatgpt_desktop / codex_atlas / codex_exec / codex_sdk_ts / codex /
// opencode）。命中后上游可放行额外能力（如 reasoning_effort=xhigh），并允许
// 透传下游 UA / Originator 而不是覆盖成兜底值。


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_size_and_weight_distribution() {
        let pool = &*UA_POOL;
        // 总量 = (5+3+2+1+1+1) × 16 模板 = 13 × 16 = 208
        assert_eq!(pool.len(), 13 * TEMPLATES.len());

        // 最新正式版 0.128.0 占比最高
        let v_latest_count = pool.iter().filter(|ua| ua.contains("/0.128.0 ")).count();
        let alpha_count = pool.iter().filter(|ua| ua.contains("alpha")).count();
        assert!(v_latest_count > alpha_count, "正式版应多于 alpha");
    }

    #[test]
    fn test_deterministic_ua() {
        let ua1 = ua_for_account("test-123");
        let ua2 = ua_for_account("test-123");
        assert_eq!(ua1, ua2);
        assert!(ua1.starts_with("codex_cli_rs/"));
    }

    #[test]
    fn test_version_extraction() {
        assert_eq!(
            version_from_ua("codex_cli_rs/0.128.0 (Mac OS 15.5.0; arm64) Apple_Terminal/464"),
            "0.128.0"
        );
        assert_eq!(
            version_from_ua("codex_cli_rs/0.129.0-alpha.4 (Ubuntu 24.04; x86_64) kitty/0.35.2"),
            "0.129.0-alpha.4"
        );
    }
}
