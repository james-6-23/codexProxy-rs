//! 全局 RPM 令牌桶限流器
//!
//! 简化版对齐 Go 版 `proxy/ratelimit.go` 的核心算法：
//! - Go 用 float 令牌 + RWMutex；Rust 改用 `tokens × 1000` 定点 + 原子 + CAS，避免锁竞争。
//! - 两者数学上等价：每毫秒补充 `rpm/60000` 个令牌（×1000 后整型存储），上限 `rpm`。
//! - Go 在桶耗尽时会进入指数退避冷却（1s..30min），Rust 未在此层实现——
//!   原因：账号级冷却由 `scheduler::mod` 的 cooldown 状态机统一管理（来自上游 429/401
//!   响应头 `Resets-At`），全局 RPM 在 Rust 仅作"软上限"返回 false 让上层重试，
//!   不需要双重退避。Go 的 multi-level (account/model) 限流目前 Rust 也不需要——
//!   账号级限速由 scheduler 的 dynamic_concurrency 与 health tier 控制。
//!
//! 这是有意的范围裁剪，详见 `~/.claude/projects/.../codex2api-rs-scope-2026.md`。

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Instant;

/// 令牌桶限流器
pub struct RateLimiter {
    /// 每分钟最大请求数（0 = 不限制）
    rpm: AtomicI64,
    /// 当前桶中可用令牌数 ×1000
    tokens_1000: AtomicI64,
    /// 上次补充时间
    last_refill: parking_lot::Mutex<Instant>,
}

impl RateLimiter {
    pub fn new(rpm: i64) -> Self {
        Self {
            rpm: AtomicI64::new(rpm),
            tokens_1000: AtomicI64::new(rpm * 1000),
            last_refill: parking_lot::Mutex::new(Instant::now()),
        }
    }

    /// 尝试获取一个令牌
    pub fn allow(&self) -> bool {
        let rpm = self.rpm.load(Ordering::Relaxed);
        if rpm <= 0 {
            return true; // 不限制
        }

        self.refill();

        loop {
            let current = self.tokens_1000.load(Ordering::Acquire);
            if current < 1000 {
                return false;
            }
            if self
                .tokens_1000
                .compare_exchange_weak(current, current - 1000, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// 补充令牌
    ///
    /// 关键不变量：
    /// 1. 用 CAS 累加而非 load-then-store，避免与 `allow()` 的 CAS 解递争用导致丢失计数。
    /// 2. 当 refill==0（rpm 太低或间隔太短）时**不**推进 `last_refill`，让 elapsed_ms
    ///    继续累加直到能凑出至少 1 个 ×1000 单位的补充——避免低 rpm 下时间被截断丢失。
    fn refill(&self) {
        let rpm = self.rpm.load(Ordering::Relaxed);
        if rpm <= 0 {
            return;
        }

        let mut last = self.last_refill.lock();
        let now = Instant::now();
        let elapsed_ms = now.duration_since(*last).as_millis() as i64;

        if elapsed_ms < 10 {
            return;
        }

        // 每毫秒补充 rpm/60000 个令牌（×1000 定点）
        let refill = (rpm * elapsed_ms * 1000) / 60000;
        if refill == 0 {
            // 时间不够凑一个 ×1000 单位，先不动 last，让下一次再算
            return;
        }

        let max = rpm * 1000;
        // CAS 循环：把 refill 累加进去，但不超过 max
        loop {
            let current = self.tokens_1000.load(Ordering::Acquire);
            let new_val = (current + refill).min(max);
            if new_val == current {
                break; // 已经在上限
            }
            if self
                .tokens_1000
                .compare_exchange_weak(
                    current,
                    new_val,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                break;
            }
        }
        *last = now;
    }

    /// 更新 RPM 限制
    pub fn set_rpm(&self, rpm: i64) {
        self.rpm.store(rpm, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unlimited_when_rpm_zero() {
        let rl = RateLimiter::new(0);
        // rpm=0 表示不限流，always true
        for _ in 0..1000 {
            assert!(rl.allow());
        }
    }

    #[test]
    fn initial_burst_equals_rpm() {
        let rl = RateLimiter::new(10);
        // 初始桶满，前 10 个 allow 应全 true
        for i in 0..10 {
            assert!(rl.allow(), "request {i} should be allowed initially");
        }
        // 第 11 个应当被拒绝（refill 间隔太短 < 10ms）
        assert!(!rl.allow());
    }

    #[test]
    fn refill_eventually_admits_after_drain() {
        // rpm=600 → 每毫秒 0.01 个令牌（×1000 = 10），20ms ≈ 0.2 token
        let rl = RateLimiter::new(600);
        // 先把桶清空
        while rl.allow() {}
        // 等 200ms，应至少补充 (600 * 200 * 1000) / 60000 = 2000，即 2 个令牌
        std::thread::sleep(std::time::Duration::from_millis(200));
        assert!(rl.allow(), "after 200ms refill should admit at least one");
    }

    #[test]
    fn set_rpm_changes_limit_without_panic() {
        let rl = RateLimiter::new(5);
        for _ in 0..5 {
            assert!(rl.allow());
        }
        rl.set_rpm(0); // 切换到无限制
        for _ in 0..10 {
            assert!(rl.allow());
        }
    }
}
