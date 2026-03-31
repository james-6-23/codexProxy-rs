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
    fn refill(&self) {
        let rpm = self.rpm.load(Ordering::Relaxed);
        if rpm <= 0 {
            return;
        }

        let mut last = self.last_refill.lock();
        let now = Instant::now();
        let elapsed = now.duration_since(*last);
        let elapsed_ms = elapsed.as_millis() as i64;

        if elapsed_ms < 10 {
            return;
        }

        // 每毫秒补充 rpm/60000 个令牌
        let refill = (rpm * elapsed_ms * 1000) / 60000;
        if refill > 0 {
            let max = rpm * 1000;
            let current = self.tokens_1000.load(Ordering::Relaxed);
            let new_val = (current + refill).min(max);
            self.tokens_1000.store(new_val, Ordering::Relaxed);
            *last = now;
        }
    }

    /// 更新 RPM 限制
    pub fn set_rpm(&self, rpm: i64) {
        self.rpm.store(rpm, Ordering::Relaxed);
    }
}
