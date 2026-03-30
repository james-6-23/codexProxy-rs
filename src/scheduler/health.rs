use super::*;
use std::sync::atomic::Ordering;

impl Scheduler {
    /// 根据账号状态重新计算健康等级和动态并发
    pub fn recompute_health(&self, account: &Account) {
        let now = chrono::Utc::now().timestamp();
        let score = scorer::Scorer::compute(account, now);

        // 存储分数
        account.score.store(score, Ordering::Relaxed);

        let score_f = score as f64 / 100.0;
        let max_c = self.max_concurrency.load(Ordering::Relaxed);

        // 判断 tier + 动态并发
        let (tier, concurrency) = if account.last_unauthorized_at.load(Ordering::Relaxed) > 0 {
            let elapsed = now - account.last_unauthorized_at.load(Ordering::Relaxed);
            if elapsed < 21600 {
                // 6 小时内 401 → banned
                (TIER_BANNED, 0)
            } else if score_f > 80.0 {
                (TIER_HEALTHY, max_c)
            } else {
                (TIER_WARM, max_c / 2)
            }
        } else if score_f >= 80.0 {
            (TIER_HEALTHY, max_c)
        } else if score_f >= 50.0 {
            // Little's Law 自适应
            let latency_ms = account.latency_ewma_100.load(Ordering::Relaxed) as f64 / 100.0;
            let adaptive_c = if latency_ms > 0.0 {
                let target_rps = 2.0;
                let optimal = (target_rps * latency_ms / 1000.0).ceil() as i64;
                optimal.clamp(1, max_c / 2)
            } else {
                (max_c / 2).max(1)
            };
            (TIER_WARM, adaptive_c)
        } else if score_f >= 20.0 {
            (TIER_RISKY, 1)
        } else {
            (TIER_BANNED, 0)
        };

        account.health_tier.store(tier, Ordering::Relaxed);
        account
            .dynamic_concurrency_limit
            .store(concurrency, Ordering::Relaxed);
    }

    /// 标记账号进入冷却（限流/禁用）
    pub fn mark_cooldown(&self, account: &Account, reason: &str, duration_secs: i64) {
        let until = chrono::Utc::now().timestamp() + duration_secs;
        account.cooldown_until.store(until, Ordering::Relaxed);
        self.recompute_health(account);
    }

    /// 清除冷却状态
    pub fn clear_cooldown(&self, account: &Account) {
        account.cooldown_until.store(0, Ordering::Relaxed);
        self.recompute_health(account);
        self.notify_available();
    }

    /// 标记账号为 banned（401）
    pub fn mark_banned(&self, account: &Account) {
        let now = chrono::Utc::now().timestamp();
        account.last_unauthorized_at.store(now, Ordering::Relaxed);
        account.health_tier.store(TIER_BANNED, Ordering::Relaxed);
        account.dynamic_concurrency_limit.store(0, Ordering::Relaxed);

        // 冷却 6 小时
        let until = now + 6 * 3600;
        account.cooldown_until.store(until, Ordering::Relaxed);
    }

    /// 尝试恢复被 ban 的账号
    pub fn try_recover(&self, account: &Account) {
        account.health_tier.store(TIER_WARM, Ordering::Relaxed);
        account.cooldown_until.store(0, Ordering::Relaxed);
        account.score.store(8000, Ordering::Relaxed); // 重置为 80 分
        account.failure_streak.store(0, Ordering::Relaxed);

        let max_c = self.max_concurrency.load(Ordering::Relaxed);
        account
            .dynamic_concurrency_limit
            .store((max_c / 2).max(1), Ordering::Relaxed);

        self.rebuild_buckets();
        self.notify_available();
    }

    /// 批量重算所有账号的健康状态 + 重建分桶
    pub fn recompute_all(&self) {
        let accounts = self.accounts.read().clone();
        for acc in &accounts {
            self.recompute_health(acc);
        }
        drop(accounts);
        self.rebuild_buckets();
    }
}
