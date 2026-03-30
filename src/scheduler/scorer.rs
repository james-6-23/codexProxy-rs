use super::*;
use std::sync::atomic::Ordering;

/// 多维评分器
///
/// 不同于 Go 版的单一线性评分，这里将多个维度独立计算后加权合成，
/// 避免单一维度（如一次 401）完全压制其他维度。
pub struct Scorer;

impl Scorer {
    /// 计算账号综合分数，返回 ×100 的整数（如 8520 = 85.20）
    pub fn compute(account: &Account, now_ts: i64) -> i64 {
        let mut score: f64 = 100.0;

        // ── 维度 1：错误惩罚（按时间衰减）──

        // 401 未授权：-50，24 小时线性衰减
        let last_401 = account.last_unauthorized_at.load(Ordering::Relaxed);
        if last_401 > 0 {
            let elapsed = (now_ts - last_401) as f64;
            let decay = (1.0 - elapsed / 86400.0).max(0.0);
            score -= 50.0 * decay;
        }

        // 429 限流：-22，1 小时衰减
        let last_429 = account.last_rate_limited_at.load(Ordering::Relaxed);
        if last_429 > 0 {
            let elapsed = (now_ts - last_429) as f64;
            let decay = (1.0 - elapsed / 3600.0).max(0.0);
            score -= 22.0 * decay;
        }

        // 超时：-18，15 分钟衰减
        let last_timeout = account.last_timeout_at.load(Ordering::Relaxed);
        if last_timeout > 0 {
            let elapsed = (now_ts - last_timeout) as f64;
            let decay = (1.0 - elapsed / 900.0).max(0.0);
            score -= 18.0 * decay;
        }

        // 5xx 服务器错误：-12，15 分钟衰减
        let last_5xx = account.last_server_error_at.load(Ordering::Relaxed);
        if last_5xx > 0 {
            let elapsed = (now_ts - last_5xx) as f64;
            let decay = (1.0 - elapsed / 900.0).max(0.0);
            score -= 12.0 * decay;
        }

        // ── 维度 2：连续失败惩罚 ──

        let fail_streak = account.failure_streak.load(Ordering::Relaxed);
        if fail_streak > 0 {
            // 每次连续失败 -6，最多 -24
            score -= (fail_streak as f64 * 6.0).min(24.0);
        }

        // ── 维度 3：成功率 ──

        let success_rate = account.recent_results.read().success_rate();
        if success_rate < 0.5 {
            score -= 15.0;
        } else if success_rate < 0.75 {
            score -= 8.0;
        }

        // ── 维度 4：用量惩罚 ──

        let usage_7d = account.usage_7d_pct_100.load(Ordering::Relaxed) as f64 / 100.0;
        if usage_7d >= 100.0 {
            // 免费账号满额直接最大惩罚
            let plan = account.plan_type.read();
            if plan.as_str() == "free" {
                score -= 40.0;
            } else {
                score -= 20.0;
            }
        } else if usage_7d >= 70.0 {
            score -= 8.0;
        }

        // ── 维度 5：延迟惩罚 ──

        let latency_ms = account.latency_ewma_100.load(Ordering::Relaxed) as f64 / 100.0;
        if latency_ms >= 20000.0 {
            score -= 15.0;
        } else if latency_ms >= 10000.0 {
            score -= 8.0;
        } else if latency_ms >= 5000.0 {
            score -= 4.0;
        }

        // ── 奖励 ──

        // 连续成功奖励：每次 +2，上限 +12
        let success_streak = account.success_streak.load(Ordering::Relaxed);
        if success_streak > 0 {
            score += (success_streak as f64 * 2.0).min(12.0);
        }

        // 老账号奖励（总请求 >10 为 proven）
        let total = account.total_requests.load(Ordering::Relaxed);
        if total > 10 {
            score += 20.0;
        }

        // 范围限制 [0, 150]
        score = score.clamp(0.0, 150.0);

        (score * 100.0) as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_fresh_account_score() {
        let acc = Account::new(1);
        let now = chrono::Utc::now().timestamp();
        let score = Scorer::compute(&acc, now);
        // 新账号无奖惩，应该约 100 × 100 = 10000
        assert!(score >= 9900 && score <= 10100, "score = {}", score);
    }

    #[test]
    fn test_proven_account_bonus() {
        let acc = Account::new(1);
        acc.total_requests.store(50, Ordering::Relaxed);
        acc.success_streak.store(5, Ordering::Relaxed);
        let now = chrono::Utc::now().timestamp();
        let score = Scorer::compute(&acc, now);
        // 100 + 20(proven) + 10(streak 5×2) = 130 × 100
        assert!(score >= 12900, "score = {}", score);
    }
}
