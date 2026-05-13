use super::*;
use std::sync::atomic::Ordering;

// ── 7d 紧迫度奖励参数 ──
// 移植自 codex2api 提交 6056430 feat(scheduler): prefer accounts nearing 7d reset
//   Paid 账号距离 7d 重置 ≤72h 且仍有显著配额时，给予调度加分以鼓励用完
const PREMIUM_7D_URGENCY_WINDOW_SEC: i64 = 72 * 3600;
const PREMIUM_7D_URGENCY_MAX_BONUS: f64 = 80.0;
const PREMIUM_7D_URGENCY_MIN_REMAINING_PCT: f64 = 5.0;
const PREMIUM_7D_URGENCY_FULL_REMAINING_PCT: f64 = 70.0;

/// 多维评分器
///
/// 不同于 Go 版的单一线性评分，这里将多个维度独立计算后加权合成，
/// 避免单一维度（如一次 401）完全压制其他维度。
pub struct Scorer;

impl Scorer {
    /// 计算账号综合分数，返回 ×100 的整数（如 8520 = 85.20）
    ///
    /// 带分层缓存优化：根据账号健康度使用不同的缓存 TTL
    /// - TIER_HEALTHY: 60 秒（健康账号变化慢）
    /// - TIER_WARM: 30 秒（温和账号需要更频繁评估）
    /// - TIER_RISKY: 10 秒（风险账号需要密切监控）
    /// - TIER_BANNED: 不缓存（立即重算）
    pub fn compute(account: &Account, now_ts: i64) -> i64 {
        let tier = account.health_tier.load(Ordering::Relaxed);

        // 分层 TTL 策略
        let ttl = match tier {
            TIER_HEALTHY => 60,  // 健康账号 60 秒
            TIER_WARM => 30,     // 温和账号 30 秒
            TIER_RISKY => 10,    // 风险账号 10 秒
            _ => 0,              // banned 不缓存
        };

        // 检查缓存是否有效
        if ttl > 0 {
            let cached_at = account.score_cached_at.load(Ordering::Relaxed);
            if cached_at > 0 && now_ts - cached_at < ttl {
                return account.score.load(Ordering::Relaxed);
            }
        }

        // 缓存过期或不缓存，重新计算
        let new_score = Self::compute_fresh(account, now_ts);

        // 更新缓存
        account.score.store(new_score, Ordering::Relaxed);
        account.score_cached_at.store(now_ts, Ordering::Relaxed);

        new_score
    }

    /// 强制重新计算分数（不使用缓存）
    pub fn compute_fresh(account: &Account, now_ts: i64) -> i64 {
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
        let usage_5h = account.usage_5h_pct_100.load(Ordering::Relaxed) as f64 / 100.0;
        let plan = account.plan_type.read();
        let plan_str = plan.as_str();

        // Prolite 按 Pro 处理（使用 5h 窗口）
        let is_pro_tier = plan_str == "pro" || plan_str == "prolite" || plan_str == "plus";

        // Pro/Prolite/Plus 优先检查 5h 窗口
        if is_pro_tier && usage_5h >= 100.0 {
            score -= 20.0;
        } else if usage_7d >= 100.0 {
            // 免费账号满额直接最大惩罚
            if plan_str == "free" {
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

    /// 调度分 = 基础分 + 紧迫度奖励
    ///
    /// 基础分用于 tier 划分（保持 [0, 150] 范围），调度分用于挑选/排序时的偏好。
    /// 这样 7d 配额即将重置的 paid 账号会被优先调度，但不会因奖励错位 tier。
    pub fn dispatch_score(account: &Account, now_ts: i64) -> i64 {
        let base = Self::compute(account, now_ts);
        let bonus = Self::urgency_bonus_7d(account, now_ts);
        base + (bonus * 100.0) as i64
    }

    /// 7d 用量紧迫度奖励（不应用于 tier 评估，仅用于调度排序）
    /// 移植自 codex2api 提交 6056430：paid 账号距离 7d reset ≤72h 且仍有配额时给奖励
    pub fn urgency_bonus_7d(account: &Account, now_ts: i64) -> f64 {
        // 仅 Plus / Pro / Team / Business / Enterprise 等 paid plan 有 7d 窗口
        let plan_is_paid = {
            let plan = account.plan_type.read();
            matches!(
                plan.as_str(),
                "plus" | "pro" | "prolite" | "team" | "business" | "enterprise"
            )
        };
        if !plan_is_paid {
            return 0.0;
        }

        // 未探针到 reset 时间 → 无法判断
        let resets_at = account.resets_at.load(Ordering::Relaxed);
        if resets_at <= 0 {
            return 0.0;
        }

        let usage_7d = account.usage_7d_pct_100.load(Ordering::Relaxed) as f64 / 100.0;
        if usage_7d >= 100.0 {
            return 0.0;
        }

        // 已 banned / 无 token / 冷却中 → 不能用，无意义
        let tier = account.health_tier.load(Ordering::Relaxed);
        if tier == TIER_BANNED {
            return 0.0;
        }
        if account.access_token.read().is_empty() {
            return 0.0;
        }
        let cooldown = account.cooldown_until.load(Ordering::Relaxed);
        if cooldown > now_ts {
            return 0.0;
        }

        // 时间窗口检查
        let time_remaining = resets_at - now_ts;
        if time_remaining <= 0 || time_remaining > PREMIUM_7D_URGENCY_WINDOW_SEC {
            return 0.0;
        }

        // 剩余配额低于阈值 → 给奖励无意义（很快会限流）
        let quota_remaining = 100.0 - usage_7d;
        if quota_remaining <= PREMIUM_7D_URGENCY_MIN_REMAINING_PCT {
            return 0.0;
        }

        // 时间因子：越接近 reset 越大
        let time_factor =
            1.0 - (time_remaining as f64 / PREMIUM_7D_URGENCY_WINDOW_SEC as f64);
        // 配额因子：剩余配额占比，FULL_REMAINING_PCT 之上视为满
        let mut quota_factor = quota_remaining / PREMIUM_7D_URGENCY_FULL_REMAINING_PCT;
        quota_factor = quota_factor.clamp(0.0, 1.0);
        // 加权：始终保留 60% 基础奖励 + 40% 与配额相关，避免低配额账号完全失去优先级
        let weighted_quota_factor = 0.6 + 0.4 * quota_factor;

        PREMIUM_7D_URGENCY_MAX_BONUS * time_factor * weighted_quota_factor
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

    #[test]
    fn test_urgency_bonus_7d_zero_for_free_plan() {
        let acc = Account::new(1);
        *acc.plan_type.write() = "free".to_string();
        *acc.access_token.write() = "tok".to_string();
        let now = chrono::Utc::now().timestamp();
        acc.resets_at.store(now + 3600, Ordering::Relaxed);
        acc.usage_7d_pct_100.store(5000, Ordering::Relaxed); // 50%
        assert_eq!(Scorer::urgency_bonus_7d(&acc, now), 0.0);
    }

    #[test]
    fn test_urgency_bonus_7d_zero_outside_window() {
        let acc = Account::new(1);
        *acc.plan_type.write() = "plus".to_string();
        *acc.access_token.write() = "tok".to_string();
        let now = chrono::Utc::now().timestamp();
        // reset 远在 72h 之后
        acc.resets_at.store(now + 100 * 3600, Ordering::Relaxed);
        acc.usage_7d_pct_100.store(5000, Ordering::Relaxed);
        assert_eq!(Scorer::urgency_bonus_7d(&acc, now), 0.0);
    }

    #[test]
    fn test_urgency_bonus_7d_zero_when_quota_exhausted() {
        let acc = Account::new(1);
        *acc.plan_type.write() = "pro".to_string();
        *acc.access_token.write() = "tok".to_string();
        let now = chrono::Utc::now().timestamp();
        acc.resets_at.store(now + 3600, Ordering::Relaxed);
        acc.usage_7d_pct_100.store(9700, Ordering::Relaxed); // 97% — quota_remaining=3 < 5
        assert_eq!(Scorer::urgency_bonus_7d(&acc, now), 0.0);
    }

    #[test]
    fn test_urgency_bonus_7d_max_near_reset_with_full_quota() {
        let acc = Account::new(1);
        *acc.plan_type.write() = "plus".to_string();
        *acc.access_token.write() = "tok".to_string();
        let now = chrono::Utc::now().timestamp();
        // 10 秒后 reset，配额 0% 用 → 近满奖励（避免 CI flake：now+1 在慢机器上可能 time_remaining<=0）
        acc.resets_at.store(now + 10, Ordering::Relaxed);
        acc.usage_7d_pct_100.store(0, Ordering::Relaxed);
        let bonus = Scorer::urgency_bonus_7d(&acc, now);
        // 10/259200 ≈ 0，time_factor 仍 ≈ 1.0；quota_factor=1.0, weighted=1.0 → ~80.0
        assert!(bonus > 79.0 && bonus <= 80.0, "bonus = {}", bonus);
    }

    #[test]
    fn test_urgency_bonus_7d_skipped_when_banned() {
        let acc = Account::new(1);
        *acc.plan_type.write() = "plus".to_string();
        *acc.access_token.write() = "tok".to_string();
        acc.health_tier.store(TIER_BANNED, Ordering::Relaxed);
        let now = chrono::Utc::now().timestamp();
        acc.resets_at.store(now + 3600, Ordering::Relaxed);
        acc.usage_7d_pct_100.store(5000, Ordering::Relaxed);
        assert_eq!(Scorer::urgency_bonus_7d(&acc, now), 0.0);
    }

    #[test]
    fn test_urgency_bonus_7d_skipped_in_cooldown() {
        let acc = Account::new(1);
        *acc.plan_type.write() = "pro".to_string();
        *acc.access_token.write() = "tok".to_string();
        let now = chrono::Utc::now().timestamp();
        acc.resets_at.store(now + 3600, Ordering::Relaxed);
        acc.usage_7d_pct_100.store(5000, Ordering::Relaxed);
        acc.cooldown_until.store(now + 300, Ordering::Relaxed);
        assert_eq!(Scorer::urgency_bonus_7d(&acc, now), 0.0);
    }
}
