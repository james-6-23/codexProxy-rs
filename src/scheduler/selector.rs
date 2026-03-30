use super::*;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crate::scheduler::scorer::Scorer;

impl Scheduler {
    /// 核心选择算法：O(1) 分桶 round-robin + fallback 全扫描
    ///
    /// 返回 (账号引用, 已自动 acquire)
    pub fn next_account(&self, exclude: &HashSet<i64>) -> Option<Arc<Account>> {
        // 优先从分桶中选择
        if let Some(acc) = self.select_from_buckets(exclude) {
            return Some(acc);
        }
        // fallback: 全量扫描（处理桶可能过期的情况）
        self.select_full_scan(exclude)
    }

    /// 等待可用账号（带超时）
    pub async fn wait_for_available(
        &self,
        exclude: &HashSet<i64>,
        timeout: Duration,
    ) -> Option<Arc<Account>> {
        // 先尝试立即获取
        if let Some(acc) = self.next_account(exclude) {
            return Some(acc);
        }

        // 等待通知或超时
        tokio::select! {
            _ = self.available_notify.notified() => {
                self.next_account(exclude)
            }
            _ = tokio::time::sleep(timeout) => {
                // 超时前最后尝试一次
                self.next_account(exclude)
            }
        }
    }

    /// 从分桶中 round-robin 选择
    fn select_from_buckets(&self, exclude: &HashSet<i64>) -> Option<Arc<Account>> {
        let accounts = self.accounts.read();
        let buckets = self.tier_buckets.read();

        // 按优先级遍历：healthy → warm → risky
        for (tier_idx, bucket) in [&buckets.healthy, &buckets.warm, &buckets.risky]
            .iter()
            .enumerate()
        {
            if bucket.is_empty() {
                continue;
            }

            let cursor = &buckets.cursors[tier_idx];
            let len = bucket.len();

            // 最多尝试 bucket 大小次（避免死循环）
            for _ in 0..len {
                let pos = cursor.fetch_add(1, Ordering::Relaxed) as usize % len;
                let acc_idx = bucket[pos];

                if acc_idx >= accounts.len() {
                    continue;
                }

                let acc = &accounts[acc_idx];

                if exclude.contains(&acc.db_id) {
                    continue;
                }

                if !acc.is_available() {
                    continue;
                }

                // CAS 获取并发槽
                if acc.try_acquire() {
                    return Some(Arc::clone(acc));
                }
            }
        }

        None
    }

    /// 全量扫描 — 按 score 降序选择最佳可用账号
    fn select_full_scan(&self, exclude: &HashSet<i64>) -> Option<Arc<Account>> {
        let accounts = self.accounts.read();
        let now = chrono::Utc::now().timestamp();

        let mut best: Option<&Arc<Account>> = None;
        let mut best_score: i64 = i64::MIN;

        for acc in accounts.iter() {
            if exclude.contains(&acc.db_id) {
                continue;
            }
            if !acc.is_available() {
                continue;
            }

            // 实时计算分数
            let score = Scorer::compute(acc, now);

            // 加入负载因子：当前活跃请求越少越好
            let load_penalty = acc.active_requests.load(Ordering::Relaxed) * 500;
            let effective_score = score - load_penalty;

            if effective_score > best_score {
                best_score = effective_score;
                best = Some(acc);
            }
        }

        if let Some(acc) = best {
            if acc.try_acquire() {
                return Some(Arc::clone(acc));
            }
        }

        None
    }
}
