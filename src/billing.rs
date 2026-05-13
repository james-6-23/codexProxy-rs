/// 计费统计模块 — 模型定价与成本计算

use std::collections::HashMap;
use std::sync::LazyLock;

/// 模型定价（每百万 token 的价格，单位：美元）
#[derive(Debug, Clone)]
pub struct ModelPricing {
    pub input_price_per_m: f64,
    pub output_price_per_m: f64,
    pub cache_read_price_per_m: f64,
    pub input_price_per_m_priority: f64,
    pub output_price_per_m_priority: f64,
    pub cache_read_price_per_m_priority: f64,
}

impl Default for ModelPricing {
    fn default() -> Self {
        Self {
            input_price_per_m: 1.0,
            output_price_per_m: 2.0,
            cache_read_price_per_m: 0.1,
            input_price_per_m_priority: 2.0,
            output_price_per_m_priority: 4.0,
            cache_read_price_per_m_priority: 0.2,
        }
    }
}

/// 模型定价表（参考 Go 版本 billing.go）
static PRICING_TABLE: LazyLock<HashMap<&'static str, ModelPricing>> = LazyLock::new(|| {
    let mut map = HashMap::new();

    // GPT-5 系列
    map.insert("gpt-5.5", ModelPricing {
        input_price_per_m: 5.0,
        output_price_per_m: 30.0,
        cache_read_price_per_m: 0.5,
        input_price_per_m_priority: 10.0,
        output_price_per_m_priority: 60.0,
        cache_read_price_per_m_priority: 1.0,
    });

    map.insert("gpt-5.4", ModelPricing {
        input_price_per_m: 2.5,
        output_price_per_m: 15.0,
        cache_read_price_per_m: 0.25,
        input_price_per_m_priority: 5.0,
        output_price_per_m_priority: 30.0,
        cache_read_price_per_m_priority: 0.5,
    });

    map.insert("gpt-5.4-mini", ModelPricing {
        input_price_per_m: 0.75,
        output_price_per_m: 4.5,
        cache_read_price_per_m: 0.075,
        input_price_per_m_priority: 1.5,
        output_price_per_m_priority: 9.0,
        cache_read_price_per_m_priority: 0.15,
    });

    map.insert("gpt-5.4-nano", ModelPricing {
        input_price_per_m: 0.2,
        output_price_per_m: 1.25,
        cache_read_price_per_m: 0.02,
        input_price_per_m_priority: 0.4,
        output_price_per_m_priority: 2.5,
        cache_read_price_per_m_priority: 0.04,
    });

    map.insert("gpt-5.3-codex-spark", ModelPricing {
        input_price_per_m: 1.5,
        output_price_per_m: 12.0,
        cache_read_price_per_m: 0.15,
        input_price_per_m_priority: 3.0,
        output_price_per_m_priority: 24.0,
        cache_read_price_per_m_priority: 0.3,
    });

    map.insert("gpt-5.3-codex", ModelPricing {
        input_price_per_m: 1.5,
        output_price_per_m: 12.0,
        cache_read_price_per_m: 0.15,
        input_price_per_m_priority: 3.0,
        output_price_per_m_priority: 24.0,
        cache_read_price_per_m_priority: 0.3,
    });

    map.insert("gpt-5.2", ModelPricing {
        input_price_per_m: 1.75,
        output_price_per_m: 14.0,
        cache_read_price_per_m: 0.175,
        input_price_per_m_priority: 3.5,
        output_price_per_m_priority: 28.0,
        cache_read_price_per_m_priority: 0.35,
    });

    // GPT-4 系列
    map.insert("gpt-4o-mini", ModelPricing {
        input_price_per_m: 0.15,
        output_price_per_m: 0.6,
        cache_read_price_per_m: 0.015,
        input_price_per_m_priority: 0.3,
        output_price_per_m_priority: 1.2,
        cache_read_price_per_m_priority: 0.03,
    });

    map.insert("gpt-4o", ModelPricing {
        input_price_per_m: 2.5,
        output_price_per_m: 10.0,
        cache_read_price_per_m: 0.25,
        input_price_per_m_priority: 5.0,
        output_price_per_m_priority: 20.0,
        cache_read_price_per_m_priority: 0.5,
    });

    map.insert("gpt-4-turbo", ModelPricing {
        input_price_per_m: 10.0,
        output_price_per_m: 30.0,
        cache_read_price_per_m: 1.0,
        input_price_per_m_priority: 20.0,
        output_price_per_m_priority: 60.0,
        cache_read_price_per_m_priority: 2.0,
    });

    map.insert("gpt-4", ModelPricing {
        input_price_per_m: 30.0,
        output_price_per_m: 60.0,
        cache_read_price_per_m: 3.0,
        input_price_per_m_priority: 60.0,
        output_price_per_m_priority: 120.0,
        cache_read_price_per_m_priority: 6.0,
    });

    // o1 系列
    map.insert("o1", ModelPricing {
        input_price_per_m: 15.0,
        output_price_per_m: 60.0,
        cache_read_price_per_m: 1.5,
        input_price_per_m_priority: 30.0,
        output_price_per_m_priority: 120.0,
        cache_read_price_per_m_priority: 3.0,
    });

    map.insert("o1-mini", ModelPricing {
        input_price_per_m: 3.0,
        output_price_per_m: 12.0,
        cache_read_price_per_m: 0.3,
        input_price_per_m_priority: 6.0,
        output_price_per_m_priority: 24.0,
        cache_read_price_per_m_priority: 0.6,
    });

    map
});

/// 成本明细
#[derive(Debug, Clone)]
pub struct CostBreakdown {
    pub input_cost: f64,
    pub output_cost: f64,
    pub cache_read_cost: f64,
    pub total_cost: f64,
    pub input_price_per_m: f64,
    pub output_price_per_m: f64,
    pub cache_read_price_per_m: f64,
}

/// 计算请求成本
pub fn calculate_cost(
    model: &str,
    input_tokens: i64,
    output_tokens: i64,
    cached_tokens: i64,
    service_tier: &str,
) -> CostBreakdown {
    // 查找模型定价（前缀匹配，支持版本号后缀）
    let pricing = PRICING_TABLE
        .iter()
        .find(|(key, _)| model.starts_with(*key))
        .map(|(_, p)| p.clone())
        .unwrap_or_else(|| ModelPricing::default());

    // 根据 service_tier 选择定价
    let is_priority = service_tier.eq_ignore_ascii_case("fast") || service_tier.eq_ignore_ascii_case("priority");

    let input_price = if is_priority {
        pricing.input_price_per_m_priority
    } else {
        pricing.input_price_per_m
    };

    let output_price = if is_priority {
        pricing.output_price_per_m_priority
    } else {
        pricing.output_price_per_m
    };

    let cache_price = if is_priority {
        pricing.cache_read_price_per_m_priority
    } else {
        pricing.cache_read_price_per_m
    };

    // 计算成本（token 数 / 1,000,000 × 单价）
    let input_cost = (input_tokens as f64 / 1_000_000.0) * input_price;
    let output_cost = (output_tokens as f64 / 1_000_000.0) * output_price;
    let cache_read_cost = (cached_tokens as f64 / 1_000_000.0) * cache_price;
    let total_cost = input_cost + output_cost + cache_read_cost;

    CostBreakdown {
        input_cost,
        output_cost,
        cache_read_cost,
        total_cost,
        input_price_per_m: input_price,
        output_price_per_m: output_price,
        cache_read_price_per_m: cache_price,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gpt54_cost() {
        let cost = calculate_cost("gpt-5.4", 1_000_000, 1_000_000, 0, "");
        assert_eq!(cost.input_cost, 2.5);
        assert_eq!(cost.output_cost, 15.0);
        assert_eq!(cost.total_cost, 17.5);
    }

    #[test]
    fn test_priority_tier_multiplier() {
        let normal = calculate_cost("gpt-5.4", 1_000_000, 0, 0, "");
        let priority = calculate_cost("gpt-5.4", 1_000_000, 0, 0, "fast");
        assert_eq!(priority.input_cost, normal.input_cost * 2.0);
    }

    #[test]
    fn test_cache_cost() {
        let cost = calculate_cost("gpt-5.4", 0, 0, 1_000_000, "");
        assert_eq!(cost.cache_read_cost, 0.25);
    }

    #[test]
    fn test_unknown_model_fallback() {
        let cost = calculate_cost("unknown-model", 1_000_000, 1_000_000, 0, "");
        assert_eq!(cost.input_cost, 1.0); // 默认定价
        assert_eq!(cost.output_cost, 2.0);
    }
}
