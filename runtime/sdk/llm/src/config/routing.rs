/*
 * Copyright (c) 2026 Talon Contributors
 */
//! 模型路由 — 按意图自动选择模型。

/// 模型路由配置（用户/App 层设置）。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct ModelRouting {
    pub routes: Vec<ModelRoute>,
}

/// 单条路由规则。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ModelRoute {
    /// 意图标识：chat / summarize / embed / agent / thinking
    pub intent: String,
    /// 目标 Provider ID
    pub provider_id: String,
    /// 目标模型名
    pub model: String,
}

impl ModelRouting {
    /// 按意图查找路由 → 返回 (provider_id, model)。
    pub fn resolve(&self, intent: &str) -> Option<(&str, &str)> {
        self.routes
            .iter()
            .find(|r| r.intent == intent)
            .map(|r| (r.provider_id.as_str(), r.model.as_str()))
    }

    /// 是否为空（无路由规则）。
    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }
}
