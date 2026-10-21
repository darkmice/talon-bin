/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 */
//! 工具定义 — JSON Schema 兼容的工具参数描述。

/// 工具参数描述（JSON Schema 风格）。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ParameterSchema {
    /// 参数名。
    pub name: String,
    /// 参数类型（string / number / boolean / object / array）。
    #[serde(rename = "type")]
    pub param_type: String,
    /// 参数描述。
    pub description: String,
    /// 是否必填。
    pub required: bool,
    /// 枚举值（可选）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enum_values: Option<Vec<String>>,
    /// 默认值（可选）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,
}

/// 工具分类标签。
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize, Default,
)]
pub enum ToolCategory {
    /// 信息查询类（低风险）
    #[default]
    Query,
    /// 数据修改类（中风险）
    Mutation,
    /// 系统操作类（高风险）
    System,
    /// 外部通信类（中风险）
    Communication,
    /// 代码执行类（根据沙箱等级决定风险）
    CodeExecution,
}

impl ToolCategory {
    /// Conservative default for whether tools in this category are read-only.
    pub fn default_read_only(self) -> bool {
        matches!(self, Self::Query)
    }

    /// Conservative default for whether tools in this category are safe to schedule concurrently.
    pub fn default_concurrency_safe(self) -> bool {
        matches!(self, Self::Query)
    }
}
