/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 */
//! AI 模型统一配置 + per-model 精细配置。

use std::collections::HashMap;

/// 统一 LLM 配置 — 多 Provider 多模型注册。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AiModelConfig {
    pub providers: Vec<ProviderConfig>,
}

/// 单个 Provider 配置。
///
/// `api_key` 字段在序列化和 Debug 输出时自动屏蔽，防止日志泄露。
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ProviderConfig {
    pub id: String,
    pub display_name: String,
    pub api_base: String,
    /// API 密钥——序列化时自动跳过，Debug 输出为 `***`。
    #[serde(skip_serializing)]
    pub api_key: String,
    pub models: Vec<String>,
    pub protocol: ProviderProtocol,
    pub enabled: bool,
    /// per-model 精细配置覆盖（model_id → 配置）。
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub model_overrides: HashMap<String, ModelOverride>,
    /// 厂商兼容性提示 — 可覆盖基于 URL 的自动检测。
    ///
    /// 支持的键（均可选，不设则走自动检测）：
    /// - `"vendor"`: 厂商标识，如 `"deepseek"` / `"qwen"` / `"moonshot"` / `"zhipu"` 等
    /// - `"use_max_tokens"`: `true` 时强制使用 `max_tokens` 而非 `max_completion_tokens`
    /// - `"normalize_tool_ids"`: `true` 时截断 tool_call ID（Mistral 兼容）
    /// - `"supports_reasoning_effort"`: `true` 时启用 reasoning_effort 参数
    /// - `"supports_developer_role"`: `true` 时使用 developer 角色替代 system
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub compat_hints: HashMap<String, serde_json::Value>,
}

impl std::fmt::Debug for ProviderConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProviderConfig")
            .field("id", &self.id)
            .field("display_name", &self.display_name)
            .field("api_base", &self.api_base)
            .field("api_key", &"***")
            .field("models", &self.models)
            .field("protocol", &self.protocol)
            .field("enabled", &self.enabled)
            .field("model_overrides", &self.model_overrides)
            .field("compat_hints", &self.compat_hints)
            .finish()
    }
}

/// Provider 协议类型。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ProviderProtocol {
    OpenAiCompatible,
    Anthropic,
    Gemini,
    Ollama,
}

/// 单模型配置覆盖。
///
/// 允许用户为特定模型设置独立的参数，
/// 覆盖 Provider 级别的默认值。
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ModelOverride {
    /// 自定义温度（覆盖请求级 temperature）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,

    /// 自定义最大输出 Token。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,

    /// 推理强度（low / medium / high / auto）。
    /// 某些模型（如 o1, o3）支持通过此参数控制深度推理。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,

    /// 是否启用流式输出。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,

    /// 自定义系统提示词追加（不替换，在末尾拼接）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_prompt_suffix: Option<String>,

    /// 额外自定义键值对（供特定 Provider 使用）。
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub extra: HashMap<String, serde_json::Value>,
}

impl ModelOverride {
    /// 检查是否所有字段都是默认值（即无实际覆盖）。
    pub fn is_empty(&self) -> bool {
        self.temperature.is_none()
            && self.max_tokens.is_none()
            && self.reasoning_effort.is_none()
            && self.stream.is_none()
            && self.system_prompt_suffix.is_none()
            && self.extra.is_empty()
    }
}
