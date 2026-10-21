/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 */
//! StreamEvent — 强类型流式事件协议。
//!
//! 所有 LLM 的 SSE 输出被映射为强类型事件（TextDelta / ToolCall / Thinking），
//! UI 层无需等待完整 JSON 闭合即可进行实时渲染。

/// LLM 流式事件枚举。
///
/// 分为四大类：
/// 1. **生命周期** — StreamStart / StreamEnd / StreamError
/// 2. **内容输出** — TextDelta / ThinkingDelta / ToolCall*
/// 3. **度量上报** — Usage / CostUpdate
/// 4. **运维信号** — ProviderSwitch / RetryAttempt / RateLimitWait
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum StreamEvent {
    // ── 生命周期 ─────────────────────────
    /// 流开始，携带模型信息。
    StreamStart { model: String, request_id: String },
    /// 流正常结束。
    StreamEnd { finish_reason: FinishReason },
    /// 流异常。
    StreamError { error: String, retryable: bool },

    // ── 文本生成 ─────────────────────────
    /// 增量文本片段。
    TextDelta { text: String },
    /// 文本生成完成（完整文本）。
    TextComplete { text: String },

    // ── 生成式 UI 块 ─────────────────────────
    /// LLM 在输出流里夹带的可渲染 UI 块（`⟦talon:block⟧` 围栏，由 talon-block 解析）。
    ///
    /// 与 tool-use 平级的输出契约：基座负责从流里切出块、原样投递；`block_type` 是
    /// 开放字符串，具体类型语义（spark_app / employee_card …）与渲染由应用层注册。
    /// `block_type` / `payload` 平铺，避免与外层 `#[serde(tag = "type")]` 的 `type` 冲突。
    UiBlock {
        block_type: String,
        #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
        payload: serde_json::Value,
    },

    // ── 推理/思考 ─────────────────────────
    /// 推理/思考开始。
    ///
    /// `thinking_id` 用于关联同一思考块的 Start/Delta/End 序列，
    /// 支持模型同时产生多个 interleaved thinking 块。
    ThinkingStart {
        #[serde(skip_serializing_if = "Option::is_none")]
        thinking_id: Option<String>,
    },
    /// 推理/思考增量。
    ThinkingDelta {
        text: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        thinking_id: Option<String>,
    },
    /// 推理/思考结束。
    ThinkingEnd {
        #[serde(skip_serializing_if = "Option::is_none")]
        thinking_id: Option<String>,
    },

    // ── 工具调用 ─────────────────────────
    /// 工具调用开始。
    ToolCallStart {
        tool_call_id: String,
        tool_name: String,
    },
    /// 工具调用参数增量。
    ToolCallArgumentsDelta { tool_call_id: String, delta: String },
    /// 工具调用参数完成。
    ToolCallComplete {
        tool_call_id: String,
        tool_name: String,
        arguments: String,
    },
    /// 工具执行结果（从 Agent 侧回注的结果，不是 LLM 生成的）。
    ToolResult {
        tool_call_id: String,
        result: String,
        is_error: bool,
    },

    // ── 使用量 ─────────────────────────
    /// Token 用量更新。
    Usage {
        input_tokens: u32,
        output_tokens: u32,
        thinking_tokens: Option<u32>,
        /// Prompt cache 命中的 token 数（Anthropic: cache_read_input_tokens；
        /// OpenAI: prompt_tokens_details.cached_tokens）。
        /// 费率通常为普通 input_tokens 的 0.1x。
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cached_input_tokens: Option<u32>,
    },
    /// 成本更新。
    CostUpdate {
        cost_cents: u64,
        cumulative_cost_cents: u64,
    },

    // ── 上下文管理 ─────────────────────────
    /// 上下文窗口压缩事件。
    ContextCompressed {
        original_tokens: u32,
        compressed_tokens: u32,
    },
    /// 上下文截断警告。
    ContextTruncated { removed_messages: u32 },

    // ── Provider 运维信号 ─────────────────────────
    /// Provider 切换（fallback 触发）。
    ProviderSwitch {
        from: String,
        to: String,
        reason: String,
    },
    /// 重试通知。
    RetryAttempt {
        attempt: u32,
        max_retries: u32,
        delay_ms: u64,
    },
    /// 限流等待。
    RateLimitWait { wait_ms: u64 },

    // ── Agent 集成 ─────────────────────────
    /// Agent 循环轮次标记。
    IterationStart { iteration: u32 },
}

/// 完成原因。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FinishReason {
    /// 正常停止（模型主动结束）。
    Stop,
    /// 需要工具调用。
    ToolUse,
    /// 达到最大 Token 上限。
    MaxTokens,
    /// 内容被安全过滤器拦截。
    ContentFilter,
    /// 发生错误。
    Error,
    /// 被用户取消（CancellationToken 触发）。
    Cancelled,
}
