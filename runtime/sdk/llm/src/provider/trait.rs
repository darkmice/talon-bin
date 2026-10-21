/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 */
//! LanguageModelProvider trait — 所有 LLM 适配器实现此接口。
//!
//! 提供：
//! - 模型枚举与能力声明（Capabilities 协商）
//! - 流式补全（返回 `AsyncStream<StreamEvent>`）
//! - Token 计数（精准上下文预算管理）
//! - 全链路打断支持（`CancellationToken`）

use crate::stream::StreamEvent;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use super::image_generation::{ImageGenerationRequest, ImageGenerationResponse};

// ── 模型能力与元数据 ─────────────────────────

/// 模型能力声明。
///
/// Agent 引擎根据此声明**按需注入**工具和上下文，
/// 而非盲目暴露所有能力给 LLM。
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ModelCapabilities {
    /// 是否支持图像输入（Vision）。
    #[serde(default)]
    pub vision: bool,
    /// 是否支持工具调用（Function Calling）。
    #[serde(default)]
    pub tool_calling: bool,
    /// 是否支持 Agent Mode（多轮自主循环）。
    #[serde(default)]
    pub agent_mode: bool,
    /// 是否支持深度推理/思考（Thinking/Reasoning）。
    #[serde(default)]
    pub thinking: bool,
    /// 是否支持文件/文档类输入（如 PDF、DOCX 附件）。
    #[serde(default)]
    pub file_input: bool,
    /// 是否支持图像生成。
    #[serde(default)]
    pub image_generation: bool,
    /// 该模型专属的编辑工具白名单。
    /// 为空表示无特殊限制。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub edit_tools: Vec<String>,
}

impl ModelCapabilities {
    /// 判断模型是否适合 Agent Mode：
    /// agentMode 未显式禁用 且 支持 toolCalling。
    pub fn suitable_for_agent_mode(&self) -> bool {
        self.agent_mode && self.tool_calling
    }
}

/// 模型信息。
///
/// 每个模型在注册时必须完整上报此结构，引擎据此进行路由和预算管理。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ModelInfo {
    /// 模型唯一标识（如 `claude-sonnet-4-20250514`）。
    pub id: String,
    /// 显示名称（如 `Claude Sonnet 4`）。
    pub display_name: String,
    /// 供应商标识（如 `anthropic`, `openai`）。
    pub vendor: String,
    /// 模型族（如 `claude`, `gpt`）。
    pub family: String,
    /// 模型版本（如 `4.0`, `20250514`），用于 Selector 精确匹配。
    pub version: String,
    /// 最大输入 Token 数。
    pub max_input_tokens: u32,
    /// 最大输出 Token 数。
    pub max_output_tokens: u32,
    /// 上下文窗口大小（通常 = max_input_tokens + max_output_tokens 或独立值）。
    pub context_window: u32,
    /// 输入价格（每百万 Token，美元）。
    pub input_price_per_m: f64,
    /// 输出价格（每百万 Token，美元）。
    pub output_price_per_m: f64,
    /// 模型能力声明。引擎据此进行功能协商与 Tool 注入过滤。
    pub capabilities: ModelCapabilities,
}

// ── 消息角色 ─────────────────────────

/// 消息角色 — 强类型枚举，不再用裸 String。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    System,
    User,
    Assistant,
    Tool,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::System => write!(f, "system"),
            Role::User => write!(f, "user"),
            Role::Assistant => write!(f, "assistant"),
            Role::Tool => write!(f, "tool"),
        }
    }
}

// ── 工具定义 & 调用 ─────────────────────────

/// 工具定义 — 传给 LLM 的工具描述。
///
/// 对应 OpenAI 的 `tools[].function` 和 Anthropic 的 `tools[]`。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ToolDefinition {
    /// 工具名称（如 `read_file`）。
    pub name: String,
    /// 工具描述（LLM 据此决策是否调用）。
    pub description: String,
    /// 输入参数 JSON Schema。
    pub parameters: serde_json::Value,
    /// 是否启用严格模式（OpenAI Structured Outputs）。
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub strict: bool,
}

/// 工具调用信息 — 模型发出的调用请求。
///
/// OpenAI 返回 `tool_calls[].function`，Anthropic 返回 `content[].tool_use`，
/// 统一映射为此结构。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ToolCallInfo {
    /// 调用 ID（用于关联 ToolResult）。
    pub id: String,
    /// 工具名称。
    pub name: String,
    /// 参数 JSON 字符串。
    pub arguments: String,
}

impl ToolCallInfo {
    /// 解析 arguments 为强类型 JSON Value。
    ///
    /// 用于上层消费前校验 LLM 输出的合法性。
    /// 返回 `Err` 说明模型输出了非法 JSON（需要重试或降级处理）。
    pub fn parsed_arguments(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::from_str(&self.arguments)
    }
}

// ── Prompt 缓存策略 ─────────────────────────

/// Prompt 缓存策略——控制 Provider 如何标记缓存断点。
///
/// Anthropic: 在 system prompt / 最后一条 message 上附加 `cache_control`。
/// OpenAI: 自动前缀缓存，此字段被忽略。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default, PartialEq, Eq)]
pub enum CachePolicy {
    /// 不主动标记缓存（Provider 默认行为）。
    #[default]
    Default,
    /// 标记 system prompt 为缓存断点（ephemeral，~5min TTL）。
    SystemPrompt,
    /// 标记 system prompt + 最后一条消息为缓存断点。
    SystemPromptAndLastMessage,
}

// ── 补全请求 ─────────────────────────

/// 补全请求。
///
/// 由 `talon-ai` 或 `talon-agent` 组装后传入 Provider。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompletionRequest {
    /// 目标模型 ID。
    pub model: String,
    /// 消息列表。
    pub messages: Vec<ChatMessage>,
    /// 系统提示词——由上层 PromptBuilder 动态组装后注入。
    pub system_prompt: Option<String>,
    /// 工具定义列表——按需注入，不是一次性全量。
    pub tools: Vec<ToolDefinition>,
    /// 温度。
    pub temperature: Option<f64>,
    /// 最大输出 Token。
    pub max_tokens: Option<u32>,
    /// 是否启用流式输出（默认 true）。
    pub stream: bool,
    /// 推理强度（部分模型如 o1 / o3 支持）。
    pub reasoning_effort: Option<String>,
    /// stop sequences。
    pub stop: Option<Vec<String>>,
    /// Prompt 缓存控制——Provider 按需消费。
    /// Anthropic: ephemeral cache_control + 可选 TTL。
    /// OpenAI: 自动前缀缓存无需额外标记，此字段被忽略。
    #[serde(default)]
    pub cache_policy: CachePolicy,
}

impl CompletionRequest {
    pub fn new(model: &str, user_message: &str) -> Self {
        Self {
            model: model.to_string(),
            messages: vec![ChatMessage {
                role: Role::User,
                content: ChatMessageContent::from(user_message),
                tool_calls: None,
                tool_call_id: None,
                name: None,
            }],
            system_prompt: None,
            tools: vec![],
            temperature: None,
            max_tokens: None,
            stream: true,
            reasoning_effort: None,
            stop: None,
            cache_policy: CachePolicy::Default,
        }
    }

    /// Check whether any message in this request contains `ChatMessagePart::File`.
    pub fn has_file_parts(&self) -> bool {
        self.messages.iter().any(|m| match &m.content {
            ChatMessageContent::Parts(parts) => parts
                .iter()
                .any(|p| matches!(p, ChatMessagePart::File { .. })),
            ChatMessageContent::Text(_) => false,
        })
    }

    /// Validate that the model's capabilities cover the parts present in this request.
    ///
    /// Returns `Err(LlmError::UnsupportedCapability)` if the request contains file
    /// parts but the model does not declare `file_input`, or if it contains image
    /// parts but the model does not declare `vision`.
    pub fn validate_capabilities(&self, caps: &ModelCapabilities) -> Result<(), crate::LlmError> {
        if !self.tools.is_empty() && !caps.tool_calling {
            return Err(crate::LlmError::UnsupportedCapability {
                capability: "tool_calling".to_string(),
                detail: format!(
                    "Request contains {} tool definitions but model {} does not support tool calling",
                    self.tools.len(),
                    self.model
                ),
            });
        }
        if self.has_file_parts() && !caps.file_input {
            return Err(crate::LlmError::UnsupportedCapability {
                capability: "file_input".to_string(),
                detail: format!(
                    "Request contains file/document parts but model {} does not support file input",
                    self.model
                ),
            });
        }
        if self.has_images() && !caps.vision {
            return Err(crate::LlmError::UnsupportedCapability {
                capability: "vision".to_string(),
                detail: format!(
                    "Request contains image parts but model {} does not support vision",
                    self.model
                ),
            });
        }
        Ok(())
    }

    /// Check whether any message contains image parts.
    pub fn has_images(&self) -> bool {
        self.messages.iter().any(|m| m.content.has_images())
    }
}

// ── 图像来源 ─────────────────────────

/// 图像来源。
///
/// Provider 转换层据此构建不同的请求格式：
/// - OpenAI：URL → `image_url.url`，Base64 → `image_url.url = "data:{mime};base64,{data}"`
/// - Anthropic：Base64 → `source.data`，URL → 需先下载再转 Base64
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum ImageSource {
    /// URL 引用（CDN / 对象存储 / 公网图片）。
    #[serde(rename = "url")]
    Url { url: String },
    /// Base64 编码内联数据。
    #[serde(rename = "base64")]
    Base64 { data: String },
}

impl std::fmt::Debug for ImageSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ImageSource::Url { url } => f.debug_struct("Url").field("url", url).finish(),
            ImageSource::Base64 { data } => {
                let preview = if data.len() > 64 {
                    format!("{}…({} bytes)", &data[..64], data.len())
                } else {
                    data.clone()
                };
                f.debug_struct("Base64").field("data", &preview).finish()
            }
        }
    }
}

/// 通用文件/附件来源。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum FileSource {
    /// URL 引用（CDN / 对象存储 / 公网文件）。
    #[serde(rename = "url")]
    Url { url: String },
    /// Base64 编码内联数据（data URI 中剥离的纯 base64）。
    #[serde(rename = "base64")]
    Base64 { data: String },
    /// 本地路径（仅在本地运行时有效，Provider 不应暴露本地路径到远端）。
    #[serde(rename = "local_path")]
    LocalPath { path: String },
    /// Provider 层已上传并返回的引用（如 OpenAI file id / provider-specific reference）。
    #[serde(rename = "provider_ref")]
    ProviderRef { reference: String },
}

// ── 消息内容（多模态） ─────────────────────────

/// 聊天消息内容部分 — 支持多模态富媒体。
///
/// 每条消息的 content 可以由多个 Part 组成，
/// 支持文本、图片、工具结果、思考过程、二进制数据五种类型。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum ChatMessagePart {
    /// 纯文本。
    #[serde(rename = "text")]
    Text { text: String },

    /// 图像。
    #[serde(rename = "image")]
    Image {
        /// 图像来源（URL 或 Base64）。
        source: ImageSource,
        /// MIME 类型（如 `image/png`, `image/jpeg`）。
        mime_type: String,
        /// 图像分辨率控制（OpenAI 专有：`auto` / `low` / `high`）。
        #[serde(skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
    },

    /// 工具调用结果。
    #[serde(rename = "tool_result")]
    ToolResult {
        tool_call_id: String,
        content: Vec<ChatMessagePart>,
        #[serde(default)]
        is_error: bool,
    },

    /// 思考/推理过程。
    #[serde(rename = "thinking")]
    Thinking {
        text: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        thinking_id: Option<String>,
    },

    /// 任意二进制数据。
    #[serde(rename = "data")]
    Data {
        mime_type: String,
        data: String, // base64
    },

    /// 通用文件/文档部件（PDF / DOCX / text / 其他二进制附件）。
    #[serde(rename = "file")]
    File {
        /// 文件来源抽象（URL / Base64 / 本地 / provider ref）。
        source: FileSource,
        /// MIME 类型（如 `application/pdf`, `text/plain`）。
        mime_type: String,
        /// 可选文件名（客户端提供的原始文件名）。
        #[serde(skip_serializing_if = "Option::is_none")]
        filename: Option<String>,
    },
}

/// 消息内容 — 兼容纯文本和富媒体两种模式。
///
/// - `Text(String)` — 向后兼容，简单场景直接用字符串
/// - `Parts(Vec<ChatMessagePart>)` — 多模态场景
///
/// **反序列化顺序**：`#[serde(untagged)]` 会按变体声明顺序尝试——
/// 先尝试 `Text(String)`（匹配 JSON 字符串 `"hello"`），
/// 不匹配再尝试 `Parts`（匹配 JSON 数组 `[{"type":"text",...}]`）。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum ChatMessageContent {
    /// 纯文本内容（向后兼容）。
    Text(String),
    /// 富媒体内容。
    Parts(Vec<ChatMessagePart>),
}

impl ChatMessageContent {
    /// 提取纯文本内容（零拷贝优先）。
    ///
    /// - `Text` 变体：返回 `Cow::Borrowed`，无内存分配。
    /// - `Parts` 变体：拼接所有 Text 部分，返回 `Cow::Owned`。
    pub fn as_text(&self) -> std::borrow::Cow<'_, str> {
        match self {
            ChatMessageContent::Text(s) => std::borrow::Cow::Borrowed(s),
            ChatMessageContent::Parts(parts) => {
                let text: String = parts
                    .iter()
                    .map(|p| match p {
                        ChatMessagePart::Text { text } => text.clone(),
                        ChatMessagePart::File { filename, .. } => match filename {
                            Some(name) => format!("[file:{}]", name),
                            None => "[file]".to_string(),
                        },
                        ChatMessagePart::Image { .. } => "[image]".to_string(),
                        ChatMessagePart::ToolResult { .. } => "[tool_result]".to_string(),
                        ChatMessagePart::Thinking { text, .. } => text.clone(),
                        ChatMessagePart::Data { .. } => "[data]".to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join("");
                std::borrow::Cow::Owned(text)
            }
        }
    }

    /// 判断是否包含图像。
    pub fn has_images(&self) -> bool {
        match self {
            ChatMessageContent::Text(_) => false,
            ChatMessageContent::Parts(parts) => parts
                .iter()
                .any(|p| matches!(p, ChatMessagePart::Image { .. })),
        }
    }
}

impl From<&str> for ChatMessageContent {
    fn from(s: &str) -> Self {
        ChatMessageContent::Text(s.to_string())
    }
}

impl From<String> for ChatMessageContent {
    fn from(s: String) -> Self {
        ChatMessageContent::Text(s)
    }
}

// ── 聊天消息 ─────────────────────────

/// 聊天消息。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChatMessage {
    /// 角色。
    pub role: Role,
    /// 消息内容（纯文本或富媒体）。
    pub content: ChatMessageContent,
    /// 模型发出的工具调用（仅 role=assistant 时存在）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCallInfo>>,
    /// 关联的工具调用 ID（仅 role=tool 时存在）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    /// 消息来源名称（用于多 Agent 区分，如 participant name）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

// ── Provider trait ─────────────────────────

/// Provider trait — 所有 LLM 适配器实现此接口。
///
/// Talon 生态中的统一 LLM 抽象。每个 Provider（OpenAI / Anthropic / Gemini / Ollama）
/// 实现此 trait，上层模块（talon-ai / talon-agent / talon-evo-core）通过此接口调用。
///
/// ## 关键设计决策
///
/// 1. **`CancellationToken` 贯穿全链路**：用户在 UI 点"停止"后，
///    底层 HTTP 请求、流迭代器全部通过 Token 瞬间级联销毁。
/// 2. **`provide_token_count`**：精确的 Token 计数能力，
///    供上下文压缩器做精准预算。
/// 3. **`models()` 返回带 `ModelCapabilities` 的完整元数据**：
///    引擎据此做功能协商（不支持工具的模型自动抹掉 Tool Schema）。
#[async_trait]
pub trait LanguageModelProvider: Send + Sync {
    /// Provider 唯一标识。
    fn id(&self) -> &str;

    /// Provider 显示名称。
    fn display_name(&self) -> &str;

    /// 列出支持的模型（含完整能力声明）。
    fn models(&self) -> Vec<ModelInfo>;

    /// 流式补全 — 返回 StreamEvent 的异步流。
    ///
    /// `cancel` 参数用于全链路取消传播：上层 drop 或取消时，
    /// Provider 应立即终止 HTTP 连接和流解析。
    async fn complete(
        &self,
        request: CompletionRequest,
        cancel: CancellationToken,
    ) -> Result<std::pin::Pin<Box<dyn futures::Stream<Item = StreamEvent> + Send>>, crate::LlmError>;

    /// Provider-native image generation.
    async fn generate_images(
        &self,
        request: ImageGenerationRequest,
        _cancel: CancellationToken,
    ) -> Result<ImageGenerationResponse, crate::LlmError> {
        Err(crate::LlmError::UnsupportedCapability {
            capability: "image_generation".to_string(),
            detail: format!(
                "Provider {} does not implement image generation for model {}",
                self.id(),
                request.model
            ),
        })
    }

    /// Token 计数。
    ///
    /// 用于上下文压缩器做精准预算裁剪。
    /// 默认实现按 Unicode **字符数**（非字节数）估算：
    /// - 英文约 4 字符 ≈ 1 token
    /// - 中文约 1.5 字符 ≈ 1 token
    /// 综合取 3 字符/token 作为安全偏高估算。
    /// 各 Provider 应覆盖为基于 tiktoken / tokenizer 的精确实现。
    async fn provide_token_count(
        &self,
        _model_id: &str,
        text: &str,
    ) -> Result<u32, crate::LlmError> {
        // 按 Unicode 字符数估算（非 byte），对中英文混合更准确
        let char_count = text.chars().count() as u32;
        Ok(char_count / 3 + 1) // +1 避免空字符串返回 0
    }
}
