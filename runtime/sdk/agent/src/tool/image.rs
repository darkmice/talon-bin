/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 */
//! 图像生成工具契约 — talon-agent 稳定 API。
//!
//! 宿主实现 [`ImageGenerationHandler`] 并注册到 [`crate::agent::AgentRunner`]，
//! 即可在 Agent 循环中透明调用 `image_generate` 内建工具。
//!
//! ## 使用示例
//!
//! ```rust,ignore
//! use talon_agent::tool::{ImageGenerateRequest, ImageGenerationError, ImageGenerationHandler, TypedToolResult};
//!
//! struct MyHandler;
//!
//! #[async_trait::async_trait]
//! impl ImageGenerationHandler for MyHandler {
//!     async fn generate_image(
//!         &self,
//!         request: ImageGenerateRequest,
//!     ) -> Result<TypedToolResult, ImageGenerationError> {
//!         // 调用宿主侧 provider ...
//!         Ok(TypedToolResult::Text { text: "done".into() })
//!     }
//! }
//! ```

// ─── TypedToolResult ────────────────────────────────────────────────────────

/// 工具的富类型返回值，涵盖文本、媒体、混合和 JSON 四种形态。
///
/// Agent 循环将此结构投影为 LLM 可见的工具调用结果文本，
/// 同时将媒体项持久化供 UI 渲染。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum TypedToolResult {
    /// 纯文本输出。
    Text { text: String },
    /// 纯媒体输出（如批量生成的图片）。
    Media { items: Vec<MediaItem> },
    /// 文本与媒体混合块（按输出顺序排列）。
    Mixed { blocks: Vec<ToolOutputBlock> },
    /// 结构化 JSON 输出。
    Json { value: serde_json::Value },
}

// ─── MediaItem ──────────────────────────────────────────────────────────────

/// 工具产生的单个媒体资产（图片、视频等）。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MediaItem {
    /// 媒体资产的稳定 ID（通常是 UUID）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_id: Option<String>,
    /// MIME 类型，例如 `"image/png"`。
    pub mime_type: String,
    /// 远端 URL（CDN / provider 返回的原始 URL）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// 本地文件系统路径（宿主持久化后填写）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_path: Option<String>,
    /// 内存内的图片数据，base64 编码（不含 `data:` 前缀）。
    ///
    /// 默认生图路径（[`crate::tool::DefaultImageGeneration`]）在**不落盘**时把生成的
    /// 字节放在这里——零实现 host 直接拿到可渲染的图（前端可拼 `data:<mime>;base64,...`），
    /// 无需自己接 provider 或落盘。host 若选择落盘，则填 `local_path`、此字段留空，
    /// 避免大体积 base64 重复进事件流。两者至少有其一非空。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base64_data: Option<String>,
    /// 图片宽度（像素）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub width: Option<u32>,
    /// 图片高度（像素）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,
    /// 宿主附加的元数据（provider、model、title 等）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

// ─── ToolOutputBlock ────────────────────────────────────────────────────────

/// [`TypedToolResult::Mixed`] 的单个内容块。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ToolOutputBlock {
    /// 纯文本块。
    Text { text: String },
    /// 媒体块（一个或多个媒体项）。
    Media { items: Vec<MediaItem> },
}

// ─── ImageGenerateRequest ───────────────────────────────────────────────────

/// `count` 字段缺省值：与 JSON schema 声明的 default 一致（生成 1 张）。
fn default_image_count() -> u32 {
    1
}

/// `image_generate` 工具的调用参数（来自 LLM 工具调用，由 Agent 框架解析后传入）。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ImageGenerateRequest {
    /// 图像生成主提示词（必填，不能为空）。
    pub prompt: String,
    /// 可选显示标题，作为本地保存的文件名前缀。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// 可选模型 ID 或 provider 别名（如 `"gpt-image-1"` 或 `"openai/gpt-image-1"`）。
    /// 为 `None` 时由宿主解析默认图像模型。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// 可选宽高比提示（如 `"16:9"`、`"1:1"`）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aspect_ratio: Option<String>,
    /// 可选尺寸提示（如 `"1024x1024"`）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<String>,
    /// 可选质量提示（如 `"standard"`、`"hd"`）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quality: Option<String>,
    /// 生成图片数量，有效范围 1–10。
    ///
    /// JSON schema 中 `count` 非必填、默认 1；LLM 省略时 serde 必须能补默认值，
    /// 否则会以「missing field count」反序列化失败（图片永远生成不了）。
    #[serde(default = "default_image_count")]
    pub count: u32,
    /// 可选负向提示词。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub negative_prompt: Option<String>,
    /// 参考图片路径或 URL 列表（用于图生图或合成）。
    #[serde(default)]
    pub reference_images: Vec<String>,
    /// 调用方附加的元数据（透传给 provider 元数据字段）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

// ─── ImageGenerationError ───────────────────────────────────────────────────

/// 图像生成操作的错误类型。
#[derive(Debug, thiserror::Error)]
pub enum ImageGenerationError {
    /// 没有可用的 provider / 模型能处理本次请求。
    #[error("image generation unavailable: {0}")]
    Unavailable(String),
    /// 请求参数无效（prompt 为空、count 超范围等）。
    #[error("invalid image generation request: {0}")]
    InvalidRequest(String),
    /// 生成已发起但失败（provider 错误、写入失败等）。
    #[error("image generation failed: {0}")]
    Failed(String),
}

// ─── ImageGenerationHandler ─────────────────────────────────────────────────

/// 宿主侧图像生成处理器接口。
///
/// 宿主实现此 trait 并通过 [`crate::agent::AgentRunner::set_image_generation_handler`]
/// 注册后，Agent 框架会在收到 `image_generate` 工具调用时自动委托到宿主实现。
///
/// 实现必须是 `Send + Sync`（通过 `Arc<dyn ImageGenerationHandler>` 持有）。
#[async_trait::async_trait]
pub trait ImageGenerationHandler: Send + Sync {
    /// 执行一次图像生成请求，返回富类型结果或错误。
    async fn generate_image(
        &self,
        request: ImageGenerateRequest,
    ) -> Result<TypedToolResult, ImageGenerationError>;
}
