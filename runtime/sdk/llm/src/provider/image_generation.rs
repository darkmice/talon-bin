/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 */
//! First-class image generation request / response contracts.

use serde_json::Value;

/// Stable request contract for provider-side image generation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageGenerationRequest {
    /// Target model ID.
    pub model: String,
    /// Main prompt.
    pub prompt: String,
    /// Optional image count. Standardized to 1-10.
    #[serde(default = "default_image_count")]
    pub count: u32,
    /// Optional output size hint such as `1024x1024`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<String>,
    /// Optional aspect ratio hint such as `1:1`, `4:3`, `16:9`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aspect_ratio: Option<String>,
    /// Optional quality hint such as `standard`, `hd`, `draft`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quality: Option<String>,
    /// Optional negative prompt for providers that support it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub negative_prompt: Option<String>,
    /// Optional provider-specific metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

fn default_image_count() -> u32 {
    1
}

impl ImageGenerationRequest {
    pub fn new(model: &str, prompt: &str) -> Self {
        Self {
            model: model.to_string(),
            prompt: prompt.to_string(),
            count: default_image_count(),
            size: None,
            aspect_ratio: None,
            quality: None,
            negative_prompt: None,
            metadata: None,
        }
    }

    /// Validate user-facing invariants before dispatching to a provider.
    pub fn validate(&self) -> Result<(), crate::LlmError> {
        if self.model.trim().is_empty() {
            return Err(crate::LlmError::ProviderError(
                "image generation request is missing model".to_string(),
            ));
        }
        if self.prompt.trim().is_empty() {
            return Err(crate::LlmError::ProviderError(
                "image generation request is missing prompt".to_string(),
            ));
        }
        if !(1..=10).contains(&self.count) {
            return Err(crate::LlmError::ProviderError(
                "image generation count must be between 1 and 10".to_string(),
            ));
        }
        Ok(())
    }

    /// Resolve an OpenAI-compatible size hint from explicit size or aspect ratio.
    pub fn resolved_size_hint(&self) -> Option<String> {
        if let Some(size) = self
            .size
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            return Some(size.to_string());
        }

        match self
            .aspect_ratio
            .as_deref()
            .map(str::trim)
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("1:1") | Some("square") => Some("1024x1024".to_string()),
            Some("4:3") | Some("16:9") | Some("landscape") => Some("1536x1024".to_string()),
            Some("3:4") | Some("9:16") | Some("portrait") => Some("1024x1536".to_string()),
            _ => None,
        }
    }
}

/// A single normalized generated image payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedImage {
    /// Raw image bytes, already normalized from either `b64_json` or downloaded URL.
    pub bytes: Vec<u8>,
    /// Best-effort detected MIME type.
    pub mime_type: String,
    /// Optional provider-revised prompt.
    pub revised_prompt: Option<String>,
    /// Optional provider-origin URL if the image was returned by URL.
    pub source_url: Option<String>,
}

/// Provider-normalized image generation result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageGenerationResponse {
    pub model: String,
    pub images: Vec<GeneratedImage>,
}
