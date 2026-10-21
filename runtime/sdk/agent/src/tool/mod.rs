pub mod builtin;
pub mod definition;
pub mod image;
pub use definition::*;
pub use image::*;
use std::{future::Future, pin::Pin};
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolSource {
    Builtin,
    Skill,
    Mcp,
    Dynamic,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ToolCapabilityOverrides {
    pub read_only: Option<bool>,
    pub concurrency_safe: Option<bool>,
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
    pub category: ToolCategory,
    pub capability_overrides: ToolCapabilityOverrides,
    pub source: ToolSource,
}
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ToolResult {
    pub content: String,
    pub is_error: bool,
    pub sandbox_level: Option<String>,
    pub structured_result: Option<TypedToolResult>,
}
impl ToolResult {
    pub fn text(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            ..Default::default()
        }
    }
    pub fn error(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            is_error: true,
            ..Default::default()
        }
    }
}
pub trait ToolExecutor: Send + Sync {
    fn execute(
        &self,
        name: &str,
        args: &serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = ToolResult> + Send + '_>>;
}
