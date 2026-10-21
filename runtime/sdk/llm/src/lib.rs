//! Public model contracts and native-provider adapter; no HTTP/model engine implementation.
pub mod config;
pub mod provider;
pub mod stream {
    mod event;
    pub use event::*;
}
pub use provider::r#trait::*;
pub use stream::StreamEvent;

#[derive(Debug, thiserror::Error)]
pub enum LlmError {
    #[error("Provider error: {0}")]
    ProviderError(String),
    #[error("Model not found: {0}")]
    ModelNotFound(String),
    #[error("API error: HTTP {status}: {message}")]
    ApiError { status: u16, message: String },
    #[error("Stream error: {0}")]
    StreamError(String),
    #[error("Rate limited, retry after {retry_after_ms}ms")]
    RateLimited { retry_after_ms: u64 },
    #[error("All retries exhausted after {attempts} attempts")]
    RetriesExhausted { attempts: u32 },
    #[error("Model does not support {capability}: {detail}")]
    UnsupportedCapability { capability: String, detail: String },
    #[error("Request cancelled")]
    Cancelled,
}
impl From<talon_runtime_transport::Error> for LlmError {
    fn from(error: talon_runtime_transport::Error) -> Self {
        Self::ProviderError(error.to_string())
    }
}
