//! Thin adapter to the shipped native provider. Credentials are never logged.
use crate::{config::ProviderConfig, provider::r#trait::*, LlmError, StreamEvent};
use futures::Stream;
use serde_json::{json, Value};
use std::{pin::Pin, sync::Arc};
use talon_runtime_transport::{call_json, Operation};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub enum TransportObservation {
    RequestStart,
    ResponseHeaders { event_stream: bool },
    BodyChunk { bytes: usize },
}
pub type TransportObserver = Arc<dyn Fn(TransportObservation) + Send + Sync>;
pub struct OpenAICompatibleProvider {
    config: ProviderConfig,
    models: Vec<ModelInfo>,
    observer: Option<TransportObserver>,
}
impl OpenAICompatibleProvider {
    pub fn new(config: ProviderConfig, models: Vec<ModelInfo>) -> Self {
        Self {
            config,
            models,
            observer: None,
        }
    }
    pub fn with_transport_observer(mut self, observer: TransportObserver) -> Self {
        self.observer = Some(observer);
        self
    }
    pub fn count_tokens(model: &str, text: &str) -> Result<u32, LlmError> {
        let result = call_json(json!({"action":"tokenize","model":model,"text":text}))?;
        result
            .as_u64()
            .and_then(|v| u32::try_from(v).ok())
            .ok_or_else(|| LlmError::ProviderError("Invalid native token count".into()))
    }
    pub fn warm_tokenizer(model: &str) -> Result<(), LlmError> {
        Self::count_tokens(model, "").map(|_| ())
    }
}
fn observe(observer: &Option<TransportObserver>, value: &Value) {
    let Some(observer) = observer else { return };
    let value = &value["observation"];
    match value["kind"].as_str() {
        Some("request_start") => observer(TransportObservation::RequestStart),
        Some("headers") => observer(TransportObservation::ResponseHeaders {
            event_stream: value["eventStream"].as_bool().unwrap_or(false),
        }),
        Some("chunk") => {
            if let Some(bytes) = value["bytes"]
                .as_u64()
                .and_then(|n| usize::try_from(n).ok())
            {
                observer(TransportObservation::BodyChunk { bytes })
            }
        }
        _ => {}
    }
}
fn native_error(value: &Value) -> LlmError {
    let message = value["message"]
        .as_str()
        .unwrap_or("Native provider failed")
        .to_string();
    match value["code"].as_str() {
        Some("api") => LlmError::ApiError {
            status: value["status"].as_u64().unwrap_or(500) as u16,
            message,
        },
        Some("rate_limited") => LlmError::RateLimited {
            retry_after_ms: value["retryAfterMs"].as_u64().unwrap_or(0),
        },
        Some("cancelled") => LlmError::Cancelled,
        _ => LlmError::ProviderError(message),
    }
}

#[async_trait::async_trait]
impl LanguageModelProvider for OpenAICompatibleProvider {
    fn id(&self) -> &str {
        &self.config.id
    }
    fn display_name(&self) -> &str {
        &self.config.display_name
    }
    fn models(&self) -> Vec<ModelInfo> {
        self.models.clone()
    }
    async fn provide_token_count(&self, model: &str, text: &str) -> Result<u32, LlmError> {
        Self::count_tokens(model, text)
    }
    async fn complete(
        &self,
        request: CompletionRequest,
        cancel: CancellationToken,
    ) -> Result<Pin<Box<dyn Stream<Item = StreamEvent> + Send>>, LlmError> {
        let mut config = serde_json::to_value(&self.config)
            .map_err(|_| LlmError::ProviderError("Invalid provider configuration".into()))?;
        config["api_key"] = json!(self.config.api_key);
        let operation = Operation::start(
            json!({"operation":"provider","config":config,"models":self.models,"request":request}),
        )?;
        // Preserve the provider's pre-stream error boundary and HTTP error type.
        loop {
            let event = tokio::select! {biased;_=cancel.cancelled()=>{operation.cancel();return Err(LlmError::Cancelled)},event=operation.next()=>event?};
            match event["kind"].as_str() {
                Some("ready") => break,
                Some("transport") => observe(&self.observer, &event),
                Some("error") => return Err(native_error(&event)),
                _ => {
                    return Err(LlmError::ProviderError(
                        "Invalid native provider handshake".into(),
                    ))
                }
            }
        }
        let observer = self.observer.clone();
        let stream = futures::stream::unfold(
            (operation, cancel, observer, false),
            |(operation, cancel, observer, ended)| async move {
                if ended {
                    return None;
                }
                loop {
                    let event = tokio::select! {biased;_=cancel.cancelled()=>{operation.cancel();return None},event=operation.next()=>event};
                    match event {
                        Ok(event) => match event["kind"].as_str() {
                            Some("transport") => observe(&observer, &event),
                            Some("event") => match serde_json::from_value(event["event"].clone()) {
                                Ok(item) => {
                                    return Some((item, (operation, cancel, observer, false)))
                                }
                                Err(_) => {
                                    return Some((
                                        StreamEvent::StreamError {
                                            error: "Invalid native stream event".into(),
                                            retryable: false,
                                        },
                                        (operation, cancel, observer, true),
                                    ))
                                }
                            },
                            Some("done") => return None,
                            Some("error") => {
                                return Some((
                                    StreamEvent::StreamError {
                                        error: native_error(&event).to_string(),
                                        retryable: false,
                                    },
                                    (operation, cancel, observer, true),
                                ))
                            }
                            _ => {
                                return Some((
                                    StreamEvent::StreamError {
                                        error: "Unexpected native provider event".into(),
                                        retryable: false,
                                    },
                                    (operation, cancel, observer, true),
                                ))
                            }
                        },
                        Err(error) => {
                            return Some((
                                StreamEvent::StreamError {
                                    error: error.to_string(),
                                    retryable: false,
                                },
                                (operation, cancel, observer, true),
                            ))
                        }
                    }
                }
            },
        );
        Ok(Box::pin(stream))
    }
}
