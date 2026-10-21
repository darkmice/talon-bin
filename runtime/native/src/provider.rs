use super::*;
use futures::{Stream, StreamExt};
use std::pin::Pin;
use talon_llm::{CompletionRequest, LanguageModelProvider, LlmError, ModelInfo, StreamEvent};

pub(super) fn llm_error(error: LlmError) -> Value {
    match error {
        LlmError::ApiError { status, message } => {
            json!({"kind":"error","code":"api","status":status,"message":message})
        }
        LlmError::RateLimited { retry_after_ms } => {
            json!({"kind":"error","code":"rate_limited","retryAfterMs":retry_after_ms})
        }
        LlmError::Cancelled => {
            json!({"kind":"error","code":"cancelled","message":"Provider cancelled"})
        }
        other => json!({"kind":"error","code":"provider","message":other.to_string()}),
    }
}
fn from_host(value: Value) -> LlmError {
    match value["code"].as_str() {
        Some("api") => LlmError::ApiError {
            status: value["status"].as_u64().unwrap_or(500) as u16,
            message: value["message"]
                .as_str()
                .unwrap_or("Host provider failed")
                .into(),
        },
        Some("rate_limited") => LlmError::RateLimited {
            retry_after_ms: value["retryAfterMs"].as_u64().unwrap_or(0),
        },
        Some("cancelled") => LlmError::Cancelled,
        _ => LlmError::ProviderError(
            value["message"]
                .as_str()
                .unwrap_or("Host provider failed")
                .into(),
        ),
    }
}

pub(super) fn prepare(request: &Value) -> Result<Job> {
    let config: talon_llm::config::ProviderConfig =
        serde_json::from_value(request["config"].clone())?;
    let models: Vec<ModelInfo> = serde_json::from_value(request["models"].clone())?;
    let request: CompletionRequest = serde_json::from_value(request["request"].clone())?;
    Ok(Box::new(move |op| {
        Box::pin(async move {
            use talon_llm::provider::openai_compat::{
                OpenAICompatibleProvider, TransportObservation,
            };
            // Observations share the bounded event queue; the callback is synchronous.
            // Use a bounded channel and a separate forwarder rather than blocking a runtime worker.
            let (tx, mut observations) = tokio::sync::mpsc::channel(128);
            let observer_op = op.clone();
            let observation_overflow = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let overflow = observation_overflow.clone();
            let extra_bytes = Arc::new(AtomicUsize::new(0));
            let extra = extra_bytes.clone();
            let provider = OpenAICompatibleProvider::new(config, models).with_transport_observer(
                Arc::new(move |event| {
                    let value = match event {
                        TransportObservation::RequestStart => json!({"kind":"request_start"}),
                        TransportObservation::ResponseHeaders { event_stream } => {
                            json!({"kind":"headers","eventStream":event_stream})
                        }
                        TransportObservation::BodyChunk { bytes } => {
                            json!({"kind":"chunk","bytes":bytes})
                        }
                    };
                    if let Err(error) = tx.try_send(value) {
                        let value = error.into_inner();
                        let bytes = value["bytes"]
                            .as_u64()
                            .and_then(|n| usize::try_from(n).ok());
                        if let Some(bytes) = bytes {
                            if extra
                                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                                    n.checked_add(bytes)
                                })
                                .is_err()
                            {
                                overflow.store(true, Ordering::Release);
                                observer_op.cancel.cancel();
                            }
                        } else {
                            overflow.store(true, Ordering::Release);
                            observer_op.cancel.cancel();
                        }
                    }
                }),
            );
            let complete = provider.complete(request, op.cancel.child_token());
            tokio::pin!(complete);
            let result = loop {
                tokio::select! {biased;observation=observations.recv()=>{if let Some(value)=observation{op.emit(json!({"kind":"transport","observation":value})).await?;}else{return Err(failure("Transport observer closed before provider handshake"));}},result=&mut complete=>break result}
            };
            flush(&op, &mut observations, &extra_bytes).await?;
            match result {
                Ok(mut stream) => {
                    op.emit(json!({"kind":"ready"})).await?;
                    loop {
                        let event = tokio::select! {biased;_=op.cancel.cancelled()=>break,observation=observations.recv()=>{if let Some(value)=observation{op.emit(json!({"kind":"transport","observation":value})).await?;}continue},event=stream.next()=>event};
                        flush(&op, &mut observations, &extra_bytes).await?;
                        if let Some(event) = event {
                            op.emit(json!({"kind":"event","event":event})).await?;
                        } else {
                            break;
                        }
                    }
                }
                Err(error) => {
                    op.emit(llm_error(error)).await?;
                }
            }
            if observation_overflow.load(Ordering::Acquire) {
                return Err(failure("Transport observation capacity exceeded"));
            }
            Ok(Value::Null)
        })
    }))
}
async fn flush(
    op: &Operation,
    observations: &mut tokio::sync::mpsc::Receiver<Value>,
    extra: &AtomicUsize,
) -> Result<()> {
    while let Ok(value) = observations.try_recv() {
        op.emit(json!({"kind":"transport","observation":value}))
            .await?;
    }
    let bytes = extra.swap(0, Ordering::AcqRel);
    if bytes > 0 {
        op.emit(json!({"kind":"transport","observation":{"kind":"chunk","bytes":bytes}}))
            .await?;
    }
    Ok(())
}

pub(super) struct HostProvider {
    pub op: Arc<Operation>,
    pub id: String,
    pub name: String,
    pub models: Vec<ModelInfo>,
}
#[async_trait::async_trait]
impl LanguageModelProvider for HostProvider {
    fn id(&self) -> &str {
        &self.id
    }
    fn display_name(&self) -> &str {
        &self.name
    }
    fn models(&self) -> Vec<ModelInfo> {
        self.models.clone()
    }
    async fn provide_token_count(
        &self,
        model: &str,
        text: &str,
    ) -> std::result::Result<u32, LlmError> {
        let (_, mut rx) = self
            .op
            .callback(json!({"method":"count_tokens","model":model,"text":text}))
            .await
            .map_err(|_| LlmError::ProviderError("Host tokenizer callback failed".into()))?;
        let value = self
            .op
            .receive(&mut rx)
            .await
            .map_err(|_| LlmError::Cancelled)?;
        if !value.done {
            return Err(LlmError::ProviderError(
                "Nonterminal token count reply".into(),
            ));
        }
        value.value["count"]
            .as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .ok_or_else(|| LlmError::ProviderError("Invalid host token count".into()))
    }
    async fn complete(
        &self,
        request: CompletionRequest,
        cancel: CancellationToken,
    ) -> std::result::Result<Pin<Box<dyn Stream<Item = StreamEvent> + Send>>, LlmError> {
        let (id, mut rx) = self
            .op
            .callback(json!({"method":"complete","request":request}))
            .await
            .map_err(|_| LlmError::ProviderError("Host model callback failed".into()))?;
        let first = tokio::select! {biased;_=cancel.cancelled()=>return Err(LlmError::Cancelled),first=self.op.receive(&mut rx)=>first.map_err(|_|LlmError::Cancelled)?};
        if first.value["kind"] == "error" {
            return Err(from_host(first.value));
        }
        if first.value["kind"] != "ready" || first.done {
            return Err(LlmError::ProviderError(
                "Invalid host model handshake".into(),
            ));
        }
        let op = self.op.clone();
        Ok(Box::pin(futures::stream::unfold(
            (rx, op, cancel, id, false),
            |(mut rx, op, cancel, id, ended)| async move {
                if ended {
                    return None;
                }
                let reply = tokio::select! {biased;_=cancel.cancelled()=>return None,reply=op.receive(&mut rx)=>reply};
                match reply {
                    Ok(reply) if reply.done => None,
                    Ok(reply) => match serde_json::from_value(reply.value["event"].clone()) {
                        Ok(event) => Some((event, (rx, op, cancel, id, false))),
                        Err(_) => Some((
                            StreamEvent::StreamError {
                                error: "Invalid host model event".into(),
                                retryable: false,
                            },
                            (rx, op, cancel, id, true),
                        )),
                    },
                    Err(_) => Some((
                        StreamEvent::StreamError {
                            error: "Host model callback cancelled".into(),
                            retryable: false,
                        },
                        (rx, op, cancel, id, true),
                    )),
                }
            },
        )))
    }
}
