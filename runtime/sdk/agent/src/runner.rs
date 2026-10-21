use crate::*;
use futures::{stream::FuturesUnordered, StreamExt};
use serde_json::{json, Value};
use std::{future::Future, pin::Pin};
use talon_llm::LanguageModelProvider;
use talon_runtime_transport::{error, Operation};

pub struct AgentRunner {
    store: talon::Store,
    registry: skill::SkillRegistry,
}
impl AgentRunner {
    pub fn new(store: &talon::Store) -> Result<Self, talon_runtime_transport::Error> {
        Ok(Self {
            store: store.clone(),
            registry: skill::SkillRegistry::new(),
        })
    }
    pub fn set_skill_registry(&mut self, registry: skill::SkillRegistry) {
        self.registry = registry;
    }
    pub fn set_tracer_provider(&mut self, _provider: Box<agent::trace::NoopTracerProvider>) {}
    pub async fn run(
        &mut self,
        request: AgentRunRequest,
        provider: &dyn LanguageModelProvider,
        executor: &dyn tool::ToolExecutor,
        events: tokio::sync::mpsc::UnboundedSender<AgentEvent>,
    ) -> ExecutionReport {
        let result = self.run_inner(request, provider, executor, &events).await;
        result.unwrap_or_else(|_| {
            let _ = events.send(AgentEvent::Error {
                message: "Native Agent bridge failed".into(),
                retryable: false,
            });
            ExecutionReport {
                iterations: 0,
                tool_calls: 0,
                final_output: None,
                finish_reason: "error".into(),
            }
        })
    }
    async fn run_inner(
        &self,
        request: AgentRunRequest,
        provider: &dyn LanguageModelProvider,
        executor: &dyn tool::ToolExecutor,
        events: &tokio::sync::mpsc::UnboundedSender<AgentEvent>,
    ) -> Result<ExecutionReport, talon_runtime_transport::Error> {
        let cancel = request.cancellation_token.clone().unwrap_or_default();
        let hitl = request.hitl.clone();
        let operation = Arc::new(Operation::start(
            json!({"operation":"agent","db":self.store.native_handle(),"request":request,"skills":self.registry.enabled_skills(),"providerId":provider.id(),"providerName":provider.display_name(),"models":provider.models()}),
        )?);
        let _hitl_binding = hitl
            .as_ref()
            .map(|channel| channel.bind(operation.clone()))
            .transpose()?;
        let callbacks_cancel = CancellationToken::new();
        struct CancelOnDrop(CancellationToken);
        impl Drop for CancelOnDrop {
            fn drop(&mut self) {
                self.0.cancel();
            }
        }
        let _cancel_callbacks = CancelOnDrop(callbacks_cancel.clone());
        type Callback<'a> = Pin<
            Box<dyn Future<Output = (u64, Result<(), talon_runtime_transport::Error>)> + Send + 'a>,
        >;
        fn queued<'a>(
            id: u64,
            cancel: CancellationToken,
            future: impl Future<Output = Result<(), talon_runtime_transport::Error>> + Send + 'a,
        ) -> Callback<'a> {
            Box::pin(async move {
                let result =
                    tokio::select! {biased;_=cancel.cancelled()=>Ok(()),result=future=>result};
                (id, result)
            })
        }
        let mut callbacks: FuturesUnordered<Callback<'_>> = FuturesUnordered::new();
        let mut cancelling = false;
        let mut callback_tokens = std::collections::HashMap::<u64, CancellationToken>::new();
        loop {
            let event = tokio::select! {
             biased;
             _=cancel.cancelled(),if !cancelling=>{cancelling=true;callbacks_cancel.cancel();callbacks.clear();operation.cancel();continue},
             result=callbacks.next(),if !callbacks.is_empty()=>{let (id,result)=result.ok_or_else(||error("Callback queue closed"))?;callback_tokens.remove(&id);if let Err(error)=result{if error.code!="callback_closed"{return Err(error);}}continue},
             event=operation.next()=>event?,
            };
            match event["kind"].as_str() {
                Some("callback_cancel") => {
                    let id = event["callbackId"]
                        .as_u64()
                        .ok_or_else(|| error("Missing callback cancellation ID"))?;
                    if let Some(cancel) = callback_tokens.get(&id) {
                        cancel.cancel();
                    }
                }
                Some("agent_event") => {
                    let event: AgentEvent = serde_json::from_value(event["event"].clone())?;
                    if matches!(&event, AgentEvent::HitlInjected { .. }) {
                        if let Some(hitl) = &hitl {
                            hitl.applied();
                        }
                    }
                    let _ = events.send(event);
                }
                Some("callback") => {
                    if callbacks.len() >= 64 {
                        return Err(error("Native callback capacity exceeded"));
                    }
                    let id = event["callbackId"]
                        .as_u64()
                        .ok_or_else(|| error("Missing callback correlation"))?;
                    let operation = operation.clone();
                    let callback_cancel = callbacks_cancel.child_token();
                    callback_tokens.insert(id, callback_cancel.clone());
                    match event["method"].as_str() {
                        Some("approval") => {
                            let (tx, rx) = tokio::sync::oneshot::channel();
                            let get =
                                |name: &str| event[name].as_str().unwrap_or_default().to_string();
                            let approval = AgentEvent::ApprovalRequired {
                                tool_call_id: get("toolCallId"),
                                tool_name: get("toolName"),
                                explanation: get("explanation"),
                                goal: get("goal"),
                                is_background: event["isBackground"].as_bool().unwrap_or(false),
                                reply_tx: Arc::new(tokio::sync::Mutex::new(Some(tx))),
                            };
                            let _ = events.send(approval);
                            callbacks.push(queued(id,callback_cancel.clone(),async move{let approved=tokio::select!{biased;_=callback_cancel.cancelled()=>return Ok(()),approved=rx=>approved.unwrap_or(false)};operation.reply_async(id,json!({"approved":approved}),true).await}));
                        }
                        Some("complete") => {
                            let request = serde_json::from_value(event["request"].clone())?;
                            callbacks.push(queued(id,callback_cancel.clone(),async move{
       let result=tokio::select!{biased;_=callback_cancel.cancelled()=>return Ok(()),result=provider.complete(request,callback_cancel.child_token())=>result};
       match result{
        Ok(mut stream)=>{operation.reply_async(id,json!({"kind":"ready"}),false).await?;loop{
         let next=tokio::select!{biased;_=callback_cancel.cancelled()=>return Ok(()),item=stream.next()=>item};
         match next{Some(item)=>operation.reply_async(id,json!({"kind":"event","event":item}),false).await?,None=>break}
        }operation.reply_async(id,Value::Null,true).await?;},
        Err(error)=>{let value=match error{talon_llm::LlmError::ApiError{status,message}=>json!({"kind":"error","code":"api","status":status,"message":message}),talon_llm::LlmError::RateLimited{retry_after_ms}=>json!({"kind":"error","code":"rate_limited","retryAfterMs":retry_after_ms}),talon_llm::LlmError::Cancelled=>json!({"kind":"error","code":"cancelled"}),error=>json!({"kind":"error","code":"provider","message":error.to_string()})};operation.reply_async(id,value,true).await?;}
       }Ok(())
      }));
                        }
                        Some("count_tokens") => {
                            let model = event["model"]
                                .as_str()
                                .ok_or_else(|| error("Missing token model"))?
                                .to_string();
                            let text = event["text"]
                                .as_str()
                                .ok_or_else(|| error("Missing token text"))?
                                .to_string();
                            callbacks.push(queued(id,callback_cancel.clone(),async move{let value=tokio::select!{biased;_=callback_cancel.cancelled()=>return Ok(()),value=provider.provide_token_count(&model,&text)=>value};match value{Ok(count)=>operation.reply_async(id,json!({"count":count}),true).await,Err(_)=>operation.reply_async(id,json!({"error":"Token count failed"}),true).await}}));
                        }
                        Some("tool") => {
                            let name = event["name"]
                                .as_str()
                                .ok_or_else(|| error("Missing tool name"))?
                                .to_string();
                            let args = event["args"].clone();
                            callbacks.push(queued(id,callback_cancel.clone(),async move{let value=tokio::select!{biased;_=callback_cancel.cancelled()=>return Ok(()),value=executor.execute(&name,&args)=>value};operation.reply_async(id,serde_json::to_value(value)?,true).await}));
                        }
                        _ => return Err(error("Unsupported native callback")),
                    }
                }
                Some("done") => {
                    return serde_json::from_value(event["result"].clone()).map_err(Into::into)
                }
                Some("error") => return Err(error("Native Agent execution failed")),
                _ => return Err(error("Unknown native Agent envelope")),
            }
        }
    }
}
