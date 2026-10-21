use super::*;
use crate::provider::HostProvider;
use sdk_agent::{AgentEvent as WireEvent, ExecutionReport as WireReport};
use talon_agent::{
    tool::{ToolExecutor, ToolOutputBlock, ToolResult, TypedToolResult},
    AgentRunRequest, AgentRunner,
};

fn limits(limits: sdk_agent::ExecutionLimits) -> talon_agent::ExecutionLimits {
    talon_agent::ExecutionLimits {
        max_iterations: limits.max_iterations,
        max_tool_calls: limits.max_tool_calls,
        max_duration: limits.max_duration,
        max_parallel_tool_calls: limits.max_parallel_tool_calls,
    }
}
fn report(report: &talon_agent::ExecutionReport) -> WireReport {
    WireReport {
        iterations: report.iterations,
        tool_calls: report.tool_calls,
        final_output: report.final_output.clone(),
        finish_reason: report.finish_reason.clone(),
    }
}
pub(super) fn inject(op: &Operation, command: sdk_agent::HitlCommand) -> Result<()> {
    op.hitl_pending
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
            (n < 128).then_some(n + 1)
        })
        .map_err(|_| failure("HITL queue capacity exceeded"))?;
    let command = match command {
        sdk_agent::HitlCommand::Inject { message } => talon_agent::HitlCommand::Inject { message },
        sdk_agent::HitlCommand::Pause => talon_agent::HitlCommand::Pause,
        sdk_agent::HitlCommand::Resume => talon_agent::HitlCommand::Resume,
        sdk_agent::HitlCommand::Abort { reason } => talon_agent::HitlCommand::Abort { reason },
        sdk_agent::HitlCommand::UpdateLimits { new_limits } => {
            talon_agent::HitlCommand::UpdateLimits {
                new_limits: limits(new_limits),
            }
        }
    };
    op.hitl.sender().send(command).map_err(|_| {
        op.hitl_pending.fetch_sub(1, Ordering::AcqRel);
        failure("Native HITL channel is closed")
    })
}
fn event(event: talon_agent::AgentEvent) -> WireEvent {
    use talon_agent::AgentEvent::*;
    match event {
        RunStart { session_id, model } => WireEvent::RunStart { session_id, model },
        IterationStart { iteration } => WireEvent::IterationStart { iteration },
        LlmStream(event) => WireEvent::LlmStream(
            serde_json::from_value(
                serde_json::to_value(event).expect("serializable stream contract"),
            )
            .expect("matching SDK stream contract"),
        ),
        ThinkingPart { thought } => WireEvent::ThinkingPart { thought },
        SkillsActivated { skill_ids } => WireEvent::SkillsActivated { skill_ids },
        LimitReached { kind, action } => WireEvent::LimitReached { kind, action },
        HitlInjected { message } => WireEvent::HitlInjected { message },
        Error { message, retryable } => WireEvent::Error { message, retryable },
        ToolUsePart {
            tool_call_id,
            tool_name,
            args,
        } => WireEvent::ToolUsePart {
            tool_call_id,
            tool_name,
            args,
        },
        ToolResult {
            tool_call_id,
            result,
            is_error,
            sandbox_level,
            ..
        } => WireEvent::ToolResult {
            tool_call_id,
            result,
            is_error,
            sandbox_level,
        },
        ProgressUpdate { message } => WireEvent::ProgressUpdate { message },
        RunEnd { report: result } => WireEvent::RunEnd {
            report: report(&result),
        },
        ApprovalRequired { .. } => WireEvent::Observation {
            name: "approval_routed_as_callback".into(),
        },
        _ => WireEvent::Observation {
            name: "native_runtime_observation".into(),
        },
    }
}
async fn project(op: &Arc<Operation>, value: talon_agent::AgentEvent) -> Result<()> {
    if matches!(
        &value,
        talon_agent::AgentEvent::HitlInjected { .. }
            | talon_agent::AgentEvent::HitlPaused
            | talon_agent::AgentEvent::HitlResumed
            | talon_agent::AgentEvent::HitlLimitsUpdated
    ) {
        let _ = op
            .hitl_pending
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                Some(n.saturating_sub(1))
            });
    }
    if let talon_agent::AgentEvent::ApprovalRequired {
        tool_call_id,
        tool_name,
        explanation,
        goal,
        is_background,
        reply_tx,
    } = value
    {
        let sender = reply_tx.lock().await.take();
        let result=async{let (_,mut rx)=op.callback(json!({"method":"approval","toolCallId":tool_call_id,"toolName":tool_name,"explanation":explanation,"goal":goal,"isBackground":is_background})).await?;let reply=op.receive(&mut rx).await?;if !reply.done{return Err(failure("Nonterminal approval reply"));}Ok(reply.value["approved"].as_bool().unwrap_or(false))}.await;
        if let Some(sender) = sender {
            let _ = sender.send(result.unwrap_or(false));
        }
        Ok(())
    } else {
        op.emit(json!({"kind":"agent_event","event":event(value)}))
            .await
    }
}
pub(super) fn prepare(request: &Value) -> Result<Job> {
    let database = database(request)?;
    let input: sdk_agent::AgentRunRequest = serde_json::from_value(request["request"].clone())?;
    let skills: Vec<talon_agent::skill::SkillDefinition> =
        serde_json::from_value(request["skills"].clone())?;
    let models: Vec<talon_llm::ModelInfo> = serde_json::from_value(request["models"].clone())?;
    let id = text(request, "providerId")?.to_string();
    let name = text(request, "providerName")?.to_string();
    if input.input.len() > 1024 * 1024 || input.tools.len() > 512 || skills.len() > 512 {
        return Err(failure("Agent request exceeds profile limits"));
    }
    let tools = serde_json::from_value(serde_json::to_value(&input.tools)?)?;
    Ok(Box::new(move |op| {
        Box::pin(async move {
            let registry = talon_agent::skill::SkillRegistry::new();
            let names = input
                .tools
                .iter()
                .map(|tool| tool.name.clone())
                .collect::<Vec<_>>();
            for skill in skills {
                registry
                    .register_validated(skill, Some(&names))
                    .map_err(|_| failure("Invalid native skill binding"))?;
            }
            let selection = match input.skills {
                sdk_agent::SkillSelection::Auto => talon_agent::SkillSelection::Auto,
                sdk_agent::SkillSelection::Manual(ids) => talon_agent::SkillSelection::Manual(ids),
                sdk_agent::SkillSelection::None => talon_agent::SkillSelection::None,
            };
            let security = serde_json::from_value(serde_json::to_value(input.security)?)?;
            let mut request = AgentRunRequest::builder(&input.model, &input.input)
                .session_id(&input.session_id)
                .tools(tools)
                .skills(selection)
                .limits(limits(input.limits))
                .security(security)
                .cancellation_token(op.cancel.child_token())
                .hitl(op.hitl.clone());
            if let Some(prompt) = input.system_prompt {
                request = request.system_prompt(prompt);
            }
            if let Some(parts) = input.input_parts {
                request =
                    request.input_parts(serde_json::from_value(serde_json::to_value(parts)?)?);
            }
            if let Some(tokens) = input.max_tokens {
                request = request.max_tokens(tokens);
            }
            let mut runner = AgentRunner::new(database.core.store_ref())
                .map_err(|_| failure("Cannot initialize native Agent"))?;
            runner.set_skill_registry(registry);
            runner.set_tracer_provider(Box::new(talon_agent::agent::trace::NoopTracerProvider));
            let provider = HostProvider {
                op: op.clone(),
                id,
                name,
                models,
            };
            let executor = HostTools { op: op.clone() };
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            // Drain in the same future: no detached consumer outlives the database owner.
            let run = runner.run(request.build(), &provider, &executor, tx);
            tokio::pin!(run);
            let result = loop {
                tokio::select! {biased;event=rx.recv()=>{if let Some(value)=event{project(&op,value).await?;}else{break run.await;}},result=&mut run=>break result}
            };
            while let Ok(value) = rx.try_recv() {
                project(&op, value).await?;
            }
            Ok(serde_json::to_value(report(&result))?)
        })
    }))
}

fn native_typed(value: sdk_agent::tool::TypedToolResult) -> Result<TypedToolResult> {
    use sdk_agent::tool::TypedToolResult as Wire;
    Ok(match value {
        Wire::Text { text } => TypedToolResult::Text { text },
        Wire::Json { value } => TypedToolResult::Json { value },
        Wire::Media { items } => TypedToolResult::Media {
            items: serde_json::from_value(serde_json::to_value(items)?)?,
        },
        Wire::Mixed { blocks } => TypedToolResult::Mixed {
            blocks: blocks
                .into_iter()
                .map(|block| match block {
                    sdk_agent::tool::ToolOutputBlock::Text { text } => {
                        Ok(ToolOutputBlock::Text { text })
                    }
                    sdk_agent::tool::ToolOutputBlock::Media { items } => {
                        Ok(ToolOutputBlock::Media {
                            items: serde_json::from_value(serde_json::to_value(items)?)?,
                        })
                    }
                })
                .collect::<Result<Vec<_>>>()?,
        },
    })
}
fn wire_typed(value: TypedToolResult) -> Result<sdk_agent::tool::TypedToolResult> {
    use sdk_agent::tool::{ToolOutputBlock as Block, TypedToolResult as Wire};
    Ok(match value {
        TypedToolResult::Text { text } => Wire::Text { text },
        TypedToolResult::Json { value } => Wire::Json { value },
        TypedToolResult::Media { items } => Wire::Media {
            items: serde_json::from_value(serde_json::to_value(items)?)?,
        },
        TypedToolResult::Mixed { blocks } => Wire::Mixed {
            blocks: blocks
                .into_iter()
                .map(|block| match block {
                    ToolOutputBlock::Text { text } => Ok(Block::Text { text }),
                    ToolOutputBlock::Media { items } => Ok(Block::Media {
                        items: serde_json::from_value(serde_json::to_value(items)?)?,
                    }),
                })
                .collect::<Result<Vec<_>>>()?,
        },
    })
}
struct HostTools {
    op: Arc<Operation>,
}
impl ToolExecutor for HostTools {
    fn execute(
        &self,
        name: &str,
        args: &Value,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ToolResult> + Send + '_>> {
        let request = json!({"method":"tool","name":name,"args":args});
        Box::pin(async move {
            let result = async {
                let (_, mut rx) = self.op.callback(request).await?;
                let reply = self.op.receive(&mut rx).await?;
                if !reply.done {
                    return Err(failure("Nonterminal host tool reply"));
                }
                let value: sdk_agent::tool::ToolResult = serde_json::from_value(reply.value)?;
                Ok(ToolResult {
                    content: value.content,
                    is_error: value.is_error,
                    sandbox_level: value.sandbox_level,
                    structured_result: value.structured_result.map(native_typed).transpose()?,
                })
            }
            .await;
            result.unwrap_or_else(|_: Failure| {
                ToolResult::error("Host tool callback failed or cancelled")
            })
        })
    }
}

pub(super) fn prepare_builtin(request: &Value) -> Result<Job> {
    let name = text(request, "name")?.to_string();
    if !matches!(
        name.as_str(),
        "read_file" | "write_file" | "list_directory" | "search_files" | "execute_skill_script"
    ) {
        return Err(failure("Built-in tool is outside platform-host-v1"));
    }
    let args = request["args"].clone();
    let policy: Option<sdk_agent::security::FileAccessPolicy> =
        serde_json::from_value(request["policy"].clone())?;
    // A constrained filesystem root is mandatory on this public native interface.
    let policy = policy.ok_or_else(|| failure("A filesystem policy is required"))?;
    let mut native_policy = talon_agent::security::FileAccessPolicy::new(policy.workspace_root);
    if policy.credential_protection {
        native_policy = native_policy.with_credential_protection();
    }
    let skills: Vec<talon_agent::skill::local::LocalSkill> =
        serde_json::from_value(request["skills"].clone())?;
    let workspace: Option<std::path::PathBuf> =
        serde_json::from_value(request["workspace"].clone())?;
    Ok(Box::new(move |op| {
        Box::pin(async move {
            let mut executor =
                talon_agent::tool::builtin::BuiltinToolExecutor::new(Some(native_policy));
            if let Some(workspace) = workspace {
                executor = executor.with_local_skills(skills, workspace);
            }
            let result = tokio::select! {biased;_=op.cancel.cancelled()=>return Err(failure("Built-in cancelled; effect may require reconciliation")),result=executor.execute(&name,&args)=>result};
            Ok(serde_json::to_value(sdk_agent::tool::ToolResult {
                content: result.content,
                is_error: result.is_error,
                sandbox_level: result.sandbox_level,
                structured_result: result.structured_result.map(wire_typed).transpose()?,
            })?)
        })
    }))
}
