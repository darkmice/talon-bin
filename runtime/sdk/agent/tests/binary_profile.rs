//! Runs against the admitted shared library, never source-links an engine.
use futures::{stream, Stream};
use serde_json::json;
use std::{
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use talon_agent_sdk::{
    security::SecurityLevel,
    tool::{ToolCategory, ToolDefinition, ToolExecutor, ToolResult, ToolSource},
    AgentEvent, AgentRunRequest, AgentRunner, ExecutionLimits, SkillSelection,
};
use talon_llm::{
    stream::FinishReason, CompletionRequest, LanguageModelProvider, LlmError, ModelCapabilities,
    ModelInfo, StreamEvent,
};
use tokio_util::sync::CancellationToken;

struct Provider {
    calls: AtomicUsize,
    hang: bool,
    batch: usize,
    only_text: bool,
    saw_steering: std::sync::atomic::AtomicBool,
    saw_image: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl LanguageModelProvider for Provider {
    fn id(&self) -> &str {
        "native-test"
    }
    fn display_name(&self) -> &str {
        "Isolated fixture"
    }
    fn models(&self) -> Vec<ModelInfo> {
        vec![ModelInfo {
            id: "fixture".into(),
            display_name: "Fixture".into(),
            vendor: "fixture".into(),
            family: "fixture".into(),
            version: "1".into(),
            max_input_tokens: 65536,
            max_output_tokens: 65536,
            context_window: 131072,
            input_price_per_m: 0.0,
            output_price_per_m: 0.0,
            capabilities: ModelCapabilities {
                vision: true,
                tool_calling: true,
                agent_mode: true,
                ..Default::default()
            },
        }]
    }
    async fn provide_token_count(&self, _model: &str, text: &str) -> Result<u32, LlmError> {
        Ok(text.chars().count() as u32 / 3 + 1)
    }
    async fn complete(
        &self,
        _request: CompletionRequest,
        cancel: CancellationToken,
    ) -> Result<Pin<Box<dyn Stream<Item = StreamEvent> + Send>>, LlmError> {
        self.entered.notify_one();
        if _request.has_images() {
            self.saw_image.store(true, Ordering::Release);
        }
        if serde_json::to_string(&_request)
            .unwrap()
            .contains("steer-token")
        {
            self.saw_steering.store(true, Ordering::Release);
        }
        if self.hang {
            cancel.cancelled().await;
            return Err(LlmError::Cancelled);
        }
        let first = self.calls.fetch_add(1, Ordering::AcqRel) == 0;
        let events = if first && !self.only_text {
            let mut events = Vec::new();
            for index in 0..self.batch {
                let id = format!("call-{index}");
                events.push(StreamEvent::ToolCallStart {
                    tool_call_id: id.clone(),
                    tool_name: "host_lookup".into(),
                });
                events.push(StreamEvent::ToolCallComplete {
                    tool_call_id: id,
                    tool_name: "host_lookup".into(),
                    arguments: "{\"value\":\"中文\"}".into(),
                });
            }
            events.push(StreamEvent::StreamEnd {
                finish_reason: FinishReason::ToolUse,
            });
            events
        } else {
            vec![
                StreamEvent::TextDelta {
                    text: "native complete".into(),
                },
                StreamEvent::StreamEnd {
                    finish_reason: FinishReason::Stop,
                },
            ]
        };
        Ok(Box::pin(stream::iter(events)))
    }
}
struct Tools {
    db: talon::SharedTalon,
    calls: AtomicUsize,
}
impl ToolExecutor for Tools {
    fn execute(
        &self,
        name: &str,
        args: &serde_json::Value,
    ) -> Pin<Box<dyn std::future::Future<Output = ToolResult> + Send + '_>> {
        assert_eq!(name, "host_lookup");
        assert_eq!(args["value"], "中文");
        Box::pin(async move {
            let ordinal = self.calls.fetch_add(1, Ordering::AcqRel) + 1;
            self.db
                .run_sql_param(
                    "INSERT INTO host_receipts (id, content) VALUES (?, ?)",
                    &[
                        talon::Value::Integer(ordinal as i64),
                        talon::Value::Text("中文".into()),
                    ],
                )
                .unwrap();
            ToolResult::text("host result")
        })
    }
}
fn definition() -> ToolDefinition {
    ToolDefinition {
        name: "host_lookup".into(),
        description: "Scoped host lookup".into(),
        parameters: json!({"type":"object","properties":{"value":{"type":"string"}},"required":["value"]}),
        category: ToolCategory::Query,
        capability_overrides: Default::default(),
        source: ToolSource::Skill,
    }
}
fn provider(hang: bool) -> Provider {
    Provider {
        calls: AtomicUsize::new(0),
        hang,
        batch: 1,
        only_text: false,
        saw_steering: std::sync::atomic::AtomicBool::new(false),
        saw_image: std::sync::atomic::AtomicBool::new(false),
        entered: tokio::sync::Notify::new(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admitted_binary_preserves_shared_store_callbacks_stream_and_cancel() {
    let identity = talon_runtime_transport::call_json(json!({"action":"identity"}))
        .expect("verified binary environment required; no skips or source fallback");
    assert_eq!(
        identity["core"]["git_commit"],
        "441e9f59fc0208fe30f21638fbd32fa60e361aad"
    );
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db");
    let db = talon::Talon::open_shared(&path).unwrap();
    let peer = talon::Talon::open_shared(&path).unwrap();
    db.run_sql("CREATE TABLE host_receipts (id INTEGER PRIMARY KEY, content TEXT NOT NULL)")
        .unwrap();
    let session = talon::SessionId::new();
    db.run_sql_param_as(session, "BEGIN", &[]).unwrap();
    assert!(matches!(
        peer.run_sql("SELECT COUNT(*) FROM host_receipts"),
        Err(talon::Error::Busy(_))
    ));
    db.run_sql_param_as(session, "ROLLBACK", &[]).unwrap();
    db.end_session(session);
    db.run_sql("CREATE TABLE decimal_probe (id INTEGER PRIMARY KEY, amount DECIMAL(38,8))")
        .unwrap();
    let amount = talon::Decimal::parse("123456789012345678901234567890.12345678").unwrap();
    db.run_sql_param(
        "INSERT INTO decimal_probe (id, amount) VALUES (?, ?)",
        &[talon::Value::Integer(1), talon::Value::Decimal(amount)],
    )
    .unwrap();
    assert_eq!(
        db.run_sql("SELECT amount FROM decimal_probe").unwrap(),
        vec![vec![talon::Value::Decimal(amount)]]
    );
    let tools = Tools {
        db: peer,
        calls: AtomicUsize::new(0),
    };
    let model = provider(false);
    let mut runner = AgentRunner::new(db.store_ref()).unwrap();
    let registry = talon_agent_sdk::skill::SkillRegistry::new();
    registry
        .register_validated(
            talon_agent_sdk::skill::SkillDefinition {
                id: "fixture-skill".into(),
                name: "Fixture".into(),
                description: "Fixture lookup".into(),
                tool_names: vec!["host_lookup".into()],
                system_prompt_patch: Some("Use the scoped host lookup.".into()),
                enabled: true,
            },
            Some(&["host_lookup".into()]),
        )
        .unwrap();
    runner.set_skill_registry(registry);
    let request = AgentRunRequest::builder("fixture", "Look up the supplied value")
        .session_id("binary-roundtrip")
        .tools(vec![definition()])
        .skills(SkillSelection::Manual(vec!["fixture-skill".into()]))
        .security(SecurityLevel::Relaxed)
        .limits(ExecutionLimits {
            max_iterations: Some(3),
            max_tool_calls: Some(2),
            max_duration: Some(Duration::from_secs(10)),
            max_parallel_tool_calls: Some(1),
        })
        .build();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let result = tokio::time::timeout(
        Duration::from_secs(15),
        runner.run(request, &model, &tools, tx),
    )
    .await
    .unwrap();
    assert_eq!(result.finish_reason, "stop");
    assert_eq!(result.final_output.as_deref(), Some("native complete"));
    let observed: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    assert_eq!(result.tool_calls, 1);
    assert_eq!(tools.calls.load(Ordering::Acquire), 1);
    let mut saw_text = false;
    let mut saw_skills = false;
    for event in observed {
        match event {
            AgentEvent::LlmStream(StreamEvent::TextDelta { text }) => {
                saw_text |= text == "native complete"
            }
            AgentEvent::SkillsActivated { skill_ids } => {
                saw_skills |= skill_ids.contains(&"fixture-skill".into())
            }
            AgentEvent::Error { message, .. } => panic!("native event error: {message}"),
            _ => {}
        }
    }
    assert!(saw_text && saw_skills);
    assert_eq!(
        db.run_sql("SELECT content FROM host_receipts").unwrap(),
        vec![vec![talon::Value::Text("中文".into())]]
    );

    // A multi-tool response may reserve only the remaining native budget.
    runner.set_skill_registry(talon_agent_sdk::skill::SkillRegistry::new());
    db.run_sql("DELETE FROM host_receipts").unwrap();
    tools.calls.store(0, Ordering::Release);
    let mut model = provider(false);
    model.batch = 3;
    let request = AgentRunRequest::builder("fixture", "bounded batch")
        .session_id("binary-budget")
        .tools(vec![definition()])
        .skills(SkillSelection::None)
        .security(SecurityLevel::Relaxed)
        .limits(ExecutionLimits {
            max_tool_calls: Some(1),
            ..Default::default()
        })
        .build();
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        runner.run(request, &model, &tools, tx),
    )
    .await
    .unwrap();
    assert_eq!(result.tool_calls, 1);
    assert_eq!(tools.calls.load(Ordering::Acquire), 1);

    // Strict approval remains a host decision; rejection never executes the tool.
    let model = provider(false);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let request = AgentRunRequest::builder("fixture", "approval")
        .session_id("binary-approval")
        .tools(vec![definition()])
        .skills(SkillSelection::None)
        .security(SecurityLevel::Strict)
        .build();
    let approval = async move {
        let mut requested = false;
        while let Some(event) = rx.recv().await {
            if let AgentEvent::ApprovalRequired { reply_tx, .. } = event {
                requested = true;
                if let Some(sender) = reply_tx.lock().await.take() {
                    let _ = sender.send(false);
                }
            }
        }
        requested
    };
    let (_, requested) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(runner.run(request, &model, &tools, tx), approval)
    })
    .await
    .unwrap();
    assert!(requested);
    assert_eq!(tools.calls.load(Ordering::Acquire), 1);

    // Injection survives the byte ABI and reaches the actual model input.
    let hitl = std::sync::Arc::new(talon_agent_sdk::HitlChannel::new());
    hitl.sender()
        .send(talon_agent_sdk::HitlCommand::Inject {
            message: "steer-token".into(),
        })
        .unwrap();
    let mut model = provider(false);
    model.only_text = true;
    let request = AgentRunRequest::builder("fixture", "steering")
        .session_id("binary-hitl")
        .skills(SkillSelection::None)
        .hitl(hitl.clone())
        .build();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        runner.run(request, &model, &tools, tx),
    )
    .await
    .unwrap();
    assert_eq!(result.finish_reason, "stop");
    assert!(model.saw_steering.load(Ordering::Acquire));
    assert!(!hitl.has_pending_injection());
    assert!(hitl
        .sender()
        .send(talon_agent_sdk::HitlCommand::Inject {
            message: "after-finish".into()
        })
        .is_err());
    assert!(std::iter::from_fn(|| rx.try_recv().ok())
        .any(|event| matches!(event,AgentEvent::HitlInjected{message} if message=="steer-token")));

    // Multimodal request parts retain their typed payload across both ABI hops.
    let mut model = provider(false);
    model.only_text = true;
    let request = AgentRunRequest::builder("fixture", "inspect fixture pixels")
        .session_id("binary-vision")
        .skills(SkillSelection::None)
        .input_parts(vec![talon_llm::ChatMessagePart::Image {
            source: talon_llm::ImageSource::Base64 {
                data: "aGVsbG8=".into(),
            },
            mime_type: "image/png".into(),
            detail: None,
        }])
        .build();
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        runner.run(request, &model, &tools, tx),
    )
    .await
    .unwrap();
    assert_eq!(result.finish_reason, "stop");
    assert!(model.saw_image.load(Ordering::Acquire));

    // Cancellation interrupts a host provider waiting before stream creation.
    {
        let model = provider(true);
        let cancel = CancellationToken::new();
        let request = AgentRunRequest::builder("fixture", "wait")
            .session_id("binary-cancel")
            .cancellation_token(cancel.clone())
            .build();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let run = runner.run(request, &model, &tools, tx);
        tokio::pin!(run);
        tokio::select! {result=&mut run=>panic!("run ended before provider callback: {}",result.finish_reason),_=model.entered.notified()=>{}}
        cancel.cancel();
        let result = tokio::time::timeout(Duration::from_secs(3), &mut run)
            .await
            .unwrap();
        assert_ne!(result.finish_reason, "stop");
    }
    // Dropping an unfinished SDK future synchronously releases the native job.
    let dropped = std::time::Instant::now();
    {
        let model = provider(true);
        let request = AgentRunRequest::builder("fixture", "drop without awaiting")
            .session_id("binary-drop")
            .skills(SkillSelection::None)
            .build();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let run = runner.run(request, &model, &tools, tx);
        tokio::pin!(run);
        tokio::select! {result=&mut run=>panic!("unexpected early finish: {}",result.finish_reason),_=model.entered.notified()=>{}}
    }
    assert!(dropped.elapsed() < Duration::from_secs(3));
    let stopped = talon_runtime_transport::call_json(json!({"action":"identity"})).unwrap();
    assert_eq!(stopped["activeTasks"], 0);
    assert_eq!(stopped["activeOperations"], 0);

    drop(runner);
    drop(tools);
    drop(db);
    for _ in 0..100 {
        let identity = talon_runtime_transport::call_json(json!({"action":"identity"})).unwrap();
        if identity["activeTasks"] == 0 {
            assert_eq!(identity["activeOperations"], 0);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let identity = talon_runtime_transport::call_json(json!({"action":"identity"})).unwrap();
    assert_eq!(identity["activeTasks"], 0);
    assert_eq!(identity["activeOperations"], 0);
    assert_eq!(identity["activeDatabases"], 0);
    // No second source Core is linked; reopening loads persisted bytes through the same binary.
    let reopened = talon::Talon::open_shared(&path).unwrap();
    assert_eq!(
        reopened
            .run_sql("SELECT COUNT(*) FROM host_receipts")
            .unwrap(),
        vec![vec![talon::Value::Integer(1)]]
    );
}
