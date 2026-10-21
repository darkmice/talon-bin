//! Host-facing contracts. The real Agent loop runs in the hash-admitted native library.
pub mod hitl;
mod runner;
pub mod security;
pub mod skill;
pub mod tool;
pub use hitl::{HitlChannel, HitlCommand};
pub use runner::AgentRunner;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExecutionLimits {
    pub max_iterations: Option<u32>,
    pub max_tool_calls: Option<u32>,
    pub max_duration: Option<Duration>,
    pub max_parallel_tool_calls: Option<usize>,
}
impl Default for ExecutionLimits {
    fn default() -> Self {
        Self {
            max_iterations: Some(25),
            max_tool_calls: Some(100),
            max_duration: Some(Duration::from_secs(600)),
            max_parallel_tool_calls: None,
        }
    }
}
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub enum SkillSelection {
    #[default]
    Auto,
    Manual(Vec<String>),
    None,
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct AgentRunRequest {
    pub session_id: String,
    pub model: String,
    pub input: String,
    #[serde(default)]
    pub input_parts: Option<Vec<talon_llm::ChatMessagePart>>,
    #[serde(default)]
    pub max_tokens: Option<u32>,
    pub tools: Vec<tool::ToolDefinition>,
    pub skills: SkillSelection,
    pub system_prompt: Option<String>,
    pub security: security::SecurityLevel,
    pub limits: ExecutionLimits,
    #[serde(skip)]
    pub cancellation_token: Option<CancellationToken>,
    #[serde(skip)]
    pub hitl: Option<Arc<HitlChannel>>,
}
impl std::fmt::Debug for AgentRunRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NativeAgentRequest(redacted)")
    }
}
impl AgentRunRequest {
    pub fn builder(model: &str, input: &str) -> AgentRunRequestBuilder {
        AgentRunRequestBuilder(Self {
            session_id: uuid_session(),
            model: model.into(),
            input: input.into(),
            input_parts: None,
            max_tokens: None,
            tools: vec![],
            skills: SkillSelection::Auto,
            system_prompt: None,
            security: Default::default(),
            limits: Default::default(),
            cancellation_token: None,
            hitl: None,
        })
    }
}
fn uuid_session() -> String {
    uuid::Uuid::new_v4().to_string()
}
pub struct AgentRunRequestBuilder(AgentRunRequest);
impl AgentRunRequestBuilder {
    pub fn session_id(mut self, id: impl Into<String>) -> Self {
        self.0.session_id = id.into();
        self
    }
    pub fn tools(mut self, tools: Vec<tool::ToolDefinition>) -> Self {
        self.0.tools = tools;
        self
    }
    pub fn input_parts(mut self, parts: Vec<talon_llm::ChatMessagePart>) -> Self {
        self.0.input_parts = Some(parts);
        self
    }
    pub fn max_tokens(mut self, tokens: u32) -> Self {
        self.0.max_tokens = Some(tokens);
        self
    }
    pub fn skills(mut self, skills: SkillSelection) -> Self {
        self.0.skills = skills;
        self
    }
    pub fn system_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.0.system_prompt = Some(prompt.into());
        self
    }
    pub fn security(mut self, security: security::SecurityLevel) -> Self {
        self.0.security = security;
        self
    }
    pub fn limits(mut self, limits: ExecutionLimits) -> Self {
        self.0.limits = limits;
        self
    }
    pub fn cancellation_token(mut self, cancel: CancellationToken) -> Self {
        self.0.cancellation_token = Some(cancel);
        self
    }
    pub fn hitl(mut self, hitl: Arc<HitlChannel>) -> Self {
        self.0.hitl = Some(hitl);
        self
    }
    pub fn build(self) -> AgentRunRequest {
        self.0
    }
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExecutionReport {
    pub iterations: u32,
    pub tool_calls: u32,
    pub final_output: Option<String>,
    pub finish_reason: String,
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum AgentEvent {
    RunStart {
        session_id: String,
        model: String,
    },
    IterationStart {
        iteration: u32,
    },
    LlmStream(talon_llm::StreamEvent),
    ThinkingPart {
        thought: String,
    },
    SkillsActivated {
        skill_ids: Vec<String>,
    },
    LimitReached {
        kind: String,
        action: String,
    },
    HitlInjected {
        message: String,
    },
    Error {
        message: String,
        retryable: bool,
    },
    ToolUsePart {
        tool_call_id: String,
        tool_name: String,
        args: String,
    },
    ApprovalRequired {
        tool_call_id: String,
        tool_name: String,
        explanation: String,
        goal: String,
        is_background: bool,
        #[serde(skip)]
        reply_tx: Arc<tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<bool>>>>,
    },
    ToolResult {
        tool_call_id: String,
        result: String,
        is_error: bool,
        sandbox_level: Option<String>,
    },
    ProgressUpdate {
        message: String,
    },
    RunEnd {
        report: ExecutionReport,
    },
    Observation {
        name: String,
    },
}
// Preserve the platform's existing tuple-variant API with a separate serde wire adapter.
pub mod agent {
    pub use crate::AgentRunner;
    pub mod trace {
        pub struct NoopTracerProvider;
    }
}
