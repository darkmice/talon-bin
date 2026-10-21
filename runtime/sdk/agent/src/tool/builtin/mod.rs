mod definitions;
use crate::{
    security::FileAccessPolicy,
    skill::local::LocalSkill,
    tool::{ToolExecutor, ToolResult},
};
pub use definitions::*;
use serde_json::json;
use std::path::PathBuf;
pub struct BuiltinToolExecutor {
    policy: Option<FileAccessPolicy>,
    skills: Vec<LocalSkill>,
    workspace: Option<PathBuf>,
}
impl BuiltinToolExecutor {
    pub fn new(policy: Option<FileAccessPolicy>) -> Self {
        Self {
            policy,
            skills: vec![],
            workspace: None,
        }
    }
    pub fn with_local_skills(mut self, skills: Vec<LocalSkill>, workspace: PathBuf) -> Self {
        self.skills = skills;
        self.workspace = Some(workspace);
        self
    }
}
impl ToolExecutor for BuiltinToolExecutor {
    fn execute(
        &self,
        name: &str,
        args: &serde_json::Value,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ToolResult> + Send + '_>> {
        let request = json!({"operation":"builtin","name":name,"args":args,"policy":self.policy,"skills":self.skills,"workspace":self.workspace});
        Box::pin(async move {
            let result = async {
                let operation = talon_runtime_transport::Operation::start(request)?;
                loop {
                    let value = operation.next().await?;
                    match value["kind"].as_str() {
                        Some("done") => {
                            return serde_json::from_value(value["result"].clone())
                                .map_err(Into::into)
                        }
                        Some("error") => {
                            return Err(talon_runtime_transport::error(
                                "Native built-in execution failed",
                            ))
                        }
                        _ => return Err(talon_runtime_transport::error("Invalid built-in event")),
                    }
                }
            }
            .await;
            result.unwrap_or_else(|_: talon_runtime_transport::Error| {
                ToolResult::error("Native built-in execution failed")
            })
        })
    }
}
