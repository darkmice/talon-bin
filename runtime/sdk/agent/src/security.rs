use std::path::PathBuf;
#[derive(Debug, Clone, Copy, Default, serde::Serialize, serde::Deserialize)]
pub enum SecurityLevel {
    Strict,
    #[default]
    Standard,
    Relaxed,
    Unrestricted,
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FileAccessPolicy {
    pub workspace_root: PathBuf,
    pub credential_protection: bool,
}
impl FileAccessPolicy {
    pub fn new(workspace_root: PathBuf) -> Self {
        Self {
            workspace_root,
            credential_protection: false,
        }
    }
    pub fn with_credential_protection(mut self) -> Self {
        self.credential_protection = true;
        self
    }
}
