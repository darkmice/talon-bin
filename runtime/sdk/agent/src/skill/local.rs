//! Native local SKILL.md loading. Loading declares guidance/capabilities, never executes code.
use super::{SkillDefinition, SkillRegistry};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalSkill {
    pub definition: SkillDefinition,
    pub root: PathBuf,
    pub manifest_sha256: String,
}
#[derive(Deserialize)]
struct Metadata {
    name: String,
    description: String,
    #[serde(default)]
    tool_names: Vec<String>,
}
fn error(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
// Cloud-only placeholders are not local skill content. Reading them may trigger
// an unbounded provider download, so inspect Darwin SF_DATALESS before opening.
fn manifest_bytes(path: &Path) -> std::io::Result<Option<Vec<u8>>> {
    let metadata = std::fs::metadata(path)?;
    if !metadata.is_file() || metadata.len() > 128 * 1024 {
        return Err(error("SKILL.md must be a regular file <=128KiB"));
    }
    #[cfg(target_os = "macos")]
    {
        use std::os::macos::fs::MetadataExt;
        if metadata.st_flags() & 0x4000_0000 != 0 {
            return Ok(None);
        }
    }
    let bytes = std::fs::read(path)?;
    if bytes.len() > 128 * 1024 {
        return Err(error("SKILL.md exceeds128KiB"));
    }
    Ok(Some(bytes))
}
impl LocalSkill {
    pub fn verify(&self) -> std::io::Result<()> {
        if digest(
            &manifest_bytes(&self.root.join("SKILL.md"))?
                .ok_or_else(|| error("Local skill content unavailable"))?,
        ) != self.manifest_sha256
        {
            return Err(error("Local skill changed after activation"));
        }
        Ok(())
    }
    pub fn script(&self, relative: &str) -> std::io::Result<PathBuf> {
        self.verify()?;
        let scripts = self.root.join("scripts").canonicalize()?;
        let path = scripts.join(relative).canonicalize()?;
        if !path.starts_with(&scripts)
            || !path.is_file()
            || !matches!(
                path.extension().and_then(|s| s.to_str()),
                Some("py" | "js" | "mjs" | "sh")
            )
        {
            return Err(error(
                "Script must be inside the selected skill scripts directory",
            ));
        }
        Ok(path)
    }
}
pub fn load_local_skills(
    roots: &[PathBuf],
    available_tools: &[String],
) -> std::io::Result<Vec<LocalSkill>> {
    let mut seen = BTreeSet::new();
    let mut skills = Vec::new();
    for root in roots {
        if !root.exists() {
            continue;
        }
        let mut entries = std::fs::read_dir(root)?
            .filter_map(Result::ok)
            .collect::<Vec<_>>();
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            if entry.file_name().to_string_lossy().starts_with('.') {
                continue;
            }
            let path = entry.path();
            if !path.join("SKILL.md").is_file() {
                continue;
            }
            let path = path.canonicalize()?;
            if !seen.insert(path.clone()) {
                continue;
            }
            if skills.len() >= 512 {
                return Err(error("Local skill catalog exceeds512 entries"));
            }
            let Some(bytes) = manifest_bytes(&path.join("SKILL.md"))? else {
                continue;
            };
            let text = std::str::from_utf8(&bytes).map_err(|_| error("SKILL.md must be UTF8"))?;
            let text = text
                .strip_prefix("---\n")
                .ok_or_else(|| error("SKILL.md requires YAML frontmatter"))?;
            let (header, body) = text
                .split_once("\n---")
                .ok_or_else(|| error("Unterminated skill frontmatter"))?;
            let metadata: Metadata =
                serde_yaml::from_str(header).map_err(|_| error("Invalid skill frontmatter"))?;
            if metadata.name.trim().is_empty() || metadata.description.trim().is_empty() {
                return Err(error("Skill name/description required"));
            }
            let id = format!(
                "local/{}/{}",
                path.file_name().unwrap_or_default().to_string_lossy(),
                &digest(path.to_string_lossy().as_bytes())[..12]
            );
            let names = if metadata.tool_names.is_empty() {
                available_tools.to_vec()
            } else {
                metadata.tool_names
            };
            let definition=SkillDefinition{id:id.clone(),name:metadata.name,description:metadata.description,tool_names:names,system_prompt_patch:Some(format!("Skill ID: {}\nLocal skill directory: {}\nRead relative references with read_file. Execute a script using execute_skill_script with this skill ID and path relative to scripts/.\n{}",id,path.display(),body)),enabled:true};
            SkillRegistry::validate_skill(&definition, Some(available_tools))
                .map_err(|_| error("Local skill references an unregistered tool"))?;
            skills.push(LocalSkill {
                definition,
                root: path,
                manifest_sha256: digest(&bytes),
            });
        }
    }
    Ok(skills)
}
