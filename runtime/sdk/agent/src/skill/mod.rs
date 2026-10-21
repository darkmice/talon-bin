/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 */
//! 技能系统 — Agent 的一等公民能力。
//!
//! Skill 是一组预定义的工具 + 提示词 + 执行策略的集合。
//! Agent 通过 SkillSelection 策略在运行时动态加载技能。

pub mod local;

/// 技能定义。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SkillDefinition {
    /// 技能 ID。
    pub id: String,
    /// 技能名称。
    pub name: String,
    /// 技能描述。
    pub description: String,
    /// 技能包含的工具名称列表。
    pub tool_names: Vec<String>,
    /// 技能的系统提示词补丁。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_prompt_patch: Option<String>,
    /// 技能是否启用。
    pub enabled: bool,
}

/// 技能校验失败原因。
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SkillValidationError {
    #[error("skill id is empty")]
    EmptyId,
    #[error("skill '{0}' has empty name")]
    EmptyName(String),
    #[error("skill '{0}' declares an empty tool name")]
    EmptyToolName(String),
    #[error("skill '{skill}' references unknown tool '{tool}'")]
    UnknownTool { skill: String, tool: String },
}

/// 技能注册表。
pub struct SkillRegistry {
    skills: std::sync::RwLock<std::collections::HashMap<String, SkillDefinition>>,
}

impl SkillRegistry {
    pub fn new() -> Self {
        Self {
            skills: std::sync::RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// 注册技能（无校验,直接覆盖）。
    pub fn register(&self, skill: SkillDefinition) {
        let mut map = self.skills.write().unwrap_or_else(|e| e.into_inner());
        map.insert(skill.id.clone(), skill);
    }

    /// 校验技能的结构完整性。`available_tools` 为 `Some` 时,额外校验
    /// 每个 `tool_names` 都存在于该集合中——防坏 skill 引用不存在的工具
    /// 后静默失效(运行时该工具不可用却无报错)。
    pub fn validate_skill(
        skill: &SkillDefinition,
        available_tools: Option<&[String]>,
    ) -> Result<(), SkillValidationError> {
        if skill.id.trim().is_empty() {
            return Err(SkillValidationError::EmptyId);
        }
        if skill.name.trim().is_empty() {
            return Err(SkillValidationError::EmptyName(skill.id.clone()));
        }
        for tool in &skill.tool_names {
            if tool.trim().is_empty() {
                return Err(SkillValidationError::EmptyToolName(skill.id.clone()));
            }
            if let Some(avail) = available_tools {
                if !avail.iter().any(|t| t == tool) {
                    return Err(SkillValidationError::UnknownTool {
                        skill: skill.id.clone(),
                        tool: tool.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    /// 校验后注册:校验失败则拒绝注册并返回错误,不污染注册表。
    pub fn register_validated(
        &self,
        skill: SkillDefinition,
        available_tools: Option<&[String]>,
    ) -> Result<(), SkillValidationError> {
        Self::validate_skill(&skill, available_tools)?;
        self.register(skill);
        Ok(())
    }

    /// 获取所有启用的技能。
    pub fn enabled_skills(&self) -> Vec<SkillDefinition> {
        let map = self.skills.read().unwrap_or_else(|e| e.into_inner());
        map.values().filter(|s| s.enabled).cloned().collect()
    }

    /// 按 ID 获取技能。
    pub fn get_skill(&self, id: &str) -> Option<SkillDefinition> {
        let map = self.skills.read().unwrap_or_else(|e| e.into_inner());
        map.get(id).cloned()
    }

    /// 按 SkillSelection 解析激活的技能列表。
    ///
    /// - `None` → 空
    /// - `Manual(ids)` → 按 ID 查找，忽略不存在的
    /// - `Auto` → 所有 enabled skills
    pub fn resolve(&self, selection: &crate::SkillSelection) -> Vec<SkillDefinition> {
        match selection {
            crate::SkillSelection::None => Vec::new(),
            crate::SkillSelection::Auto => self.enabled_skills(),
            crate::SkillSelection::Manual(ids) => {
                let map = self.skills.read().unwrap_or_else(|e| e.into_inner());
                ids.iter().filter_map(|id| map.get(id).cloned()).collect()
            }
        }
    }

    /// 收集激活技能声明的所有 tool_names（去重）。
    pub fn resolve_tool_names(&self, selection: &crate::SkillSelection) -> Vec<String> {
        let skills = self.resolve(selection);
        let mut names: Vec<String> = skills.into_iter().flat_map(|s| s.tool_names).collect();
        names.sort();
        names.dedup();
        names
    }
}

impl Default for SkillRegistry {
    fn default() -> Self {
        Self::new()
    }
}
