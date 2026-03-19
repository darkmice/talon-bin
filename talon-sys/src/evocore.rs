/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 */
//! EvoCore FFI 封装 — 通过 `module:"evo"` 命令访问进化引擎。
//!
//! 使用前提：预编译库必须是 `libtalon-evocore.a`（包含 talon + talon-ai + evo-core）。
//! 通过 Cargo feature `evocore` 启用。

use crate::{Talon, TalonError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// ── EvoCore 公开类型 ────────────────────────────────────────────────────────

/// 学习输入。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvoLearningInput {
    pub domain: String,
    pub task_type: String,
    #[serde(default)]
    pub complexity: u8,
    pub success: bool,
    #[serde(default = "default_strategy")]
    pub strategy: String,
    #[serde(default)]
    pub skill_name: Option<String>,
    #[serde(default)]
    pub error_type: Option<String>,
    #[serde(default)]
    pub execution_id: Option<String>,
    #[serde(default)]
    pub context: BTreeMap<String, String>,
}

fn default_strategy() -> String {
    "default".into()
}

/// 学习结果。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvoLearningResult {
    pub strategy_used: String,
    pub personality_shift: BTreeMap<String, f64>,
    pub mutations: Vec<serde_json::Value>,
    pub cycle_ms: u64,
}

/// 策略推荐。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvoStrategyRecommendation {
    pub strategy: String,
    pub confidence: f64,
    pub explanation: String,
}

/// 个性快照。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvoPersonalitySnapshot {
    pub dimensions: BTreeMap<String, f64>,
    pub timestamp: i64,
}

// ── EvoCore 封装 ────────────────────────────────────────────────────────────

/// EvoCore 引擎封装（通过 talon_execute JSON 协议）。
pub struct EvoEngine<'a> {
    db: &'a Talon,
    instance_id: u64,
}

impl<'a> EvoEngine<'a> {
    /// 使用默认配置创建 EvoCore 实例。
    pub fn open(db: &'a Talon) -> Result<Self, TalonError> {
        Self::open_with_config(db, None)
    }

    /// 使用自定义配置创建 EvoCore 实例。
    ///
    /// `config` 为 `None` 时使用 EvoCore 默认配置。
    /// 传入 `serde_json::Value` 可灵活配置各项参数。
    ///
    /// ```rust,no_run
    /// use talon::{Talon, TalonEvoExt};
    /// let db = Talon::open("./data").unwrap();
    /// let config = serde_json::json!({
    ///     "memory": { "fts_weight": 0.4, "vec_weight": 0.6 }
    /// });
    /// // let evo = EvoEngine::open_with_config(&db, Some(config)).unwrap();
    /// ```
    pub fn open_with_config(
        db: &'a Talon,
        config: Option<serde_json::Value>,
    ) -> Result<Self, TalonError> {
        let mut cmd_params = serde_json::json!({});
        if let Some(cfg) = config {
            cmd_params["config"] = cfg;
        }
        let cmd = serde_json::json!({
            "module": "evo",
            "action": "open",
            "params": cmd_params
        });
        let resp = db.exec_cmd_json(&cmd)?;
        let id = resp
            .get("id")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| TalonError("evo open: missing instance id".into()))?;
        Ok(Self { db, instance_id: id })
    }

    /// 执行后学习 — 触发完整进化周期。
    pub fn learn(&self, input: &EvoLearningInput) -> Result<EvoLearningResult, TalonError> {
        let cmd = serde_json::json!({
            "module": "evo",
            "action": "learn",
            "params": {
                "instance_id": self.instance_id,
                "input": serde_json::to_value(input)
                    .map_err(|e| TalonError(format!("serialize: {e}")))?
            }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        serde_json::from_value(resp)
            .map_err(|e| TalonError(format!("deserialize learn result: {e}")))
    }

    /// 策略推荐。
    pub fn recommend_strategy(
        &self,
        signals: &[&str],
    ) -> Result<EvoStrategyRecommendation, TalonError> {
        let cmd = serde_json::json!({
            "module": "evo",
            "action": "recommend_strategy",
            "params": {
                "instance_id": self.instance_id,
                "signals": signals
            }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        serde_json::from_value(resp)
            .map_err(|e| TalonError(format!("deserialize strategy: {e}")))
    }

    /// 获取个性快照。
    pub fn personality_snapshot(&self) -> Result<EvoPersonalitySnapshot, TalonError> {
        let cmd = serde_json::json!({
            "module": "evo",
            "action": "personality_snapshot",
            "params": {
                "instance_id": self.instance_id
            }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        serde_json::from_value(resp)
            .map_err(|e| TalonError(format!("deserialize personality: {e}")))
    }
}

impl<'a> Drop for EvoEngine<'a> {
    fn drop(&mut self) {
        let cmd = serde_json::json!({
            "module": "evo",
            "action": "close",
            "params": {
                "instance_id": self.instance_id
            }
        });
        let _ = self.db.exec_cmd_json(&cmd);
    }
}

/// 扩展 trait：为 Talon 添加 EvoCore 能力。
pub trait TalonEvoExt {
    /// 使用默认配置打开 EvoCore 进化引擎。
    fn evo(&self) -> Result<EvoEngine<'_>, TalonError>;

    /// 使用自定义配置打开 EvoCore 进化引擎。
    fn evo_with_config(&self, config: serde_json::Value) -> Result<EvoEngine<'_>, TalonError>;
}

impl TalonEvoExt for Talon {
    fn evo(&self) -> Result<EvoEngine<'_>, TalonError> {
        EvoEngine::open(self)
    }

    fn evo_with_config(&self, config: serde_json::Value) -> Result<EvoEngine<'_>, TalonError> {
        EvoEngine::open_with_config(self, Some(config))
    }
}
