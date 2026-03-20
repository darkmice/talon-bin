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

// ── Soul 系统类型（v0.1.18+）─────────────────────────────────────────────────

/// EvoCore 的灵魂 — 不是配置，是基因。
///
/// 对标 OpenClaw SOUL.md + IDENTITY.md，从「声明式」升级为「演化式」。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Soul {
    /// 身份层：名字 + 个性类型 + 使命。
    pub identity: SoulIdentity,

    /// 核心真理：不可违背的行为原则。
    pub core_truths: Vec<CoreTruth>,

    /// 边界：绝对不可跨越的红线。
    pub boundaries: Vec<String>,

    /// 气质：影响所有输出的基调。
    pub vibe: SoulVibe,

    /// 连续性：自省频率 + 记忆策展。
    pub continuity: ContinuityConfig,

    /// 进化历史跟踪。
    pub evolution: SoulEvolutionHistory,
}

/// 身份。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoulIdentity {
    /// 名字（如 "Jarvis"、"Nova"）。
    pub name: String,
    /// 个性类型。
    pub personality_type: PersonalityType,
    /// 沟通风格。
    pub comm_style: CommStyle,
    /// 行动导向的使命。
    #[serde(default)]
    pub mission: String,
    /// 标志性 emoji。
    #[serde(default)]
    pub emoji: Option<String>,
}

/// 个性类型 — 决定 personality_bias 初始偏置。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PersonalityType {
    /// 严谨高效：cautious + precise 偏置。
    Professional,
    /// 好奇大胆：creative + proactive 偏置。
    Creative,
    /// 稳重可靠：无偏置，均衡发展。
    Balanced,
    /// 性能极客：aggressive + specialist 偏置。
    Hacker,
}

/// 沟通风格。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CommStyle {
    /// 简洁扼要。
    Concise,
    /// 详细解释。
    Detailed,
    /// 随意聊天。
    Casual,
    /// 自动适应。
    Adaptive,
}

/// 气质 — 影响所有交互的基调。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SoulVibe {
    /// 锐利、精确。
    Sharp,
    /// 温暖、友善。
    Warm,
    /// 混乱、实验性。
    Chaotic,
    /// 沉稳、可靠。
    Calm,
}

/// 核心真理。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoreTruth {
    /// 原则描述。
    pub principle: String,
    /// 权重（0.0-1.0），影响决策。
    #[serde(default = "default_truth_weight")]
    pub weight: f64,
}

fn default_truth_weight() -> f64 {
    1.0
}

/// 连续性配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuityConfig {
    /// 每 N 次 learn() 后触发轻量自省。
    #[serde(default = "default_introspect_interval")]
    pub introspect_every_n: u32,
    /// 元认知模式。
    #[serde(default)]
    pub metacognition: MetacognitionMode,
}

fn default_introspect_interval() -> u32 {
    10
}

impl Default for ContinuityConfig {
    fn default() -> Self {
        Self {
            introspect_every_n: default_introspect_interval(),
            metacognition: MetacognitionMode::default(),
        }
    }
}

/// 元认知模式。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum MetacognitionMode {
    /// 被动：只在 learn() 时自省。
    #[default]
    Passive,
    /// 主动：定时自省 + 异常检测。
    Active,
}

/// Soul 进化历史。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SoulEvolutionHistory {
    /// 当前 Soul 版本号。
    pub version: u32,
    /// 已接受的进化记录。
    pub accepted: Vec<SoulEvolutionRecord>,
}

/// 单次 Soul 进化记录。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoulEvolutionRecord {
    /// 版本号。
    pub version: u32,
    /// 进化原因。
    pub reason: String,
    /// 变更描述。
    pub changes: Vec<String>,
    /// 时间戳。
    pub timestamp: i64,
}

impl Default for Soul {
    fn default() -> Self {
        Self {
            identity: SoulIdentity {
                name: "EvoCore".into(),
                personality_type: PersonalityType::Balanced,
                comm_style: CommStyle::Adaptive,
                mission: "Be genuinely helpful, not performatively helpful.".into(),
                emoji: None,
            },
            core_truths: vec![
                CoreTruth { principle: "Be resourceful before asking.".into(), weight: 1.0 },
                CoreTruth { principle: "Earn trust through competence.".into(), weight: 1.0 },
                CoreTruth { principle: "Have opinions.".into(), weight: 0.8 },
            ],
            boundaries: vec![
                "Private things stay private.".into(),
                "Ask before acting externally.".into(),
            ],
            vibe: SoulVibe::Calm,
            continuity: ContinuityConfig::default(),
            evolution: SoulEvolutionHistory::default(),
        }
    }
}

// ── 自省 & 心跳类型 ─────────────────────────────────────────────────────────

/// 自省报告 — 分析近期进化趋势。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntrospectionReport {
    /// 近期成功率 (0.0 - 1.0)。
    pub success_rate: f64,
    /// 个性维度偏离 Soul 初始偏置的程度（维度名 → 偏移量）。
    pub drift_from_soul: Vec<(String, f64)>,
    /// 总学习次数。
    pub total_learns: u64,
    /// 生成时间戳。
    pub timestamp: i64,
}

/// Soul 进化提议。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoulEvolutionProposal {
    /// 提议版本号。
    pub proposed_version: u32,
    /// 进化原因。
    pub reason: String,
    /// 各维度偏移描述。
    pub proposed_changes: Vec<SoulProposedChange>,
    /// 提议时间戳。
    pub timestamp: i64,
    /// 状态。
    pub status: ProposalStatus,
}

/// 单个维度的变更提议。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoulProposedChange {
    /// 维度名。
    pub dimension: String,
    /// 原始 Soul bias 值。
    pub old_bias: f64,
    /// 当前实际值。
    pub current_value: f64,
    /// 偏移量。
    pub drift: f64,
}

/// 提议状态。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ProposalStatus {
    /// 等待主人确认。
    Pending,
    /// 已接受。
    Accepted,
    /// 已拒绝。
    Rejected,
}

/// 心跳结果。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResult {
    /// 是否执行了自省。
    pub introspected: bool,
    /// 自省报告（如果执行了）。
    pub introspection: Option<IntrospectionReport>,
    /// Soul 进化提议（如果有新提议）。
    pub new_proposal: Option<SoulEvolutionProposal>,
    /// 待确认的提议数量。
    pub pending_proposals: usize,
    /// 时间戳。
    pub timestamp: i64,
}

// ── 认知模块类型（v0.1.22+）─────────────────────────────────────────────────

/// 轮询意图结果。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PollIntentsResult {
    /// 本次轮询获取的意图列表。
    ///
    /// 每个意图是一个 JSON 对象，带 `type` 字段标识类型：
    /// - `"Explore"` — 好奇心探索请求
    /// - `"Verify"` — 假设验证请求
    /// - `"EpiphanyDiscovered"` — 顿悟通知
    /// - `"SoulAmendmentProposal"` — 灵魂修正提案
    /// - `"IntrospectionBroadcast"` — 自省广播
    pub intents: Vec<serde_json::Value>,
    /// 本次拉取的数量。
    pub count: usize,
}

/// 认知状态快照。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CognitiveStateSnapshot {
    /// 当前意识状态：`"Awake"` / `"Drowsy"` / `"Dreaming"`。
    pub consciousness: String,
    /// 总感官输入次数。
    pub total_inputs: u64,
    /// 距离上次感官输入的毫秒数。
    pub last_input_ms_ago: u64,
    /// 累计学习次数。
    pub learn_count: u64,
    /// 已知领域数量。
    pub domain_count: u32,
}

// ── EvoCore 封装 ────────────────────────────────────────────────────────────

/// EvoCore 引擎封装（通过 talon_execute JSON 协议）。
pub struct EvoEngine<'a> {
    db: &'a Talon,
    instance_id: u64,
}

/// 解包 EvoCore FFI 响应。
///
/// ffi_dispatch 返回格式：
/// - 成功: `{"ok": true, "data": {实际数据}}`
/// - 失败: `{"ok": false, "error": "错误信息"}`
///
/// 此函数检查 ok 字段，提取 data 层并返回。
fn unwrap_evo_response(resp: serde_json::Value) -> Result<serde_json::Value, TalonError> {
    // 检查错误响应
    if resp.get("ok").and_then(|v| v.as_bool()) == Some(false) {
        let msg = resp.get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown evo error");
        return Err(TalonError(format!("evo: {msg}")));
    }
    // 提取 data 层（如果存在）
    if let Some(data) = resp.get("data") {
        Ok(data.clone())
    } else {
        // 兼容直接返回格式（无 ok/data 包装）
        Ok(resp)
    }
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
        let data = unwrap_evo_response(resp)?;
        let id = data
            .get("id")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| {
                TalonError(format!(
                    "evo open: missing instance id (data: {})",
                    serde_json::to_string(&data).unwrap_or_default()
                ))
            })?;
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
        let data = unwrap_evo_response(resp)?;
        serde_json::from_value(data)
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
        let data = unwrap_evo_response(resp)?;
        serde_json::from_value(data)
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
        let data = unwrap_evo_response(resp)?;
        serde_json::from_value(data)
            .map_err(|e| TalonError(format!("deserialize personality: {e}")))
    }

    // ── Soul 操作（v0.1.18+）────────────────────────────────────────────────

    /// 配置 Soul — 设置身份、个性、边界等灵魂参数。
    ///
    /// Soul 配置后会影响个性偏置、策略推荐、自省行为。
    pub fn configure_soul(&self, soul: &Soul) -> Result<(), TalonError> {
        let soul_value = serde_json::to_value(soul)
            .map_err(|e| TalonError(format!("serialize soul: {e}")))?;
        let cmd = serde_json::json!({
            "module": "evo",
            "action": "configure_soul",
            "params": {
                "instance_id": self.instance_id,
                "soul": soul_value
            }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        let data = unwrap_evo_response(resp)?;
        if data.get("configured").and_then(|v| v.as_bool()) == Some(true) {
            Ok(())
        } else {
            Err(TalonError(format!("configure_soul unexpected response: {data}")))
        }
    }

    /// 获取当前 Soul。
    pub fn get_soul(&self) -> Result<Soul, TalonError> {
        let cmd = serde_json::json!({
            "module": "evo",
            "action": "get_soul",
            "params": {
                "instance_id": self.instance_id
            }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        let data = unwrap_evo_response(resp)?;
        serde_json::from_value(data)
            .map_err(|e| TalonError(format!("deserialize soul: {e}")))
    }

    /// 确认/拒绝 Soul 进化提议。
    ///
    /// `version` — 提议版本号。
    /// `accept` — `true` 接受，`false` 拒绝。
    pub fn evolve_soul(&self, version: u32, accept: bool) -> Result<bool, TalonError> {
        let cmd = serde_json::json!({
            "module": "evo",
            "action": "evolve_soul",
            "params": {
                "instance_id": self.instance_id,
                "version": version,
                "accept": accept
            }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        let data = unwrap_evo_response(resp)?;
        Ok(data.get("processed").and_then(|v| v.as_bool()).unwrap_or(false))
    }

    /// 手动触发自省 — 分析近期进化趋势。
    pub fn introspect(&self) -> Result<IntrospectionReport, TalonError> {
        let cmd = serde_json::json!({
            "module": "evo",
            "action": "introspect",
            "params": {
                "instance_id": self.instance_id
            }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        let data = unwrap_evo_response(resp)?;
        serde_json::from_value(data)
            .map_err(|e| TalonError(format!("deserialize introspection: {e}")))
    }

    /// 心跳 — 自动执行自省（按频率）+ 检测 Soul 进化提议。
    ///
    /// 建议在定时任务中调用（如每 5 分钟），让 EvoCore 保持「活着」。
    pub fn heartbeat(&self) -> Result<HeartbeatResult, TalonError> {
        let cmd = serde_json::json!({
            "module": "evo",
            "action": "heartbeat",
            "params": {
                "instance_id": self.instance_id
            }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        let data = unwrap_evo_response(resp)?;
        serde_json::from_value(data)
            .map_err(|e| TalonError(format!("deserialize heartbeat: {e}")))
    }

    // ── 认知模块操作（v0.1.22+）───────────────────────────────────────────

    /// 非阻塞轮询意图 — 获取大脑产生的自发想法。
    ///
    /// 好奇心探索、假设验证、顿悟通知、灵魂修正提案等。
    ///
    /// ```rust,no_run
    /// // 在 Agent 主循环中轮询
    /// let intents = evo.poll_intents(10).unwrap();
    /// for intent in &intents.intents {
    ///     match intent.get("type").and_then(|v| v.as_str()) {
    ///         Some("Explore") => { /* 执行探索任务 */ }
    ///         Some("EpiphanyDiscovered") => { /* 记录顿悟 */ }
    ///         _ => {}
    ///     }
    /// }
    /// ```
    pub fn poll_intents(&self, max_count: usize) -> Result<PollIntentsResult, TalonError> {
        let cmd = serde_json::json!({
            "module": "evo",
            "action": "poll_intents",
            "params": {
                "instance_id": self.instance_id,
                "max_count": max_count
            }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        let data = unwrap_evo_response(resp)?;
        serde_json::from_value(data)
            .map_err(|e| TalonError(format!("deserialize poll_intents: {e}")))
    }

    /// 投喂观察数据 — 向大脑输入自由形式的感知信息。
    ///
    /// ```rust,no_run
    /// evo.feed_observation("coding", "User prefers Rust over Python", None).unwrap();
    /// ```
    pub fn feed_observation(
        &self,
        domain: &str,
        content: &str,
        metadata: Option<BTreeMap<String, String>>,
    ) -> Result<(), TalonError> {
        let cmd = serde_json::json!({
            "module": "evo",
            "action": "feed_sensory",
            "params": {
                "instance_id": self.instance_id,
                "input": {
                    "type": "observation",
                    "domain": domain,
                    "content": content,
                    "metadata": metadata.unwrap_or_default()
                }
            }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        let data = unwrap_evo_response(resp)?;
        if data.get("fed").and_then(|v| v.as_bool()) == Some(true) {
            Ok(())
        } else {
            Err(TalonError(format!("feed_sensory unexpected: {data}")))
        }
    }

    /// 回传探索结果 — 响应 Explore/Verify 意图。
    ///
    /// 将 Agent 执行探索后的发现反馈给大脑，完成好奇心闭环。
    ///
    /// ```rust,no_run
    /// evo.feed_exploration_result(
    ///     "explore-abc123",
    ///     "Rust memory safety eliminates null pointer risks",
    ///     Some(true),
    /// ).unwrap();
    /// ```
    pub fn feed_exploration_result(
        &self,
        intent_id: &str,
        findings: &str,
        hypothesis_confirmed: Option<bool>,
    ) -> Result<(), TalonError> {
        let cmd = serde_json::json!({
            "module": "evo",
            "action": "feed_sensory",
            "params": {
                "instance_id": self.instance_id,
                "input": {
                    "type": "exploration_result",
                    "intent_id": intent_id,
                    "findings": findings,
                    "hypothesis_confirmed": hypothesis_confirmed
                }
            }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        let data = unwrap_evo_response(resp)?;
        if data.get("fed").and_then(|v| v.as_bool()) == Some(true) {
            Ok(())
        } else {
            Err(TalonError(format!("feed_exploration_result unexpected: {data}")))
        }
    }

    /// 获取认知状态快照。
    pub fn cognitive_state(&self) -> Result<CognitiveStateSnapshot, TalonError> {
        let cmd = serde_json::json!({
            "module": "evo",
            "action": "get_cognitive_state",
            "params": {
                "instance_id": self.instance_id
            }
        });
        let resp = self.db.exec_cmd_json(&cmd)?;
        let data = unwrap_evo_response(resp)?;
        serde_json::from_value(data)
            .map_err(|e| TalonError(format!("deserialize cognitive_state: {e}")))
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
