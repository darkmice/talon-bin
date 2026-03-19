/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 */
//! Talon Bundle EvoCore: 将 Talon 核心引擎 + AI 扩展 + EvoCore 进化引擎打包为单一预编译库。
//!
//! 产物层级（俄罗斯套娃）：
//! - `libtalon.a` — 基础数据引擎
//! - `libtalon-ai.a` — ↑ + AI 引擎（Session/Memory/RAG/Agent/Trace）
//! - `libtalon-evocore.a` — ↑ + 进化引擎（Learn/Evolve/CircuitBreaker/Team）
//!
//! 注册机制（与 talon-bundle 一致）：
//! 1. `#[ctor]`（自动）：库加载时自动注册
//! 2. `talon_bundle_init_ai()` + `talon_bundle_init_evo()`（显式 C ABI）

use std::sync::OnceLock;

static AI_INIT: OnceLock<()> = OnceLock::new();
static EVO_INIT: OnceLock<()> = OnceLock::new();

/// 注册 AI 模块处理器。
fn do_register_ai() {
    AI_INIT.get_or_init(|| {
        talon::register_ai_handler(talon_ai::ffi_dispatch::handle_ai_command);
    });
}

/// 注册 EvoCore 模块处理器。
fn do_register_evo() {
    EVO_INIT.get_or_init(|| {
        talon::register_evo_handler(evo_core::ffi_dispatch::handle_evo_command);
    });
}

/// 程序启动时自动注册所有模块处理器（ctor 方案）。
#[ctor::ctor]
fn register_all_modules() {
    do_register_ai();
    do_register_evo();
}

/// 显式 C ABI 注册入口 — AI 模块。
#[no_mangle]
pub extern "C" fn talon_bundle_init_ai() {
    do_register_ai();
}

/// 显式 C ABI 注册入口 — EvoCore 模块。
#[no_mangle]
pub extern "C" fn talon_bundle_init_evo() {
    do_register_evo();
}

// Re-export 所有 C FFI 符号，确保链接器保留。
pub use talon::*;
