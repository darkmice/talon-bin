/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 */
//! Talon Bundle: 将 Talon 核心引擎 + AI / LLM / Agent 默认栈打包为单一预编译库。
//!
//! 提供两种 AI handler 注册机制（互补，确保在各平台均能正确工作）：
//!
//! 1. **`#[ctor]`（自动）**：库加载时自动注册，适用于动态链接或 force_load 场景。
//! 2. **`talon_bundle_init_ai()`（显式 C ABI）**：供 `talon-sys` 的 `Talon::open` 显式调用，
//!    彻底绕开 macOS 静态链接 dead-stripping 对 ctor 的破坏。
//!
//! 说明：
//! - `talon-ai` 仍然是当前需要显式注册到 Talon JSON 路由器的模块；
//! - `talon-llm` 与 `talon-agent` 作为默认栈依赖一并编译进 bundle；
//! - `talon-trace` / `talon-sandbox` / `talon-evo-core` 只进入 full bundle，不进入默认 bundle。

use std::sync::OnceLock;

#[allow(unused_imports)]
use talon_agent as _;
#[allow(unused_imports)]
use talon_llm as _;

static AI_INIT: OnceLock<()> = OnceLock::new();

/// 内部幂等注册函数。
fn do_register_ai() {
    AI_INIT.get_or_init(|| {
        talon::register_ai_handler(talon_ai::ffi_dispatch::handle_ai_command);
    });
}

/// 程序启动时自动注册 AI 模块处理器（ctor 方案）。
///
/// 在支持 ctor 且链接器不 dead-strip 的环境下自动触发。
#[ctor::ctor]
fn register_ai_module() {
    do_register_ai();
}

/// 显式 C ABI 注册入口（供 talon-sys 的 Talon::open 调用）。
///
/// 解决 macOS 静态链接时 `-force_load` 无法保留 `#[ctor]` 函数的问题。
/// `talon-sys` 在 `raw_ffi` 里声明此函数，并在 `Talon::open` 时显式调用。
/// 由于是直接调用（非 ctor），链接器不会 dead-strip 它。
///
/// 多次调用是幂等的（通过 OnceLock 保证），无副作用。
#[no_mangle]
pub extern "C" fn talon_bundle_init_ai() {
    do_register_ai();
}

// Re-export talon core 的所有 C FFI 符号，确保 libtalon.a 中包含完整的 FFI 入口。
// 通过 `use` 强制链接器保留 talon crate 中的所有 pub 符号。
pub use talon::*;
