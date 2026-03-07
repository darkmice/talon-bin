/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 */
//! Talon Bundle: 将 Talon 核心引擎 + AI 扩展打包为单一预编译库。
//!
//! 通过 `ctor` 在库加载时自动注册 AI 模块处理器，
//! 使 `talon_execute({"module":"ai",...})` 路由到 talon-ai 的真实实现。

/// 程序启动时自动注册 AI 模块处理器。
///
/// `ctor` 保证在 `main()` 之前执行（或动态库加载时执行）。
/// 注册后，所有通过 `talon_execute` 发送的 `{"module":"ai"}` 命令
/// 将被路由到 `talon_ai::ffi_dispatch::handle_ai_command`。
#[ctor::ctor]
fn register_ai_module() {
    talon::register_ai_handler(talon_ai::ffi_dispatch::handle_ai_command);
}

// Re-export talon core 的所有 C FFI 符号，确保 libtalon.a 中包含完整的 FFI 入口。
// 通过 `use` 强制链接器保留 talon crate 中的所有 pub 符号。
pub use talon::*;
