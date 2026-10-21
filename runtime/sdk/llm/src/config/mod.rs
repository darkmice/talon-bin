/*
 * Copyright (c) 2026 Talon Contributors
 * Author: dark.lijin@gmail.com
 * Licensed under the Talon Community Dual License Agreement.
 */
//! 模型配置与路由。

mod model_config;
mod routing;

pub use model_config::{AiModelConfig, ModelOverride, ProviderConfig, ProviderProtocol};
pub use routing::ModelRouting;
