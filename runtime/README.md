# Talon Native Runtime / Rust SDK

`platform-host-v1` 把 Core、AI、LLM 与 Agent 编译进同一个原生库。
Rust 消费端只编译 `sdk/{core,llm,agent,transport}` 的公开契约和适配器，
不下载或编译私有引擎实现。它与纯 Core `v0.1.56` 的发布线独立，
发布标签使用 `runtime-v<version>`，不能替换旧 `libtalon-core-*` 名称。

## 消费边界

- 使用固定 talon-bin Git commit 的 `talon-core-sdk`、`talon-llm-sdk` 和 `talon-agent-sdk`。
- 从对应 runtime Release 安装原生库；消费方固定 archive/manifest/library 摘要和 Ed25519 公钥身份。
- 用 `talon_runtime_transport::configure` 提供已验证的绝对库路径及固定库摘要。
  环境变量 `TALON_RUNTIME_LIBRARY` / `TALON_RUNTIME_SHA256` 仅适合由受信安装器准备的独立消费者/测试环境。
- `Talon::open_shared` 创建逻辑属主，SQL 与 Agent 使用同一库内的 Core registry。
  不传递 Rust `Store` 内存指针、trait object 或跨库 allocator 所有权。
- Native Agent 请求、模型/工具/审批回调和事件通过版本化字节 ABI 传递。
  HITL 绑定后同步进入 native admission，native close/send 决定是否接受，完成后拒绝追加消息。
- 默认保留平台的宿主权限复核与工具执行权。SDK 不授予 actor、租户或资源权限。

## 协议 v1

入口见 `native/include/talon_runtime.h`。输入/输出最大 8MiB，最多 32 个活动操作、
64 个数据库逻辑句柄、每个操作 64 个活动回调、128 个待处理 HITL 指令。
事件队列最多 128 项 / 16MiB，poll 每批最多 16 项；满队列施加背压，最终状态单独保留。
Callback receiver 的释放通过优先控制事件取消宿主操作；旧 ID、重复结束响应和关闭句柄直接拒绝。

`identity` 返回运行时版本、生产提交/dirty 状态、固定源码锁、真实 Core build manifest 和活动资源计数。
开发候选的 `producerDirty=true` 不能作为线上制品准入证据。
Source Agent 在统一工具 admission 中按已批准执行尝试累计计数，批量调用也遵守剩余预算。

Profile 覆盖当前平台使用的模型流、工具、审批、技能、本地文件策略、Tokenizer、HITL、取消及 Core SQL。
平台消费的 Agent 事件有明确类型；其他未被平台消费的 native 观测事件使用 `Observation`。
不宣称支持原 Agent 的所有可选 Soul、Sandbox、Trace 或其他自定义扩展 trait。

## 生产端开发与验收

只有生产端/CI 需要私有源码读取权限。`native/sources.lock.json` 固定每个输入的 commit、
源文件树摘要与 manifest overlay 摘要；`prepare-native-sources.py` 校验并准备忽略的 `.sources/`。
Core、Session、Block 的不可变依赖同时固定在生产 manifest 和 Cargo.lock 中。
Core 通过生产端固定的干净 Git 检出构建，避免 Cargo Git 缓存的 `.cargo-ok`
被引擎自证视为源码修改；不设置 dirty=false 覆盖，也不删除未知修改。

```sh
python3 runtime/prepare-native-sources.py
cargo test --locked --manifest-path runtime/native/Cargo.toml --lib
cargo build --locked --manifest-path runtime/native/Cargo.toml --lib
# 由安装器/CI设置经过校验的原生库绝对路径和 SHA256，再运行：
cargo test --locked --manifest-path runtime/sdk/Cargo.toml -p talon-agent-sdk --tests
```

本仓没有自动源码回退。构建/签名证据、SDK fixture、平台启动、真实供应商及生产准入是不同层级。
新制品必须通过当前目标的字节 ABI、同库回调、流式/错误、预算、审批、HITL、取消、释放及文件策略验收后才可发布。
