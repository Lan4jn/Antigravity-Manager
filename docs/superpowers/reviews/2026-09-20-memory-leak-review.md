# 内存修复分支检查报告

## 检查对象与结论

- 日期：2026-09-20。
- 仓库：Antigravity-Manager。
- 分支：`fix/memory-leaks`。
- 被检查提交：`f33b1bdf4980e4f7b94db8322dcfa75152a75c52`。
- 对照基线：`main` / `0802afd045cfac4297fa46a16a30cec68dbb48fa`。
- 范围：10 个提交，8 个 Rust 文件及原实现计划；检查缓存、日志、代理池和调用路径。

**结论：部分修复有效，但存在未完成的内存修复及新增配置回归，暂不满足合并条件。** 新增测试通过不能证明实际保留内存有界，也没有覆盖再次初始化后的配置共享。

配套文档：[修复规格](../specs/2026-09-20-memory-leak-remediation-spec.md)、[开发计划](../plans/2026-09-20-memory-leak-remediation-plan.md)、[执行提示词](../handoffs/2026-09-20-memory-leak-remediation-prompt.md)。本轮修复行为和验收以这份新规格为准；2026-09-18 文档保留作历史背景。

## 已确认问题

### R1 / P1：日志文本变短，底层分配仍保留

**位置：** `src-tauri/src/proxy/monitor.rs`，`truncate_string_safe`（检查时第 39–48 行）、`log_request` 的内存日志构造。

`log_request` 克隆完整日志，再调用 `String::truncate` 和 `push_str`。`truncate` 不缩减 capacity；当原正文很大时，缓存仍持有大块分配。默认条数从 1000 减为 100 有帮助，但单条 16 KiB 预览并未变成相应的内存上限。

从实际源文件提取截断函数，用 Rust 独立程序输入 10 MiB ASCII 文本，输出：

```text
truncated_body_len=16419, retained_capacity=10485760
```

**影响：** 两个正文各 10 MiB 的 100 条日志，仅正文分配就可约占 2 GiB。现有测试只断言字符串内容、长度和日志条数，无法识别该问题。

**修复目标：** 按截断后的前缀直接构造小字符串，避免为内存摘要先克隆完整正文；完整正文继续交给数据库持久化。验收检查 capacity 和队列正文容量总和。

### R2 / P1：再次初始化后代理池和服务不再共享配置

**位置：** `src-tauri/src/proxy/proxy_pool.rs::init_global_proxy_pool`（第 23–46 行）；`server.rs::AxumServer::start`、`update_proxy_pool`。

首次初始化时，服务和代理池持有同一个 `Arc<RwLock<ProxyPoolConfig>>`。再次初始化时，函数复用全局 manager，通过脱离调用方的任务把新配置值复制到旧 Arc；服务却保存新 Arc。之后热更新修改服务侧 Arc，manager 继续读取旧 Arc。初始化返回与异步配置复制之间也没有完成顺序保证。

以实际初始化函数配合最小配置/manager 外壳复现，省略网络和绑定持久化：

```text
after_restart_hot_update: server_enabled=true, manager_enabled=false, shared_config=false
```

**触发边界：** 同一进程再次调用初始化函数或重新创建 Axum 服务。`ensure_admin_server` 在已有管理服务器时直接返回，`stop_proxy_service` 仅逻辑停用代理；普通界面启停不一定重新初始化。进程退出后重开也不会保留旧 OnceLock。首次服务启动在初始化代理池之后失败、随后重试，是需要覆盖的路径。

**影响：** 代理池开关、代理列表、账号绑定及健康检查配置与服务显示的状态可能不一致。

**修复目标：** 初始化等待配置及绑定同步完成；服务状态、请求状态和 manager 使用同一规范配置 Arc；随后热更新立即可见。保持管理服务器现有生命周期语义。

### R3 / P2：容量限制已补上，访问顺序淘汰仍未实现

**位置：** `signature_cache.rs::evict_oldest_if_needed` 及各 getter；`cache_manager.rs::evict_oldest_dashmap`、`lookup_si`、`lookup_tools`、`lookup_prefix`。

淘汰使用写入 timestamp 或 created_at，读取命中不刷新淘汰依据。缓存条目总数得到限制，但行为仍按写入时间排序，无法保留最近访问的热门条目。

实际 SignatureCache 源码的独立复现：先写入 hot，再写入 499 个 cold，读取 hot，最后写入 new，输出：

```text
recently_read_tool_retained=false
```

**影响：** 活跃工具签名、系统提示词、工具 schema 和前缀条目可能被优先挤出，引起缓存抖动、签名恢复命中率下降。没有证明每次缺失都会导致请求失败。

**修复目标：** 独立记录最近访问时间，成功读取和有效写入更新该时间；过期判断继续使用现有 TTL 字段。补充“读取旧条目后超限”的测试。

### R4 / P2：单会话签名历史仍能持续增长

**位置：** `signature_cache.rs::cache_session_signature`，`session_signatures` 的嵌套 `HashMap<usize, SessionSignatureEntry>`。

当前 1000 条限制约束外层会话数。单个会话每个新 message_count 都能增加一条签名，并刷新外层 TTL；内部历史没有条数或字节预算。该问题属于既有风险，本分支的外层容量修复没有覆盖它。

从实际源码构造单会话、连续递增 message_count 的复现：

```text
session_count=1, signatures_in_one_session=5000
```

**影响：** 长时间活跃会话可积累历史，外层容量和 TTL 不会阻止这种增长。

**修复目标：** 设定每会话历史条数和保留签名字节预算，优先保留最新 message_count；签名必须完整保存，超过预算的历史按缓存缺失处理。保持 rewind 和同轮签名更新规则。

## 已有修复的有效部分

| 模块 | 检查结果与边界 |
|---|---|
| WebSocket 工具调用缓存 | 增加了 500 条容量、30 分钟 TTL 及读取刷新；现有相关测试通过。条数上限不等于任意 Value 的总字节上限。 |
| session_accounts | 增加最后访问时间和后台过期清理；清理测试通过。TTL 不能提供任意请求速率下的严格容量上限。 |
| SignatureCache / CacheManager | 外层条目数淘汰已补齐；仍需 R3、R4。 |
| ProxyPoolManager | 重复启动会中止旧任务，Weak 减少强引用保留；需修复 R2。 |
| Vision MCP | 增加 100 条容量与访问时过期判断；生命周期及容量测试通过。 |
| 日志队列 | 默认 100 条已生效；正文真实内存约束需修复 R1。 |

健康检查停止方法当前主要由测试调用。由于管理服务器在代理逻辑停用后继续存在，不能仅凭这一点断言普通停用时必须销毁 manager；本次要求验证任务唯一性并保留现有管理功能。

## 测试证据与局限

执行命令（目录 `src-tauri`）：

```bash
cargo test --lib proxy:: --no-fail-fast --offline
```

- 编译完成；572 通过，18 失败，47 filtered out，退出码 101。
- 在上述结果中，6 个直接相关模块测试及会话清理测试共 49 项，全部通过。
- 18 项失败涉及 Claude 请求映射、错误分类、工具结果压缩、OpenCode 目录及安全数据库相关测试。
- 尚未在 main 上运行同条件对照，因此未把这 18 项归因为本分支回归或基线问题。
- 原始输出在检查机器 `/tmp/antigravity-memory-review-tests.log`；这是临时证据，不是其他 agent 的前置依赖。
- R1、R3、R4 使用提取的实际函数/模块，日志宏被替换为空操作；R2 使用实际初始化函数和简化外壳。它们是针对根因的最小复现，执行 agent 必须补入仓库内回归测试。
- 未进行应用进程长期 RSS 压测；本报告不声称所有内存增长来源已排除。

## 合并条件

完成 R1–R4 的规格验收，相关回归测试通过，并对整体测试失败完成基线对照。给出实际命令、退出码、失败归因和剩余限制后，再判断是否可合并。
