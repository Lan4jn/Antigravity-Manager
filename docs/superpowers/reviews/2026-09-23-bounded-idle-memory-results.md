# 空闲内存保留修复结果

基线：`1af95c8`。开发分支：`codex/bounded-idle-memory`。

## 开发目标与实现

1. **Responses 历史有界并可过期释放**：全局存活节点估算 64 MiB、单链 16 MiB、索引及链深度各 1000。Weak 注册表避免共享祖先重复计数，并计入在途父引用。超预算拒绝存储或淘汰整个索引记录，不截断历史；缺失 previous_response_id 明确返回 HTTP 400。读取先检查 TTL。
2. **全局缓存限制字节容量**：签名四层合计 32 MiB（2/2/24/4），SI/tools 各 8 MiB。记录按 LRU 整条淘汰，超大单条不缓存。已有单会话 256 条/1 MiB、签名完整性、TTL 语义保留。
3. **限制日志持有与积压**：完整日志流水线最多 4 条，读取请求体前准入，直到数据库工作结束才释放名额。取消请求不会让仍运行的数据库任务提前归还名额。完整正文直接移动给写入任务，不再克隆。内存 error 字段同样使用 16 KiB 预览，数据库完整错误不变。
4. **空闲清理**：接入 TokenManager 现有 60 秒、可取消的清理循环，同时处理三个全局缓存，不引入新的永久任务。TTL 未缩短。
5. **关闭日志仍保留用量统计**：不构建完整响应日志；serde 跳过输出字段，提取 OpenAI/Responses/Anthropic/Gemini 用量及输入估算。大单事件可短暂落盘，事件结束即关闭临时文件。

## 验证证据

- 修复前：3 个缓存预算测试失败，`/tmp/idle-memory-red.log`。
- 修复前：过期读取、索引容量及超大历史 3 个新测试失败，`/tmp/session-memory-red.log`。
- 修复前：1 MiB error 仍完整留在内存缓存的回归断言失败，`/tmp/idle-memory-error-red.log`。
- 会话测试 11/11、真实 missing previous_response_id handler 测试 1/1 通过，见 `/tmp/session-memory-green.log` 与 `/tmp/session-handler-green.log`。
- 完整 proxy 套件：634 通过、8 失败（退出码 101）；失败名单与基线审查记录相同。见 `/tmp/idle-memory-verified-proxy.log`。不将历史 8 项失败归类为本次修复要求。
- 新测试覆盖全局字节容量、空闲清理、祖先 Weak 释放、活跃引用无法绕过预算、完整历史物化、客户端转发、读取正文前等待准入、拆分 SSE 用量，以及大于 2 MiB 的 response.completed 用量。
- 最后 `cargo check --manifest-path src-tauri/Cargo.toml --offline` 通过（退出码 0），日志 `/tmp/idle-memory-verified-check.log`；末次过期优先淘汰调整后，`cargo test --manifest-path src-tauri/Cargo.toml --lib proxy::cache_manager::tests --offline` 默认并发复测 31/31 通过（退出码 0），日志 `/tmp/idle-memory-cache-final.log`。
- 全量命令为 `cargo test --manifest-path src-tauri/Cargo.toml --lib proxy:: --offline -- --test-threads=1`；数据库相关运行使用独立临时 `ABV_DATA_DIR`。编译仍有项目既有警告，不宣称零警告。

## 实际限制

缓存预算约束存储数据而非整个进程 RSS；Responses 使用堆容量估算，JSON 容器和分配器存在额外成本。DashMap 并发插入过程中可短暂超预算，写入结束后收敛。4 条日志准入会使更多并发请求排队。

缓存/历史淘汰可能使续接请求需要重新提交完整输入。Responses 单次保存超预算时当前响应仍正常交付，后续续接明确返回不可用错误。单个用量事件超过 100 MiB 监控上限或临时文件 I/O 失败时可能无法统计该事件；客户端字节继续转发。

没有连接用户的生产 Docker 容器，没有构建 Linux 镜像或测得部署后的 RSS 变化。需部署此版本后确认用户场景的改善程度，不能宣称已解释或消除了全部 1 GiB 占用。
