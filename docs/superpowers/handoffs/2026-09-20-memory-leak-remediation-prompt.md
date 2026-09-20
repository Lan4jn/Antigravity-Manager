请在 Antigravity-Manager 仓库中执行内存修复补全任务，完成代码修改、回归测试和验证报告。

工作目录：`/Users/mewcapoo/Project/Antigravity-Manager`。
目标分支：`fix/memory-leaks`，已检查提交为 `f33b1bdf4980e4f7b94db8322dcfa75152a75c52`。先检查当前 HEAD 与工作区；若已有后续改动，保留它们并核实哪些问题仍存在。

依次阅读：
1. `docs/superpowers/reviews/2026-09-20-memory-leak-review.md`
2. `docs/superpowers/specs/2026-09-20-memory-leak-remediation-spec.md`
3. `docs/superpowers/plans/2026-09-20-memory-leak-remediation-plan.md`

以新 spec 的 G1–G4 和 A1–A11 为开发目标与验收依据，按开发计划顺序执行。需要完成：
- 内存日志按 UTF-8 安全前缀直接生成小容量正文，验证 capacity 和队列正文分配总和，数据库保留完整内容。
- 代理池异步初始化返回前完成配置与绑定同步；AxumServer、AppState 和 manager 共享同一配置 Arc；再次初始化后的热更新立即生效。保持 Admin Server 的逻辑启停语义。
- SignatureCache 四层、CacheManager 三层使用独立最近访问字段实现 LRU；读取不延长 TTL。
- 单会话签名历史最多 256 条，保留签名 capacity 总和最多 1 MiB；优先保留最近轮次，完整保存签名，保持 rewind、同轮更新及缓存缺失行为。

先补能暴露原问题的失败测试，再做最小修复。沿用现有模块和依赖。遵守仓库 AGENTS.md；只有存在 `.codegraph/` 时才使用 CodeGraph。不要覆盖其他人的改动。发现与当前代码不符的细节时，先核实调用链并在结果中记录必要调整。

在隔离 ABV_DATA_DIR 下验证，处理全局 OnceLock 和环境变量导致的测试干扰。原检查中相关 49 项测试通过，但代理模块整体为 572 通过、18 失败；这些数字是历史证据，不能直接作为本轮结果。必须与 `0802afd045cfac4297fa46a16a30cec68dbb48fa` 基线使用同条件对照，修复新增失败并说明既有失败，不能仅凭失败位于其他模块就判定无关。

持续执行至验收完成。最终更新计划复选框，并创建 `docs/superpowers/reviews/2026-09-20-memory-leak-remediation-results.md`，记录 A1–A11 对应证据、命令退出码、内存容量数值、基线失败归因和剩余限制。最终回复给出修改摘要、测试结果以及是否满足合并条件；如果有未完成的验收，明确说明。此次交接要求本地实现与验证，推送或合并需另行授权。
