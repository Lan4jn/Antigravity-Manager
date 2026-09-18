# Hermes 身份归一化与 429/503 排查报告

**更新日期：** 2026-09-12（保留原文件名及链接）
**状态：** 身份补丁已实现；用户更新容器后仍报告 429，生产故障尚未闭环。
**基线：** 官方上游 734e2bde5d60cd7ecffe2467b5cab9cb154a3f5e / 4.7.1；当前合并提交 1ae1fa5，另含本地未提交的 Hermes/Docker 修改。

## 1. 当前结论

2026-09-11 的对照实验支持：规范 Hermes 身份是当时失败请求的一个触发因素，精确替换它曾让完整提示及工具请求返回 200。这不能证明 Google 内部分类规则，也不能证明所有 RESOURCE_EXHAUSTED 均由身份导致。

2026-09-12 用户确认更新容器后仍收到 All accounts exhausted，末次上游错误为 HTTP 429、RESOURCE_EXHAUSTED。尚未取得该次部署的镜像 ID、转换结果、错误详情和实时配额，不能宣称生产故障已修复，也不能仅凭该错误断言日配额耗尽。

## 2. 上游同步记录

- fork origin：https://github.com/Lan4jn/Antigravity-Manager.git，main 仍为 3d42ac0。
- 官方 upstream：https://github.com/lbjlaq/Antigravity-Manager.git，main 为 734e2bd。
- 直连 fetch 失败后，通过下载代理获取 Git 对象，与 GitHub 官方 API 返回的完整 SHA 核对一致后合并。
- 已合入 codex/hermes-identity-normalization 分支，未推送远端，也未重置本地 main。
- 合并前修改已通过命名 stash 备份并无冲突恢复；备份名包含 before upstream 734e2bd sync。

| 官方更新 | 对排查的影响 |
|---|---|
| a620ec2：配额多端点回退、project_id 缓存、bucket 合并 | 面板百分比不能单独证明实际可用性，需要刷新并取证 |
| 734e2bd：细分 429 | 部分分类路径区分通用资源耗尽、显式 quota/reset 与 model capacity |
| 734e2bd：动态 requestType | 纯文本不再统一标记 agent；带 tools 或工具历史才标记 agent，旧矩阵需重跑 |
| 734e2bd：会话隔离、超长会话恢复 | sessionId 混入对话指纹和代数，不再只是每账号固定会话 |
| 工具参数修复、Compose 日志轮转 | 保留上游行为，身份补丁不能覆盖这些更新 |

注意：handlers/common.rs 的 determine_retry_strategy() 仍包含 RESOURCE_EXHAUSTED 快速轮换分支。分类和重试不是同一函数，不能因分类 helper 更新就宣称整条链路已消除误判。

## 3. 历史证据：2026-09-11 / 4.6.9

当时代码为 3d42ac0，Hermes custom provider 地址为 http://server.model.dpdns.org:8045/v1，模型为 gemini-3.8-flash-high。记录了两个启用账号，目标配额显示 96%，无 validation block 或 quota protection；模型列表成功、密钥哈希比对一致。以上均为历史状态，不代表当前部署。

每组历史身份实验前清除了 Manager 本地限流，不能直接与当前未清理状态混比。

| 请求 | 当时结果 |
|---|---|
| 中性简短 system | 200 |
| 仅规范 Hermes 身份 | 429 |
| 完整 Hermes system，无 tools | 429 |
| 完整 Hermes system，含 20 tools | 429 |
| Hermes 身份前追加 Antigravity 身份 | 429 |
| 替换规范身份，保留约 29 KB 后缀及 20 tools | 200，产生 session_search 工具调用 |

同次记录中，简短请求流式/非流式均成功，含完整 tools 的简短请求成功；Chat 和 Responses 的完整 Hermes 请求均曾失败。这缩小了当时范围，不是永久排除鉴权、模型、工具、配额或传输问题。

## 4. 当前代码路径

以下路径均相对于 src-tauri/src：

1. proxy/server.rs：/v1/chat/completions → handle_chat_completions()；/v1/responses、/responses → handle_completions()。
2. proxy/handlers/openai.rs：Responses input/instructions 转入 OpenAI mapper；部分分支先把 instructions 转为 system message。
3. proxy/mappers/openai/request.rs 的 transform_openai_request_with_session() 收集 system/developer 文本及独立 instructions。
4. normalize_openai_client_identity() 使用 strip_prefix 精确匹配，返回 Cow。
5. 归一化先于 compute_si_key、L1 lookup 和 sanitize_system_instruction_for_cache，再执行既有去重、context_blocks 分段、工具映射和 prefix hash 计算。

规范前缀：

    You are Hermes Agent, an intelligent AI assistant created by Nous Research.

替换身份：

    You are Antigravity, a powerful agentic AI coding assistant designed by the Google Deepmind team working on Advanced Agentic Coding.

不忽略前导空格、换行、BOM、大小写或缺失句号；user/assistant/tool 不参与归一化。后缀再次引用 Hermes 必须保留，不能全局删除。

“逐字保留后缀”是身份函数契约。既有 sanitize/context_blocks 仍可能清洗动态字段、折叠空行和分类输出。端到端应与“仅手动替换前缀后经过相同 mapper”的结果比较，不能要求上游 JSON 等于原 OpenAI JSON。

## 5. 验证证据及适用范围

旧构建证据见本地 dist/docker/build-info.json、build.log、test.log（产物可能未入库）：

- 基线 d509491 加本地修改，版本 4.6.9。
- cargo test --release --lib hermes -- --test-threads=1：8 passed，0 failed。
- 统一镜像 antigravity-manager:hermes-20260912，linux/amd64。
- 镜像 ID：sha256:b7e6549721e28e902aae5701e00f34e7d0c95fdad8e88c402f782db24647f0c4。
- 归档 SHA-256：8660d15ae51b2bb3b6adca5cfd19fe76814e8c3dede451e9088f76c157a702d3。
- 容器 health、HTML、主 JS 验证通过；没有真实上游验收证据。

这些结果不适用于刚合并的 4.7.1。现存镜像及 dist 仍是旧产物，必须重新测试和构建新标签。用户确认已更新是有效反馈，但仍需镜像 ID 关联具体部署。

## 6. 当前 429 的复查顺序

1. 确认失败地址、容器、镜像 ID、模型映射及协议，避免测试到另一实例。
2. 核查 instruction 来源、命中数量、长度、归一化前后摘要，不输出完整提示或凭据。
3. 记录 Retry-After、details.reason、quotaResetDelay、时间窗口、匿名账号及实时配额；原始错误留存前脱敏。
4. 同模型、协议、tools/history/参数下只改变身份：中性、Hermes、Antigravity。另做无工具/有工具两组，避免动态 requestType 混淆结论。
5. 尊重冷却时间，优先隔离实例或等待到期，清空生产限流不是常规修复。
6. 分别验证 Chat/Responses 流式、非流式、工具调用和第二轮上下文；真实配额不足仍应失败。

中性/Antigravity 也失败时转查账号、速率、容量、项目和会话；仅 Hermes 失败时确认命中与其他指令残留。单次 200 或 health 成功不能关闭生产故障。

## 7. 后续交付

按 [Spec](../specs/2026-09-11-hermes-identity-normalization-spec.md) 和 [计划](../plans/2026-09-11-hermes-identity-normalization-plan.md) 完成合并后回归、前后端统一镜像重建和生产验收。更新必须保留旧镜像、数据备份和原配置，并提供回滚步骤。
