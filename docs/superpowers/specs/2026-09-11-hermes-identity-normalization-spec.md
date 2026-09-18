# Hermes 身份归一化与统一 Docker 交付 Spec

**更新日期：** 2026-09-12
**基线：** 官方上游 734e2bd / 4.7.1，加当前本地身份及 Docker 补丁。
**状态：** 用于合并后验证与生产闭环，不声明当前 429 已解决。

## 1. 目标和范围

在共享 OpenAI 转换层精确替换 Hermes 官方身份，保留后缀及原有提示、工具、历史和缓存转换语义。支持 Chat Completions、Responses 流式/非流式调用，交付单个提供 Web UI 与 API 的 Docker 镜像。

本功能不承诺绕过真实 RPM/TPM、日配额或容量限制，不修改 Hermes 源码，不增加身份配置开关。保留上游 4.7.1 的配额刷新、限流分类、动态 requestType、会话隔离和工具参数行为；额外 bug 必须先复现再单独处理。

## 2. 精确替换契约

Hermes 完整前缀：

    You are Hermes Agent, an intelligent AI assistant created by Nous Research.

替换身份：

    You are Antigravity, a powerful agentic AI coding assistant designed by the Google Deepmind team working on Advanced Agentic Coding.

- 仅匹配单条已收集 instruction 字节零位置的完整前缀，区分大小写和句号。
- 不 trim，不做子串或模糊匹配。前导空白/BOM、中间引用、近似拼写不命中。
- 输出严格等于“替换身份 + 原后缀”。Unicode、CRLF、空行及后缀再次出现 Hermes 句子均逐字保留。
- 未命中返回 Cow::Borrowed，不为身份判断分配新 String。已归一化文本再次执行结果不变。
- 作用域仅为 system/developer 收集结果与 Responses instructions。user/assistant/tool 不参与身份归一化。
- 文本块先按既有规则连接，再判断完整 instruction；不对每个块独立做替换。

## 3. 共享入口和缓存

唯一接入点为 transform_openai_request_with_session() 中 L1 cache map：

    收集 instructions → normalize_openai_client_identity
    → compute_si_key → lookup_si / sanitize → dedupe
    → context_blocks / tools → compute_prefix_hash

覆盖 /v1/chat/completions、/v1/responses、/responses 的实际 handler 路径，不在 HTTP handler 复制身份逻辑。禁止为该补丁清空缓存、注入随机内容或改变会话绑定。

相同请求的 normalized text、SI key、systemInstruction、tools 与实际 prefix hash 应相同。随机 requestId 不属于稳定性断言；会话代数改变也不能误判为身份缓存不稳定。

## 4. 内容兼容与上游行为

身份函数逐字保留后缀；完整 mapper 继续运行动态清洗、分段和工具 schema 规范化。以“同一请求只手动替换前缀后经过相同 mapper”为 oracle，比较 systemInstruction、tools、toolConfig、generationConfig 和 contents；排除原本随机的外层 requestId。

至少覆盖 20 个合成工具的名称、参数、描述，覆盖 tool_choice、thinking、媒体、工具返回和多轮历史。非 Hermes 请求与未增加身份补丁的基线一致，不能承诺最终输出与原始 OpenAI 文本逐字相同。

必须保留上游新语义：

- 纯文本且无工具交互时，不强制注入 agent requestType。
- tools 或工具历史存在时，保留 agent 标记行为。
- sessionId 遵循账号、对话指纹及代数隔离。
- 通用 RESOURCE_EXHAUSTED、显式 QUOTA_EXHAUSTED、RATE_LIMIT_EXCEEDED、MODEL_CAPACITY_EXHAUSTED 按实际分类与重试链路验证；只验证 helper 不足以证明整体行为。

## 5. 验收矩阵

| 层级 | 覆盖 | 通过条件 |
|---|---|---|
| 纯函数 | 完整前缀、空后缀、Unicode/CRLF、后缀引用、近似文本、BOM/空白、幂等 | 严格字节等式，未命中借用 |
| mapper | system、developer、文本块、独立 instructions | 与手工归一化 oracle 一致，user/assistant/tool 未改写 |
| 内容与缓存 | 20 tools、tool_choice/thinking、媒体/历史、重复转换 | 语义保留，SI key 与实际 prefix hash 稳定 |
| HTTP handler | Chat、两条 Responses 路由；stream true/false | mock 上游捕获身份正确，响应/SSE 与工具往返正常 |
| 上游回归 | requestType、会话、限流分类与重试 | 保留 4.7.1 基线行为，无身份补丁引入的偏差 |
| 统一容器 | Linux 二进制、HTML、JS、health | 单镜像正常启动，静态资源与本次前端构建一致 |
| 生产 | 合成对照矩阵、完整 Hermes tools、两轮上下文 | 合理配额/容量条件下两协议成功，失败有可解释证据 |

中间引用必须保留，但不要求其真实上游必然返回 200。模拟配额耗尽检查错误传播，不为验收故意耗尽生产账号。

## 6. Docker 交付

- 默认用 docker/Dockerfile 构建当前前端和当前后端；最终镜像包含 /app/dist 与 /app/antigravity-tools，使用 --headless。
- backend.localdist 可复用本次源码重新编译的 dist。backend 复用旧镜像前端仅适用于已验证兼容的场景，不能作为完整前后端更新默认方式。
- Cargo 二进制名为 antigravity-tools；当前 Rust 基础镜像 CARGO_HOME=/usr/local/cargo，镜像源与缓存路径须一致。
- 镜像标签包含版本及构建标识；记录源码 SHA、未提交修改状态、image ID、平台、归档 SHA-256 与测试证据。
- 合并后版本为 4.7.1；旧 hermes-20260912 / 4.6.9 镜像不是新交付。版本号不能替代 image ID 核验。
- 沿用原 Compose 文件、项目名、网络、密钥及数据挂载。默认配置为 host 网络、ABV_BIND_LOCAL_ONLY=true；不能把示例 API_KEY=test 带入生产。
- 保留上游日志轮转。更新前保存旧镜像、配置和一致数据备份；更新后验证 health 与页面资源，失败可切回旧镜像。
- 外挂 /app/dist 会遮住镜像前端，更新前必须核查。镜像归档支持 docker load，目标机不需要 Rust/Node。

## 7. 安全和完成条件

诊断只记录协议、模型、命中数量、长度、摘要、匿名账号、错误 reason 和等待时间。不得新增完整提示、API key、token 或历史消息日志；需要留存的原始错误须脱敏。

所有层级验收完成且用户生产反馈闭环后才能标记问题解决。当前旧 4.6.9 的 8 项 Hermes 测试与容器验证只作历史证据；4.7.1 合并后的测试、镜像重建与生产矩阵均待执行。

参考：[报告](../reviews/2026-09-11-hermes-identity-investigation.md)、[计划](../plans/2026-09-11-hermes-identity-normalization-plan.md)。
