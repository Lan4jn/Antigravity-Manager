# Hermes 请求 429/503 完整排查报告

**日期：** 2026-09-11

**项目：** `Antigravity-Manager`

**检查版本：** `4.6.9`，仓库 HEAD `3d42ac0`

## 结论

根因在 `Antigravity-Manager` 的 OpenAI 请求转换层，不在 Hermes。

Google Antigravity 上游会依据 system instruction 的 Agent 身份文本分类请求。Manager 已对 Claude Agent SDK 身份做归一化，但 OpenAI mapper 没有处理 Hermes 的规范身份前缀：

```text
You are Hermes Agent, an intelligent AI assistant created by Nous Research.
```

该身份原样发送给上游时，两个配额均为 96% 的有效账号都收到 `RESOURCE_EXHAUSTED`，Manager 最终返回：

```text
429 All accounts exhausted
503 Token error: All accounts limited
```

仅将上述规范前缀替换为 Antigravity 官方身份，保留其余 29KB Hermes 系统提示和 20 个工具定义后，请求返回 `200`，模型正常产生 `session_search` 工具调用。

## 用户症状

Hermes QQBot 返回：

```text
The model provider failed after retries. I kept raw provider details out of chat;
check gateway logs for diagnostics.
```

130 Gateway 日志显示：

```text
provider=custom
base_url=http://server.model.dpdns.org:8045/v1
model=gemini-3.8-flash-high
HTTP 429: All accounts exhausted
HTTP 503: Token error: All accounts limited. Wait 20-29s
```

## 环境核对

- Manager `/health`：`4.6.9`。
- Manager active accounts：2。
- 两个账号：enabled，未 validation block，未 quota protection。
- 两个账号的 `gemini-3.8-flash-high` 配额：96%。
- 调度模式：`Balance`，最大等待 60 秒。
- Hermes 模型名：`gemini-3.8-flash-high`，无终端转义字符。
- Hermes 与 curl 使用的 API key SHA-256 相同。
- `/v1/models` 返回 200，目标模型存在。

## 排除项

### 不是模型不存在

`/v1/models` 返回 94 个模型，包含 `gemini-3.8-flash-high`。

### 不是 API key 错误

Hermes 配置密钥和 custom provider `key_env` 密钥哈希相同；简短请求返回 200。

### 不是 Responses API 独有问题

`/v1/responses` 会失败；切换 `/v1/chat/completions` 后，Hermes 完整请求仍失败。

### 不是 stream 参数

简短的 `stream=false` 和 `stream=true` 请求均返回 200。

### 不是 tools 字段

约 60KB、包含完整 20 个 Hermes tools 的简短请求返回 200；完整 Hermes system prompt 在无 tools 时仍返回 429。

### 不是真实日配额耗尽

两个账号目标模型配额均为 96%。清除 Manager 本地限流后，Hermes 身份请求立即复现 429。

### 不是 sticky session 永久损坏

相同首条 user 文本 `hello` 的简短请求返回 200；加入 Hermes 身份后返回 429。

## 决定性实验矩阵

每组身份实验前调用 Manager 自带接口清除本地限流，避免前一请求污染后一请求：

```text
DELETE /api/proxy/rate-limits -> 200
```

| 请求 | 结果 |
|---|---|
| 中性 system：`You are a helpful assistant.` | 200 |
| 仅 Hermes 规范身份 | 429 |
| 完整 Hermes system，无 tools | 429 |
| 完整 Hermes system，含 20 tools | 429 |
| 在 Hermes 身份前追加 Antigravity 身份 | 429 |
| 替换 Hermes 身份前缀，保留其余提示和 tools | 200 + tool call |

追加身份无效，证明上游不仅查看第一段身份，还会识别冲突身份。修复必须替换规范前缀，不能简单 prepend。

## 代码路径

请求入口：

```text
src-tauri/src/proxy/server.rs
  /v1/chat/completions -> handle_chat_completions
  /v1/responses -> handle_completions
```

OpenAI system message 收集：

```text
src-tauri/src/proxy/mappers/openai/request.rs:357-407
```

当前流程只做 cache sanitize 和 dedupe，未做客户端身份归一化。

官方身份 fallback：

```text
src-tauri/src/proxy/mappers/openai/request.rs:1299-1341
```

fallback 仅在没有 system/developer instructions 时生效。Hermes 自带 system prompt，因此不会使用 fallback。

已有同类正确实现：

```text
src-tauri/src/proxy/mappers/claude/request.rs:11-27
```

`normalize_claude_client_identity()` 将 Claude Agent SDK 的独立身份归一化为 Claude Code 身份，以避免同类 `RESOURCE_EXHAUSTED`。

## 修复边界

应在 OpenAI mapper 内、cache key 计算之前处理身份。只匹配规范 Hermes system prompt 的开头，并只替换该前缀；后续 Hermes 指令、用户 system prompt、工具、消息历史全部保留。

不应：

- 修改 Hermes 源码或系统提示。
- 删除或压缩 Hermes tools。
- 对所有 system prompt 强制覆盖。
- 用随机字符绕过分类，这会破坏 prompt cache。
- 修改账号池限流语义来掩盖上游身份分类。

## 风险

- 匹配过宽会修改用户自定义提示。
- 在 cache sanitize 后修改会产生旧 cache 命中，导致修复不稳定。
- 只修 chat path 会遗漏 Responses path；两者最终都必须经过共享 `transform_openai_request_with_session()`。
- 追加而非替换会保留冲突身份，已验证仍失败。

## 开发目标

在不修改 Hermes、不减少其能力、不破坏 prompt cache 的前提下，让 `Antigravity-Manager` 将 Hermes 的规范 Agent 身份归一化为上游支持的 Antigravity 身份，使 `/v1/chat/completions` 与 `/v1/responses` 的完整 Hermes 请求稳定成功，并保持所有非 Hermes system prompt 字节不变。
