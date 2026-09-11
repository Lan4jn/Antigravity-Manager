# Hermes 身份归一化实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框跟踪进度。

**目标：** 在 OpenAI mapper 共享路径精确归一化 Hermes 官方身份，使完整 Hermes chat/Responses 请求通过 Antigravity 上游，同时完整保留其余提示和工具。

**架构：** 在 `request.rs` 增加一个纯函数，使用 `strip_prefix` 精确识别 Hermes 规范身份并用现有 Antigravity 身份替换。函数在 system instructions 进入 cache sanitize 前运行，因此 chat 和 Responses 入口共享修复，且 normalized prompt 保持稳定缓存前缀。

**技术栈：** Rust、serde_json、`std::borrow::Cow`、Cargo tests、Axum OpenAI mapper。

---

### 任务 1：建立身份归一化纯函数

**文件：**
- 修改：`src-tauri/src/proxy/mappers/openai/request.rs:1-30`
- 测试：`src-tauri/src/proxy/mappers/openai/request.rs` 内现有 `#[cfg(test)]` 模块

- [ ] **步骤 1：编写失败测试**

在现有 test module 增加：

```rust
#[test]
fn hermes_identity_prefix_is_normalized_without_losing_remainder() {
    let remainder = "\n\nKeep every Hermes capability and tool instruction.";
    let input = format!("{HERMES_AGENT_IDENTITY_PREFIX}{remainder}");

    let normalized = normalize_openai_client_identity(&input);

    assert!(normalized.starts_with(ANTIGRAVITY_AGENT_IDENTITY));
    assert!(normalized.ends_with(remainder));
    assert!(!normalized.contains(HERMES_AGENT_IDENTITY_PREFIX));
}

#[test]
fn hermes_identity_mention_is_not_rewritten() {
    let input = format!(
        "Compatibility note: {HERMES_AGENT_IDENTITY_PREFIX}"
    );

    assert_eq!(normalize_openai_client_identity(&input), input);
}

#[test]
fn unrelated_identity_is_borrowed_unchanged() {
    let input = "You are a helpful assistant.";

    assert!(matches!(
        normalize_openai_client_identity(input),
        std::borrow::Cow::Borrowed(_)
    ));
}
```

- [ ] **步骤 2：运行红灯**

```powershell
Set-Location F:\Documents\Project\Antigravity-Manager\src-tauri
cargo test hermes_identity_prefix_is_normalized_without_losing_remainder --lib
```

预期：编译失败，因为常量和函数尚不存在。

- [ ] **步骤 3：实现最小纯函数**

在 imports 增加：

```rust
use std::borrow::Cow;
```

在模块级增加：

```rust
const HERMES_AGENT_IDENTITY_PREFIX: &str =
    "You are Hermes Agent, an intelligent AI assistant created by Nous Research.";
const ANTIGRAVITY_AGENT_IDENTITY: &str =
    "You are Antigravity, a powerful agentic AI coding assistant designed by the Google Deepmind team working on Advanced Agentic Coding.";

fn normalize_openai_client_identity(text: &str) -> Cow<'_, str> {
    match text.strip_prefix(HERMES_AGENT_IDENTITY_PREFIX) {
        Some(remainder) => Cow::Owned(format!(
            "{ANTIGRAVITY_AGENT_IDENTITY}{remainder}"
        )),
        None => Cow::Borrowed(text),
    }
}
```

- [ ] **步骤 4：运行纯函数测试**

```powershell
cargo test hermes_identity --lib
```

预期：三个测试通过。

- [ ] **步骤 5：提交**

```powershell
git add src-tauri/src/proxy/mappers/openai/request.rs
git commit -m "fix(openai): normalize Hermes agent identity"
```

---

### 任务 2：接入共享 OpenAI system instruction 流程

**文件：**
- 修改：`src-tauri/src/proxy/mappers/openai/request.rs:357-407`
- 测试：`src-tauri/src/proxy/mappers/openai/request.rs`

- [ ] **步骤 1：编写转换层失败测试**

在现有 OpenAI mapper tests 增加：

```rust
#[test]
fn transform_normalizes_hermes_identity_and_preserves_tools() {
    let remainder = "\n\nHermes-specific instructions remain intact.";
    let request: OpenAIRequest = serde_json::from_value(json!({
        "model": "gemini-3.8-flash-high",
        "messages": [
            {
                "role": "system",
                "content": format!(
                    "{HERMES_AGENT_IDENTITY_PREFIX}{remainder}"
                )
            },
            {"role": "user", "content": "hello"}
        ],
        "tools": [{
            "type": "function",
            "function": {
                "name": "session_search",
                "description": "Search sessions",
                "parameters": {"type": "object", "properties": {}}
            }
        }]
    }))
    .unwrap();

    let (body, _, _, _) = transform_openai_request(
        &request,
        "test-project",
        "gemini-3.8-flash-high",
        None,
    );
    let system = body["request"]["systemInstruction"]["parts"][0]["text"]
        .as_str()
        .unwrap();

    assert!(system.contains(ANTIGRAVITY_AGENT_IDENTITY));
    assert!(system.contains("Hermes-specific instructions remain intact."));
    assert!(!system.contains(HERMES_AGENT_IDENTITY_PREFIX));
    assert_eq!(
        body["request"]["tools"][0]["functionDeclarations"][0]["name"],
        "session_search"
    );
}
```

- [ ] **步骤 2：运行红灯**

```powershell
cargo test transform_normalizes_hermes_identity_and_preserves_tools --lib
```

预期：失败，输出仍包含 Hermes identity。

- [ ] **步骤 3：在 cache 前接入归一化**

修改 `system_instructions` 的 map，在 `compute_si_key()` 之前处理：

```rust
system_instructions = system_instructions
    .into_iter()
    .map(|instruction| {
        normalize_openai_client_identity(&instruction).into_owned()
    })
    .collect();
```

该代码必须位于现有 cache sanitize map 之前。不要修改 `sanitize_system_instruction_for_cache()`，因为身份归一化不是动态字段清洗。

- [ ] **步骤 4：运行 mapper 测试**

```powershell
cargo test transform_normalizes_hermes_identity_and_preserves_tools --lib
cargo test proxy::mappers::openai::request --lib
```

预期：全部通过。

- [ ] **步骤 5：提交**

```powershell
git add src-tauri/src/proxy/mappers/openai/request.rs
git commit -m "fix(openai): apply identity normalization before caching"
```

---

### 任务 3：覆盖 Responses 路径与缓存稳定性

**文件：**
- 修改：`src-tauri/src/proxy/mappers/openai/request.rs`，仅增加测试
- 测试：`src-tauri/src/proxy/mappers/openai/request.rs` 的现有 test module

- [ ] **步骤 1：增加 Responses 转换测试**

直接构造带 `instructions` 的 `OpenAIRequest`。Responses handler 已将
`instructions` 写入该字段，共享 mapper 会在 380-385 行把它放入
`system_instructions`：

```rust
#[test]
fn responses_hermes_instructions_reach_shared_normalizer() {
    let request: OpenAIRequest = serde_json::from_value(json!({
        "model": "gemini-3.8-flash-high",
        "instructions": format!(
            "{HERMES_AGENT_IDENTITY_PREFIX}\n\nKeep Hermes rules."
        ),
        "messages": [{"role": "user", "content": "hello"}],
        "tools": [{
            "type": "function",
            "function": {
                "name": "session_search",
                "description": "Search sessions",
                "parameters": {"type": "object", "properties": {}}
            }
        }]
    }))
    .unwrap();
    let (body, _, _, _) = transform_openai_request(
        &request,
        "test-project",
        "gemini-3.8-flash-high",
        None,
    );
    let system = body["request"]["systemInstruction"]["parts"][0]["text"]
        .as_str()
        .unwrap();

    assert!(system.contains(ANTIGRAVITY_AGENT_IDENTITY));
    assert!(system.contains("Keep Hermes rules."));
}
```

- [ ] **步骤 2：增加缓存稳定测试**

```rust
#[test]
fn normalized_hermes_prompt_is_byte_stable() {
    let prompt = format!(
        "{HERMES_AGENT_IDENTITY_PREFIX}\n\nStable Hermes rules."
    );
    let first = normalize_openai_client_identity(&prompt).into_owned();
    let second = normalize_openai_client_identity(&prompt).into_owned();

    assert_eq!(first, second);
    assert_eq!(
        CacheManager::compute_si_key(&first),
        CacheManager::compute_si_key(&second)
    );
}
```

- [ ] **步骤 3：运行 OpenAI/Responses 测试**

```powershell
cargo test responses_hermes --lib
cargo test normalized_hermes_prompt_is_byte_stable --lib
cargo test proxy::mappers::openai::request --lib
```

- [ ] **步骤 4：提交测试**

```powershell
git add src-tauri/src/proxy/mappers/openai/request.rs
git commit -m "test(openai): cover Hermes identity across protocols"
```

---

### 任务 4：完整验证

- [ ] **步骤 1：格式化与静态检查**

```powershell
Set-Location F:\Documents\Project\Antigravity-Manager\src-tauri
cargo fmt --check
cargo check --lib
```

- [ ] **步骤 2：运行相关完整测试**

```powershell
cargo test proxy::mappers::openai --lib
cargo test proxy::handlers::openai --lib
cargo test proxy::session_manager --lib
cargo test proxy::token_manager --lib
```

- [ ] **步骤 3：审查 diff**

```powershell
Set-Location F:\Documents\Project\Antigravity-Manager
git diff --check
git status --short --branch
git diff HEAD~3 -- src-tauri/src/proxy/mappers/openai/request.rs
```

必须确认：没有账号池、限流、模型映射、UI 或配置文件改动。

---

### 任务 5：部署与真实验收

- [ ] **步骤 1：构建与部署 Manager**

按该项目现有 backend/Docker 发布流程构建 4.6.9 后续版本。记录部署 SHA，保留当前服务回滚镜像或二进制。

- [ ] **步骤 2：健康检查**

```bash
curl http://server.model.dpdns.org:8045/health
```

预期返回新版本和 `status=ok`。

- [ ] **步骤 3：生产最小身份矩阵**

每个测试使用合成文本，不上传真实历史：

```text
neutral identity -> 200
Hermes canonical identity -> 200
Hermes identity mention in middle -> 200 and unchanged
full Hermes-shaped request with tools -> 200/tool call
```

- [ ] **步骤 4：Hermes 真实管线验收**

在 130 使用现有 custom provider，保持 Hermes 原始系统提示和工具：

```bash
hermes -z "Reply with exactly: HERMES_OK" \
  -m gemini-3.8-flash-high --provider custom --safe-mode
```

预期：返回 `HERMES_OK`，Gateway 日志不出现 `All accounts exhausted`。

- [ ] **步骤 5：QQBot 验收**

新建 QQ 会话并发送普通消息，再执行一个安全工具调用。预期文本回复和工具调用均成功，连续两轮保持上下文。

- [ ] **步骤 6：失败边界验收**

真实耗尽账号时仍应返回 429/503，不能由身份归一化清除真实限流记录或无限重试。

## 完成标准

- 三类单元测试全部通过。
- chat 和 Responses 共享归一化，无重复 handler 逻辑。
- 完整 Hermes prompt 和 tools 保留。
- normalized prefix byte-stable。
- 生产身份矩阵通过。
- 130 Hermes CLI 返回 `HERMES_OK`。
- QQBot 文本、工具和第二轮上下文通过。
- 非 Hermes 请求行为不变。
- 真实配额错误仍正确返回。
