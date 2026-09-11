# Hermes 身份归一化规格

## 开发目标

在 `Antigravity-Manager` 的 OpenAI 请求转换层识别 Hermes 官方 system prompt，并将其身份前缀替换为 Google Antigravity 支持的官方身份。除身份前缀外，Hermes 的系统指令、工具定义、消息历史和请求参数必须完整保留。

## 术语

- **Hermes Identity Prefix**：`You are Hermes Agent, an intelligent AI assistant created by Nous Research.`
- **Antigravity Identity**：OpenAI mapper 当前使用的 `fallback_antigravity_identity` 第一行身份。
- **Normalization**：只替换 system/developer instruction 开头的 Hermes Identity Prefix。
- **User Text**：role 为 user、assistant 或 tool 的内容，永不参与身份归一化。

## 功能需求

### FR-1 识别规范 Hermes 身份

当一个 OpenAI system 或 developer instruction 以完整 Hermes Identity Prefix 开头时，系统必须识别为 Hermes 客户端身份。

不得匹配：

- 中间或末尾提及 Hermes Identity Prefix 的文本。
- `Hermes Agent` 的模糊子串。
- 大小写不同、拼写不同或缺少完整句号的文本。
- user/assistant/tool 消息中的相同文本。

### FR-2 替换而非追加

系统必须删除且仅删除开头的 Hermes Identity Prefix，并在相同位置放入 Antigravity Identity。

不能仅在前面追加，因为实测同时存在两个身份仍返回 429。

### FR-3 保留剩余内容

替换后：

- 原身份后的所有字符逐字保留，包括换行。
- 所有其他 system/developer instruction 顺序不变。
- tools、tool choice、thinking config、history、media 和 user message 不变。

### FR-4 共享覆盖两个 OpenAI 入口

归一化必须位于 `transform_openai_request_with_session()` 的共享 system instruction 收集路径，使以下入口自动覆盖：

- `/v1/chat/completions`
- `/v1/responses`
- `/responses`

不得在 HTTP handler 中复制三份判断。

### FR-5 缓存稳定

归一化必须发生在 `CacheManager::compute_si_key()` 和 `sanitize_system_instruction_for_cache()` 之前。相同 Hermes prompt 每次必须产生字节相同的 normalized system instruction 和 prefix hash。

### FR-6 非 Hermes 请求无变化

所有未匹配 Hermes Identity Prefix 的 String 必须原值返回，不分配新的 String；可使用 `Cow<'_, str>` 达成。

## 安全要求

- 不记录完整 system prompt。
- 不在日志中输出 user content、API key、账号邮箱或 token。
- 测试只使用合成提示，不使用生产 request dump。
- 匹配必须精确且仅限 system/developer 层。

## 兼容性要求

- 不改变 Claude mapper 的 `normalize_claude_client_identity()`。
- 不改变 model mapping、quota tracker、sticky session 或重试逻辑。
- 不新增配置项或环境变量。
- 不改变 API response schema。
- 不影响 Windows、Linux、macOS；实现只使用 Rust stdlib。

## 验收标准

1. GIVEN 以 Hermes Identity Prefix 开头的 system instruction, WHEN 转换请求, THEN 输出必须以 Antigravity Identity 开头且不再包含原 Hermes identity prefix。
2. GIVEN Hermes Identity Prefix 后存在任意后续指令, WHEN 转换请求, THEN 后续指令必须逐字保留。
3. GIVEN 文本只是在中间引用 Hermes Identity Prefix, WHEN 转换请求, THEN 输出必须原样不变。
4. GIVEN user 消息包含 Hermes Identity Prefix, WHEN 转换请求, THEN user 消息必须原样不变。
5. GIVEN 完整 Hermes system + tools, WHEN 分别调用 chat 和 Responses 转换路径, THEN 两条路径都必须包含 normalized identity 和全部 tools。
6. GIVEN 相同 Hermes prompt 转换两次, WHEN 计算 prefix hash, THEN 两次 hash 必须相等。
7. GIVEN 生产端两个账号配额可用, WHEN 发送完整 Hermes request, THEN Manager 必须返回 200，不得返回身份分类导致的 RESOURCE_EXHAUSTED。

## 非目标

- 解决真实 Google TPM、RPM 或日配额耗尽。
- 增加账号数量或改变轮询算法。
- 修改 Hermes prompt 内容和工具数量。
- 为任意第三方 Agent 名称做通用伪装。
- 将客户端身份做成用户配置。
