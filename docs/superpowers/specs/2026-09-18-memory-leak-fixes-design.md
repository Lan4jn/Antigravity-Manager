# 内存泄露与内存膨胀全面修复设计规格 (Memory Leak Fixes Design Spec)

**日期**: 2026-09-18  
**目标**: 根治 Antigravity-Manager 项目中在长时间运行和密集代理请求场景下存在的内存泄露（Memory Leak）与内存膨胀（Memory Bloat）隐患。

---

## 1. 背景与问题定义

通过第一性原理深度代码审计，识别出 6 处潜在/确凿的内存风险：
1. **`WEBSOCKET_TOOL_CALL_CACHE` 静态全局哈希表只增不减**（OpenAI 响应工具调用，无任何清理，永久占用堆内存）。
2. **`session_accounts` 会话账号映射无界增长**（每次生成新 `sid` 插入，缺少 TTL 与后台自动清理机制）。
3. **`ProxyMonitor.logs` 内存中保存 1000 条全量大请求/响应 Body**（长上下文交互下单条日志可达数 MB，1000 条常驻导致 500MB~2GB+ 内存占用）。
4. **`SignatureCache` 与 `CacheManager` 伪 LRU 淘汰缺陷**（仅过滤过期条目，在未过期密集请求下突破限制持续增长）。
5. **`ProxyPoolManager` 后台健康检查协程泄漏**（每次重启代理产生无法退出的死循环协程与旧实例强引用）。
6. **`ZaiVisionMcpState` 缺少会话超时清理**（仅依靠客户端 DELETE，未正常关闭则永久留存）。

---

## 2. 设计方案与分模块实施细节

恪守 KISS 原则，不引入沉重的第三方外部依赖，基于现有标准库与既有模式进行内聚、高效修复。

### 模块一：`WEBSOCKET_TOOL_CALL_CACHE` 引入容量上限与时间戳淘汰
- **文件**: `src-tauri/src/proxy/handlers/openai.rs`
- **设计**:
  - 将 `HashMap<String, Value>` 调整为包含时间戳的条目 `HashMap<String, (Value, std::time::Instant)>`。
  - 定义常量 `TOOL_CALL_CACHE_CAP: usize = 500` 和 `TOOL_CALL_CACHE_TTL: Duration = Duration::from_secs(1800)`（30分钟）。
  - 在 `insert_cached_tool_call` 时：
    1. 超过容量时，先清除过期（`elapsed() > TTL`）项；
    2. 若清理后仍达到容量上限，通过 `min_by_key` 淘汰最旧的条目；
    3. 插入新条目。
  - 在 `get_cached_tool_call` 时刷新其 `Instant`（或保留原始时间），确保活跃项优先保留。

### 模块二：`session_accounts` 会话账号映射增加 TTL 与后台自动清理
- **文件**: `src-tauri/src/proxy/token_manager.rs`
- **设计**:
  - 将 `DashMap<String, String>` 改为 `DashMap<String, (String, std::time::Instant)>`。
  - 定义会话过期时间 `const SESSION_ACCOUNT_TTL: Duration = Duration::from_secs(2 * 3600);`（2小时）。
  - 在写入 `session_accounts.insert` 时记录 `Instant::now()`；读取时检查是否过期（若过期则懒清除）。
  - 在已有的 `start_auto_cleanup` 周期任务（每 60 秒触发）中，增加 `cleanup_expired_sessions`：遍历 `session_accounts` 移除超期项。

### 模块三：`ProxyMonitor.logs` 内存日志体瘦身与容量下调
- **文件**:
  - `src-tauri/src/proxy/monitor.rs`
  - `src-tauri/src/commands/proxy.rs`
- **设计**:
  - 原理：所有请求与响应的完整 Body 已经在 SQLite 数据库（`proxy_db`）中进行了持久化，前端或 API 查询单条详细记录也是从 SQLite 动态读取。
  - 将内存中 `max_logs` 默认从 `1000` 调整为 `200`（完全满足前端面板实时监控展示需求）。
  - 存入内存 `logs: RwLock<VecDeque<ProxyRequestLog>>` 时，对 `request_body` 和 `response_body` 施加内存安全保护：
    - 若长度超过 `4096` 字符（4KB），截断并追加 `"... (body truncated for memory in cache, view full body in DB)"`，或者内存缓存中只保留预览。
    - 数据库存储保持完整无损（直接存入 SQLite 的是完整版本）。
  - 效果：将原本可能占用数百 MB 至数 GB 的内存队列稳定控制在几 MB 以内，彻底消除 OOM 隐患。

### 模块四：`SignatureCache` 与 `CacheManager` 补齐真正 LRU 驱逐
- **文件**:
  - `src-tauri/src/proxy/signature_cache.rs`
  - `src-tauri/src/proxy/cache_manager.rs`
- **设计**:
  - 在 `SignatureCache` 中，当 `cache.len() >= LIMIT` 时：
    1. 首先执行 `cache.retain(|_, v| !v.is_expired())`；
    2. 若清理后条目数依然 `>= LIMIT`，计算超出量 `excess = cache.len() - LIMIT + 1`，按 `created_at`（或访问时间）排序找出最旧的 `excess` 个 key 并移除。
  - 在 `CacheManager`（`si_cache`, `tools_cache`, `prefix_tracker`）中同样应用该逻辑，保证在密集突发请求下，缓存容量绝对不会越过上限阈值。

### 模块五：`ProxyPoolManager` 后台任务生命周期管理
- **文件**:
  - `src-tauri/src/proxy/proxy_pool.rs`
  - `src-tauri/src/proxy/server.rs`
- **设计**:
  - 在 `ProxyPoolManager` 结构体中添加 `health_check_task: Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>`。
  - 在 `start_health_check_loop` 时，若之前已存在 `handle`，先执行 `handle.abort()`。
  - 提供 `stop_health_check_loop(&self)` 方法，在代理服务关闭时调用，确保协程优雅终止，旧的 manager 引用能被及时 drop。

### 模块六：`ZaiVisionMcpState` 会话增加 TTL 与被动/主动清理
- **文件**:
  - `src-tauri/src/proxy/zai_vision_mcp.rs`
- **设计**:
  - 在 `ZaiVisionSession` 中启用 `created_at: Instant` 和 `last_accessed: Instant`。
  - 定义 `const SESSION_TTL: Duration = Duration::from_secs(1800);`（30分钟）。
  - 在 `create_session` 和 `get_session` 时先调用 `cleanup_expired_sessions(&mut sessions)` 剔除超过 30 分钟未活跃的孤儿会话。

---

## 3. 测试与验证策略

1. **单元测试验证**：
   - 编写 `SignatureCache` 和 `CacheManager` 在未过期情况下的超限驱逐测试，验证超限后数量不超过容量上限。
   - 编写 `WEBSOCKET_TOOL_CALL_CACHE` 插入超过 `TOOL_CALL_CACHE_CAP` 后的容量边界测试。
   - 编写 `session_accounts` 在过期后自动剔除测试。
2. **构建与运行测试**：
   - 运行 `cargo test` 确保所有单元测试与集成测试通过。
   - 检查是否有编译警告或并发死锁隐患。
