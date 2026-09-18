# 内存泄露与内存膨胀全面修复实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 根除并防御项目中存在的 6 项内存泄露与大内存膨胀隐患，确保服务在长久运行与高并发代理请求下内存稳定。

**架构：** 基于项目现有的内聚轻量模式（无外部沉重依赖），对全局静态缓存补齐容量上限与访问淘汰，Session 映射加入 TTL 自动清理，ProxyMonitor 内存日志进行截断保护与容量收敛，ProxyPool 后台任务由 JoinHandle 进行生命周期管理，Vision MCP 会话增加超时驱逐。

**技术栈：** Rust, Tokio, DashMap, Axum, Tauri

---

### 任务 1：修复 `WEBSOCKET_TOOL_CALL_CACHE` 静态全局哈希表只增不减泄露

**文件：**
- 修改：`src-tauri/src/proxy/handlers/openai.rs:5675-5710`

- [ ] **步骤 1：编写失败的测试**
在 `src-tauri/src/proxy/handlers/openai.rs` 底部或现有测试模块中添加针对 Tool Call Cache 容量控制与淘汰的单元测试：
```rust
#[tokio::test]
async fn test_websocket_tool_call_cache_eviction() {
    for i in 0..600 {
        insert_cached_tool_call(format!("call_{}", i), serde_json::json!({ "id": i }));
    }
    // 验证容量上限不会超过 TOOL_CALL_CACHE_CAP (500)
    let cache = WEBSOCKET_TOOL_CALL_CACHE.get().unwrap();
    let guard = cache.read().await;
    assert!(guard.len() <= 500);
}
```

- [ ] **步骤 2：运行测试验证失败**
运行：`cargo test --bin antigravity-manager -- test_websocket_tool_call_cache_eviction`
预期：FAIL（目前无容量上限，长度将为 600）。

- [ ] **步骤 3：实现带时间戳与容量限制的缓存管理**
在 `src-tauri/src/proxy/handlers/openai.rs` 中：
```rust
const TOOL_CALL_CACHE_CAP: usize = 500;
const TOOL_CALL_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(1800);

static WEBSOCKET_TOOL_CALL_CACHE: OnceLock<TokioRwLock<HashMap<String, (Value, std::time::Instant)>>> = OnceLock::new();

pub fn get_cached_tool_call(call_id: &str) -> Option<Value> {
    if let Some(cache) = WEBSOCKET_TOOL_CALL_CACHE.get() {
        if let Ok(guard) = cache.try_read() {
            return guard.get(call_id).map(|(v, _)| v.clone());
        }
    }
    None
}

pub fn insert_cached_tool_call(call_id: String, item: Value) {
    if call_id.is_empty() {
        return;
    }

    let cache = WEBSOCKET_TOOL_CALL_CACHE.get_or_init(|| TokioRwLock::new(HashMap::new()));

    if let Ok(mut guard) = cache.try_write() {
        let now = std::time::Instant::now();
        // 1. 超限时先清理过期条目
        if guard.len() >= TOOL_CALL_CACHE_CAP {
            guard.retain(|_, (_, ts)| now.duration_since(*ts) < TOOL_CALL_CACHE_TTL);
        }
        // 2. 如果依然超限，淘汰最旧的条目
        if guard.len() >= TOOL_CALL_CACHE_CAP {
            if let Some(oldest_key) = guard.iter().min_by_key(|(_, (_, ts))| *ts).map(|(k, _)| k.clone()) {
                guard.remove(&oldest_key);
            }
        }
        guard.insert(call_id, (item, now));
    }
}
```

- [ ] **步骤 4：运行测试验证通过**
运行：`cargo test -- test_websocket_tool_call_cache_eviction`
预期：PASS。

- [ ] **步骤 5：Commit**
```bash
git add src-tauri/src/proxy/handlers/openai.rs
git commit -m "fix(openai): add bounded capacity and LRU eviction to WEBSOCKET_TOOL_CALL_CACHE"
```

---

### 任务 2：修复 `session_accounts` 会话账号映射无界增长与后台清理缺失

**文件：**
- 修改：`src-tauri/src/proxy/token_manager.rs:144, 177, 210-245, 1910, 2026, 3435-3445, 3823, 3852`

- [ ] **步骤 1：编写失败的测试**
在 `src-tauri/src/proxy/token_manager.rs` 的测试模块中编写测试：
```rust
#[tokio::test]
async fn test_session_accounts_cleanup_expired() {
    let tm = TokenManager::new_for_test();
    tm.session_accounts.insert("expired_sess".to_string(), ("acc_1".to_string(), std::time::Instant::now() - std::time::Duration::from_secs(7300)));
    tm.session_accounts.insert("fresh_sess".to_string(), ("acc_2".to_string(), std::time::Instant::now()));
    
    let cleaned = tm.cleanup_expired_sessions();
    assert_eq!(cleaned, 1);
    assert!(!tm.session_accounts.contains_key("expired_sess"));
    assert!(tm.session_accounts.contains_key("fresh_sess"));
}
```

- [ ] **步骤 2：运行测试验证失败**
运行：`cargo test -- test_session_accounts_cleanup_expired`
预期：FAIL（方法未定义，类型不匹配）。

- [ ] **步骤 3：实现会话映射 TTL 与自动清理**
1. 将 `session_accounts` 改为 `Arc<DashMap<String, (String, std::time::Instant)>>`。
2. 定义 `const SESSION_ACCOUNT_TTL: std::time::Duration = std::time::Duration::from_secs(2 * 3600);`。
3. 读写时适配 `(account_id, timestamp)` 元组，读出如果超时则清理返回 None。
4. 添加 `pub fn cleanup_expired_sessions(&self) -> usize` 方法，遍历移除 `ts.elapsed() >= SESSION_ACCOUNT_TTL` 的条目。
5. 在 `start_auto_cleanup` 的后台定时循环（每 60 秒）中调用 `tracker.cleanup_expired(); self_clone.cleanup_expired_sessions();`。

- [ ] **步骤 4：运行测试验证通过**
运行：`cargo test -- test_session_accounts_cleanup_expired`
预期：PASS。

- [ ] **步骤 5：Commit**
```bash
git add src-tauri/src/proxy/token_manager.rs
git commit -m "fix(proxy): add TTL and auto-cleanup for session_accounts map"
```

---

### 任务 3：优化 `ProxyMonitor` 内存日志大文本常驻与容量下调

**文件：**
- 修改：`src-tauri/src/proxy/monitor.rs:40-60, 145-160`
- 修改：`src-tauri/src/commands/proxy.rs:113, 234`

- [ ] **步骤 1：编写测试**
在 `monitor.rs` 添加测试，验证放入 `self.logs` 的日志对超长 `request_body` 和 `response_body` 进行了安全截断保护：
```rust
#[tokio::test]
async fn test_monitor_logs_truncation_for_memory_safety() {
    let monitor = ProxyMonitor::new_test(10);
    let mut log = ProxyRequestLog::default();
    log.request_body = Some("a".repeat(100_000));
    log.response_body = Some("b".repeat(100_000));
    monitor.log_request(log).await;
    
    let logs = monitor.logs.read().await;
    assert!(logs[0].request_body.as_ref().unwrap().len() <= 4096 + 100);
    assert!(logs[0].response_body.as_ref().unwrap().len() <= 4096 + 100);
}
```

- [ ] **步骤 2：运行测试验证失败**
运行：`cargo test -- test_monitor_logs_truncation_for_memory_safety`
预期：FAIL。

- [ ] **步骤 3：实现截断与容量调整**
1. 在 `commands/proxy.rs` 中，将 `ProxyMonitor::new(1000, app_handle)` 的内存容量从 `1000` 调整为 `200`。
2. 在 `monitor.rs` 中，在 `log_request` 存入 SQLite 前保持全量，而在将日志 push 到内存 `self.logs` 时执行截断辅助函数 `truncate_body_for_cache(body, 4096)`。
3. 补充提示后缀 `\n... [Truncated for memory cache, full content preserved in database]`。

- [ ] **步骤 4：运行测试验证通过**
运行：`cargo test -- test_monitor_logs_truncation_for_memory_safety`
预期：PASS。

- [ ] **步骤 5：Commit**
```bash
git add src-tauri/src/proxy/monitor.rs src-tauri/src/commands/proxy.rs
git commit -m "perf(monitor): truncate in-memory log bodies and reduce memory queue size"
```

---

### 任务 4：修复 `SignatureCache` 和 `CacheManager` 伪 LRU 淘汰缺陷

**文件：**
- 修改：`src-tauri/src/proxy/signature_cache.rs:60-110, 200-280, 285-350`
- 修改：`src-tauri/src/proxy/cache_manager.rs:230-260, 320-350, 430-460`

- [ ] **步骤 1：编写失败的测试**
针对 `SignatureCache` 编写未过期但达到容量上限的严格淘汰测试：
```rust
#[test]
fn test_signature_cache_strict_capacity_limit() {
    let cache = SignatureCache::new();
    for i in 0..1100 {
        cache.cache_thinking_family(&format!("model_{}", i), &format!("family_{}", i));
    }
    // 验证即使所有条目均未过期（创建于0秒前），超出 1000 条上限时也能通过真正的 LRU 驱逐最旧项
    assert!(cache.thinking_families.lock().unwrap().len() <= 1000);
}
```

- [ ] **步骤 2：运行测试验证失败**
运行：`cargo test -- test_signature_cache_strict_capacity_limit`
预期：FAIL（目前不会剔除未过期条目，长度依然为 1100）。

- [ ] **步骤 3：实现真正的超限 LRU 驱逐**
在 `SignatureCache` 的 4 个哈希表插入逻辑中，当 `cache.len() >= LIMIT` 时：
1. 先过滤已过期条目；
2. 如果 `cache.len() >= LIMIT`，计算超额数量 `excess = cache.len() - LIMIT + 1`；
3. 从中挑选出按时间戳排序最旧的 `excess` 个 key 并移除（`min_by_key` 或提取键值列表排序剔除）。
在 `CacheManager`（`si_cache`, `tools_cache`, `prefix_tracker`）中同样落实此逻辑。

- [ ] **步骤 4：运行测试验证通过**
运行：`cargo test -- test_signature_cache_strict_capacity_limit`
预期：PASS。

- [ ] **步骤 5：Commit**
```bash
git add src-tauri/src/proxy/signature_cache.rs src-tauri/src/proxy/cache_manager.rs
git commit -m "fix(proxy): implement true LRU eviction for signature and manager caches"
```

---

### 任务 5：修复 `ProxyPoolManager` 后台健康检查协程泄漏

**文件：**
- 修改：`src-tauri/src/proxy/proxy_pool.rs:35-50, 620-645`
- 修改：`src-tauri/src/proxy/server.rs:520-540`

- [ ] **步骤 1：为 ProxyPoolManager 增加任务 Handle 管理**
在 `ProxyPoolManager` 结构体中新增：
```rust
health_check_task: Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
```

- [ ] **步骤 2：重写 `start_health_check_loop` 与新增 `stop_health_check_loop`**
在 `start_health_check_loop` 中：
```rust
pub async fn start_health_check_loop(self: Arc<Self>) {
    let mut task_guard = self.health_check_task.lock().await;
    if let Some(old_handle) = task_guard.take() {
        old_handle.abort();
    }
    let weak_self = Arc::downgrade(&self);
    let handle = tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
            if let Some(mgr) = weak_self.upgrade() {
                mgr.check_all_proxies().await;
            } else {
                break; // Manager已被销毁，自动退出循环
            }
        }
    });
    *task_guard = Some(handle);
}
```
使用 `Arc::downgrade` 打破强引用循环，并在退出时自动 break；同时提供 abort 旧 handle 的防重复机制。

- [ ] **步骤 3：在 `server.rs` 启动和停止时接入**
在 `ProxyServer::start` 和停止服务时调用生命周期管理。

- [ ] **步骤 4：运行编译与代理池单元测试**
运行：`cargo test -- proxy_pool`
预期：PASS。

- [ ] **步骤 5：Commit**
```bash
git add src-tauri/src/proxy/proxy_pool.rs src-tauri/src/proxy/server.rs
git commit -m "fix(proxy_pool): manage health check task lifecycle and avoid Arc cycle leak"
```

---

### 任务 6：修复 `ZaiVisionMcpState` 孤儿会话超时清理

**文件：**
- 修改：`src-tauri/src/proxy/zai_vision_mcp.rs`

- [ ] **步骤 1：编写会话超时驱逐测试**
在 `zai_vision_mcp.rs` 中编写测试：
```rust
#[tokio::test]
async fn test_zai_vision_session_ttl_cleanup() {
    let state = ZaiVisionMcpState::new();
    let sid = state.create_session().await;
    // 模拟时间流逝并测试清理逻辑
    ...
}
```

- [ ] **步骤 2：实现基于 TTL 的会话清理**
在 `zai_vision_mcp.rs` 中定义 `SESSION_TTL: Duration = Duration::from_secs(1800);`。在每次 `create_session` 或 `get_session` 时，调用内部的 `cleanup_expired(&mut guard)`，淘汰超时会话。

- [ ] **步骤 3：运行测试验证**
运行：`cargo test -- zai_vision_mcp`
预期：PASS。

- [ ] **步骤 4：Commit**
```bash
git add src-tauri/src/proxy/zai_vision_mcp.rs
git commit -m "fix(mcp): add TTL eviction for orphan zai vision mcp sessions"
```

---

### 任务 7：全局构建与全量测试回归

- [ ] **步骤 1：运行全量 Cargo 构建**
运行：`cargo check` 与 `cargo test`
预期：所有包编译通过，测试全部通过，无新增 warning。

- [ ] **步骤 2：前端类型与构建检查**
运行：`npm run build` 或 `npx tsc --noEmit`
预期：前端无破坏，顺利通过。

- [ ] **步骤 3：最终整理与 Commit**
```bash
git commit --allow-empty -m "chore: complete all memory leak fixes verification"
```
