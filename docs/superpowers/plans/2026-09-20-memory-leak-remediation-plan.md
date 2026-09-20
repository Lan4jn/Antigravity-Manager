# 内存修复补全开发计划

> **面向执行 agent：** 按顺序执行本计划；有 `executing-plans` 技能时按其流程推进。用复选框记录进度，测试先证明问题再验证修复。每完成一个任务记录实际证据。

**目标：** 修复日志实际分配保留、代理池再次初始化的配置分离、七层缓存访问顺序淘汰和单会话签名历史无界增长，通过规格 A1–A11 验收。

**架构：** 保持模块结构，内存正文直接生成预览；代理池提供规范配置与等待完成的更新入口；缓存独立维护访问顺序；签名历史在所属模块内执行双预算。沿用当前依赖。

**技术栈：** Rust、Tokio、DashMap、标准库 Mutex/Arc/HashMap、SQLite。

**输入：** [检查报告](../reviews/2026-09-20-memory-leak-review.md)、[规格](../specs/2026-09-20-memory-leak-remediation-spec.md)。起点 `f33b1bdf4980e4f7b94db8322dcfa75152a75c52`，基线 `0802afd045cfac4297fa46a16a30cec68dbb48fa`。

## 文件职责与顺序

| 文件 | 工作内容 |
|---|---|
| `src-tauri/src/proxy/monitor.rs` | 紧凑正文预览、日志摘要构造、capacity 和持久化测试 |
| `src-tauri/src/proxy/proxy_pool.rs` | 异步初始化、规范配置 Arc、配置/绑定同步、复用及任务测试 |
| `src-tauri/src/proxy/server.rs` | start 等待初始化、两处服务状态共享规范 Arc、热更新入口 |
| `src-tauri/src/proxy/signature_cache.rs` | 四层 LRU、单会话签名条数/字节预算、行为测试 |
| `src-tauri/src/proxy/cache_manager.rs` | 三层 LRU、TTL/统计/并发容量测试 |
| `src-tauri/src/commands/proxy.rs` | 核查逻辑启停与 Admin Server 复用；仅在调用签名变化要求时适配 |
| `src-tauri/src/modules/proxy_db.rs` | 查阅 save_log/get_log_detail 的持久化行为，沿用现有接口 |
| `docs/superpowers/reviews/2026-09-20-memory-leak-remediation-results.md` | 创建最终执行结果，逐项关联验收和证据 |

任务依次为：0 基线 → 1 日志 → 2 配置共享 → 3 LRU → 4 历史预算 → 5 综合验收。任务 3、4 修改同一文件，顺序执行。无需拆分公共缓存框架。

## 任务 0：建立可比验证环境

- [ ] 读取仓库指令，检查工作区、分支和提交；保留已有用户改动。若根目录存在 `.codegraph/`，代码定位优先 CodeGraph。

```bash
git status --short
git branch --show-current
git rev-parse HEAD
git merge-base main HEAD
```

预期：确认当前分支包含被检查提交。若 HEAD 已推进，先检查新增 diff 对 R1–R4 的影响，再调整具体行号。

- [ ] 在独立终端进程设置隔离数据目录，保存路径用于后续同一验证环境。以下命令在仓库根目录运行：

```bash
review_root=$(mktemp -d /tmp/antigravity-remediation.XXXXXX)
mkdir -p "$review_root/candidate-data" "$review_root/baseline-data"
export ABV_DATA_DIR="$review_root/candidate-data"
cd src-tauri
cargo test --lib proxy:: --offline -- --test-threads=1 > "$review_root/candidate-before.log" 2>&1
```

记录退出码。报告中的 572/18 是默认并行测试的历史结果；串行运行数字可能不同，必须保存本次真实结果。

- [ ] 在隔离 checkout 检查基线；如果可用，使用原生 worktree 工具，否则运行以下 shell。不要在当前目录切换到基线。

```bash
git worktree add --detach "$review_root/baseline" 0802afd045cfac4297fa46a16a30cec68dbb48fa
cd "$review_root/baseline/src-tauri"
ABV_DATA_DIR="$review_root/baseline-data" cargo test --lib proxy:: --offline -- --test-threads=1 > "$review_root/baseline.log" 2>&1
```

离线依赖缺失时记录原因并在允许网络的环境重试同一命令去掉 `--offline`，基线与候选使用一致策略。单独保留各自 target 或串行共享构建缓存，避免两个 Cargo 同时竞争同一路径。

- [ ] 建立失败清单：测试名、基线结果、候选结果、错误摘要。不能用“其他模块失败”代替归因。

## 任务 1：让日志正文的实际分配有界（A1–A3）

- [ ] 在 monitor 测试模块加入原行为的失败断言，直接检查 capacity。旧函数可先使用以下测试证实 R1：

```rust
#[test]
fn memory_preview_releases_large_allocation() {
    let mut body = "x".repeat(10 * 1024 * 1024);
    truncate_string_safe(&mut body, MAX_MEMORY_BODY_BYTES);
    assert!(body.capacity() <= MAX_MEMORY_BODY_BYTES + "... [TRUNCATED FOR IN-MEMORY CACHE]".len());
}
```

运行 `cargo test --lib memory_preview_releases_large_allocation`，预期旧实现失败。最终实现转换为下述预览接口后，同步更新这个测试调用，保持 capacity 断言。

- [ ] 用借用输入的帮助函数替换原地截断；前缀之外不分配正文内容。建议实现：

```rust
const MEMORY_BODY_SUFFIX: &str = "... [TRUNCATED FOR IN-MEMORY CACHE]";

fn body_preview(body: &str) -> String {
    let mut end = body.len().min(MAX_MEMORY_BODY_BYTES);
    while !body.is_char_boundary(end) {
        end -= 1;
    }
    let suffix = if end < body.len() { MEMORY_BODY_SUFFIX } else { "" };
    let mut result = String::with_capacity(end + suffix.len());
    result.push_str(&body[..end]);
    result.push_str(suffix);
    result
}
```

- [ ] 增加 `fn memory_log_preview(log: &ProxyRequestLog) -> ProxyRequestLog`，逐字段复制元数据并对正文调用 `body_preview`。以下构造可放入该函数；不用 `..log.clone()`，因为它仍克隆完整正文：

```rust
ProxyRequestLog {
    id: log.id.clone(), timestamp: log.timestamp,
    method: log.method.clone(), url: log.url.clone(),
    status: log.status, duration: log.duration,
    model: log.model.clone(), mapped_model: log.mapped_model.clone(),
    account_email: log.account_email.clone(), client_ip: log.client_ip.clone(),
    error: log.error.clone(),
    request_body: log.request_body.as_deref().map(body_preview),
    response_body: log.response_body.as_deref().map(body_preview),
    input_tokens: log.input_tokens, output_tokens: log.output_tokens,
    cached_tokens: log.cached_tokens, protocol: log.protocol.clone(),
    username: log.username.clone(),
}
```

在 `log_request` 仅把该函数结果放入 logs；数据库继续收到完整原始 log。

- [ ] 添加边界测试：空/None；16,384 字节；16,385 字节；中文和 emoji 横跨截断点；`String::with_capacity(10 MiB)` 后只写入 `"short"`。每次检查 UTF-8、len、capacity 和原始输入内容不变。

- [ ] 复用 monitor 测试的日志构造字段，循环写入超过 100 条带大正文的日志，验证 100 条窗口和两个正文 capacity 总和。使用适量大正文测试队列（例如每个正文 128 KiB），单独的 10 MiB 用例证明原始缺陷，控制测试峰值。

- [ ] 持久化测试用唯一日志 ID、临时 ABV_DATA_DIR；轮询 `proxy_db::get_log_detail`，5 秒 timeout 内等到异步写入完成，断言读回正文等于原文。用有条件的短间隔等待，不用固定 sleep 作为成功判据。

- [ ] 运行 `cargo test --lib proxy::monitor::tests -- --test-threads=1`，确认容量与完整持久化都通过；记录测试结果。

## 任务 2：恢复全局代理池配置共享（A4–A5）

- [ ] 在 `proxy_pool.rs` 模块内增加两个核心回归：再次初始化后更新服务侧配置应反映到 manager；初始化返回后不经过 yield/sleep 就能读到新配置与绑定。已有测试的固定 50 ms sleep 不足以验证完成顺序。

再次初始化测试的关键断言如下，`first`、`second` 分别由初次和再次初始化获得：

```rust
assert!(Arc::ptr_eq(&first, &second));
assert!(Arc::ptr_eq(&server_config, &second.shared_config()));
server_config.write().await.enabled = true;
assert!(second.config.read().await.enabled);
```

`shared_config` 是本任务新增方法；`server_config` 要按最终 server 的真实接线方式获取，不能仍沿用新建且被丢弃的 Arc。

- [ ] 增加配置入口：

```rust
pub fn shared_config(&self) -> Arc<RwLock<ProxyPoolConfig>> {
    self.config.clone()
}
```

增加 `pub async fn update_config(&self, new_config: ProxyPoolConfig)`，在 manager 内用一个配置更新 Mutex 串行完成配置替换和 `sync_bindings_from_config().await`。在 new 中初始化这把锁；避免外层仍持有 config 写锁就调用同步绑定函数。

- [ ] 将初始化改为 `pub async fn init_global_proxy_pool(...) -> Arc<ProxyPoolManager>`。用 `OnceLock::get_or_init` 取得唯一 manager，先在独立语句克隆传入配置值并释放读 guard，再 `manager.update_config(snapshot).await` 后返回；删除脱离调用方的配置复制 spawn。调用时传入现有规范 Arc 也必须完成。

- [ ] `AxumServer::start` 等待初始化，并取得规范配置：

```rust
let input_config = Arc::new(RwLock::new(proxy_pool_config));
let proxy_pool_manager =
    crate::proxy::proxy_pool::init_global_proxy_pool(input_config).await;
let proxy_pool_state = proxy_pool_manager.shared_config();
```

AppState 和 AxumServer 都保存这里的 `proxy_pool_state`。将 `update_proxy_pool` 的配置更新改为 `self.proxy_pool_manager.update_config(new_config).await`，保留 HTTP client cache 清理。

- [ ] 搜索所有初始化调用并适配 await，检查所有配置写入路径是否可能与新入口竞争；绑定变更继续使用现有持久化规则。

```bash
rg -n 'init_global_proxy_pool|update_proxy_pool|proxy_pool_state|sync_bindings_from_config' src
```

- [ ] 添加测试覆盖规范 Arc 自身再次初始化、两个并发初始化的 manager 相同、更新返回后配置和绑定一致、首次初始化后再次调用模拟启动重试。全局 OnceLock 场景合并为一个顺序测试或使用测试锁隔离，避免测试之间覆盖全局配置。

- [ ] 保留并运行健康检查任务重复启动/停止/释放测试。核对 `ensure_admin_server`、`stop_proxy_service`，保持逻辑停用时管理服务可用。运行 `cargo test --lib proxy::proxy_pool::tests -- --test-threads=1` 和 `cargo check`。

## 任务 3：七层缓存最近访问淘汰（A6–A7、A10）

- [ ] 先增加 SignatureCache 的热条目回归；以下是完整测试形状，使用注入时间构造顺序，不依赖 sleep：

```rust
#[test]
fn tool_cache_preserves_recently_read_entry() {
    let cache = SignatureCache::new();
    cache.cache_tool_signature("hot", "x".repeat(60));
    for i in 0..TOOL_CACHE_LIMIT - 1 {
        cache.cache_tool_signature(&format!("cold_{i}"), "x".repeat(60));
    }
    assert!(cache.get_tool_signature("hot").is_some());
    cache.cache_tool_signature("new", "x".repeat(60));
    assert!(cache.get_tool_signature("hot").is_some());
    assert_eq!(cache.tool_cache_len(), TOOL_CACHE_LIMIT);
}
```

为了完全确定顺序，测试内部将 hot 的访问时间设为最早、cold 设置为中间；读取 hot 后再插入 new。旧代码没有 last_accessed 时先用原 timestamp 注入年龄来证实失败；字段新增后调整测试注入位置。

- [ ] `signature_cache.rs` 导入 Instant，`CacheEntry<T>` 增加 `last_accessed`，构造初始化；有效写入更新。getter 使用可变条目，先判断 TTL，再取得实际返回值，确认命中后刷新：

```rust
entry.last_accessed = Instant::now();
```

覆盖 `get_tool_signature`、`get_signature_family`、`get_session_signature`、`get_session_signature_at`、`get_session_reasoning`。历史轮次 miss 不刷新；保持 None 的原有含义。

- [ ] 淘汰帮助函数的时间类型改为 Instant（或泛型 `T: Ord`），四个调用点用 `last_accessed`。原 timestamp 继续负责 TTL。检查所有 CacheEntry 字面量，包括测试内部构造。

- [ ] CacheManager 三个条目新增访问时间。lookup 在持有 `get_mut` guard 时判过期和更新 last_accessed，复制返回值后释放 guard，再执行统计或删除。淘汰用访问时间；prefix 的两个有效命中计数入口同时刷新访问时间但不修改 expires_at。

- [ ] 对其余六层分别加入相同的“填满 → 设置 hot 为最早 → 命中 hot → 插入 → 检查 hot 存活且 cold 淘汰”用例。各层用现有构造/API：`cache_thinking_family`、`cache_session_signature`、`cache_session_reasoning`、`cache_si`、`cache_tools`、`insert_prefix`。断言数据值、容量和具体被淘汰键。

- [ ] 直接注入过期 timestamp/expires_at，验证重复读取不延长 TTL；有效读取只改变 last_accessed。历史轮次缺失和空 reasoning 返回 None，访问时间不变。保留既有统计测试。

- [ ] DashMap 并发回归：多个线程分别插入唯一 SI/tools/prefix key，在所有线程 join 完成后检查三层数量不超过上限；以独立测试进程超时约束发现死锁。避免持 entry guard 调用 len/remove/retain。

- [ ] 运行并记录：

```bash
cargo test --lib proxy::signature_cache::tests -- --test-threads=1
cargo test --lib proxy::cache_manager::tests -- --test-threads=1
```

## 任务 4：单会话签名历史双预算（A8–A9）

- [ ] 添加旧实现必然失败的测试，验证单会话 10,000 次写入后历史条数：

```rust
#[test]
fn session_signature_history_is_bounded() {
    let cache = SignatureCache::new();
    for message_count in 0..10_000 {
        cache.cache_session_signature("s", "x".repeat(60), message_count);
    }
    {
        let guard = cache.session_signatures.lock().unwrap();
        let history = &guard.get("s").unwrap().data;
        assert!(history.len() <= 256);
        let bytes: usize = history.values().map(|v| v.signature.capacity()).sum();
        assert!(bytes <= 1024 * 1024);
    }
    assert!(cache.get_session_signature_at("s", 0).is_none());
    assert!(cache.get_session_signature_at("s", 9_999).is_some());
}
```

- [ ] 增加规格 G4 中两个常量。在进入缓存写锁前拒绝超过单会话字节预算的输入；已有最短长度检查保留。将接收值紧凑复制为 `signature.as_str().to_owned()`，再释放原输入，避免原 capacity 绕过预算。

- [ ] 在查找/创建会话条目后，若旧条目过期则重建空历史；再按顺序执行：刷新写入时间及访问时间 → rewind 清理 → 同轮更长值替换 → 双预算淘汰。

- [ ] 预算帮助函数仅处理现有内层 map。每次最多约 257 条时直接求容量和并移除最小 message_count 即可，避免引入独立缓存库或不必要的账本状态。基本淘汰循环为：

```rust
while history.len() > SESSION_SIGNATURE_HISTORY_LIMIT
    || history.values().map(|v| v.signature.capacity()).sum::<usize>()
        > SESSION_SIGNATURE_HISTORY_BYTES
{
    let oldest = history.keys().copied().min();
    if let Some(key) = oldest {
        history.remove(&key);
    } else {
        break;
    }
}
```

该代码放在 `cache_session_signature` 写入后或该文件局部帮助函数；`history` 指向当前 `entry.data`。

- [ ] 边界用例：17 条各 64 KiB 签名触发字节淘汰；单条恰好 1 MiB 可保存；大于 1 MiB 被拒绝且旧缓存不变；小 len 大 capacity 被紧凑保存；同轮更短不覆盖、更长覆盖后重算预算；写入轮次 10、20、30 后回退到 20，30 消失；过期会话再次写入时旧历史清空。

- [ ] 运行所有 SignatureCache 测试并核查生产消费端 `get_session_signature_at` 和 latest getter 的 None 路径，确认缓存缺失保持原有行为。外层 1000 会话限制继续有效。

## 任务 5：综合验收与交付（A1–A11）

- [ ] 在隔离 ABV_DATA_DIR 中运行所有相关模块。以下每条命令单独记录退出码，某条失败也保留其余结果：

```bash
cargo check
cargo test --lib proxy::monitor::tests -- --test-threads=1
cargo test --lib proxy::proxy_pool::tests -- --test-threads=1
cargo test --lib proxy::signature_cache::tests -- --test-threads=1
cargo test --lib proxy::cache_manager::tests -- --test-threads=1
cargo test --lib proxy::handlers::openai::tests -- --test-threads=1
cargo test --lib proxy::zai_vision_mcp::tests -- --test-threads=1
cargo test --lib test_cleanup_expired_sessions -- --test-threads=1
cargo test --lib proxy:: -- --test-threads=1
cargo test -- --test-threads=1
```

如保留 offline 策略，所有命令按同一策略附加 `--offline`。未修改前端无需为了这次 Rust 修复重复前端构建。

- [ ] 对失败测试执行同环境基线对照；全量运行中新出现但任务 0 未覆盖的失败，在基线运行同名测试或相同全量命令补证据。修复本轮新增失败；基线失败记录测试名、错误和对照输出。无法判定时明确标记验证未完成。

- [ ] 运行定向持续负载验证：10,000 次日志窗口替换、10,000 个缓存 key、10,000 个单会话轮次、重复初始化/热更新。记录日志正文 capacity 总和、每层 entry count、每会话历史条数和 bytes。可选 RSS 采样只能作为补充，不能代替对象容量断言。

- [ ] 检查修改文件格式和 diff，避免格式化整仓带来无关变更：

```bash
git diff --check
git diff --stat
git status --short
```

- [ ] 创建 `docs/superpowers/reviews/2026-09-20-memory-leak-remediation-results.md`，包含：实际起止提交/分支、R1–R4 修复说明、A1–A11 逐项证据、命令/退出码、基线失败归因、保留内存测量值、剩余限制。没有通过的验收保持未完成状态。

- [ ] 最终向用户报告改动与证据。提交代码时按有意义的任务划分本地 commit；推送、PR、合并等交付操作遵循用户另行授权。
