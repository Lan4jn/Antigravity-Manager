use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use tauri::Emitter;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyRequestLog {
    pub id: String,
    pub timestamp: i64,
    pub method: String,
    pub url: String,
    pub status: u16,
    pub duration: u64,                // ms
    pub model: Option<String>,        // 客户端请求的模型名
    pub mapped_model: Option<String>, // 实际路由后使用的模型名
    pub account_email: Option<String>,
    pub client_ip: Option<String>, // 客户端 IP 地址
    pub error: Option<String>,
    pub request_body: Option<String>,
    pub response_body: Option<String>,
    pub input_tokens: Option<u32>,
    pub output_tokens: Option<u32>,
    pub cached_tokens: Option<u32>,
    pub protocol: Option<String>, // 协议类型: "openai", "anthropic", "gemini"
    pub username: Option<String>, // User token username
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProxyStats {
    pub total_requests: u64,
    pub success_count: u64,
    pub error_count: u64,
}

/// 常驻内存的请求/响应体最大字节数（16KB），超出则截断，防止大 prompt/response 膨胀撑爆内存
const MAX_MEMORY_BODY_BYTES: usize = 16 * 1024;

const TRUNCATION_SUFFIX: &str = "... [TRUNCATED FOR IN-MEMORY CACHE]";

fn truncate_str_safe(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        s.to_string()
    } else {
        let mut truncate_at = max_bytes;
        while truncate_at > 0 && !s.is_char_boundary(truncate_at) {
            truncate_at -= 1;
        }
        let mut out = String::with_capacity(truncate_at + TRUNCATION_SUFFIX.len());
        out.push_str(&s[..truncate_at]);
        out.push_str(TRUNCATION_SUFFIX);
        out
    }
}

pub struct ProxyMonitor {
    pub logs: RwLock<VecDeque<ProxyRequestLog>>,
    pub stats: RwLock<ProxyStats>,
    pub max_logs: usize,
    pub enabled: AtomicBool,
    app_handle: Option<tauri::AppHandle>,
}

impl ProxyMonitor {
    pub fn new(max_logs: usize, app_handle: Option<tauri::AppHandle>) -> Self {
        // Initialize DB
        if let Err(e) = crate::modules::proxy_db::init_db() {
            tracing::error!("Failed to initialize proxy DB: {}", e);
        }

        let retention = crate::modules::config::load_app_config()
            .map(|config| config.proxy.log_retention)
            .unwrap_or_default();
        tokio::task::spawn_blocking(move || {
            match crate::modules::proxy_db::apply_retention(&retention) {
                Ok((cleared, deleted)) => {
                    if cleared > 0 || deleted > 0 {
                        tracing::info!(
                            "Proxy log retention: cleared {} bodies, deleted {} rows",
                            cleared,
                            deleted
                        );
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to cleanup old logs: {}", e);
                }
            }
        });

        tokio::spawn(async {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(3600));
            interval.tick().await;
            loop {
                interval.tick().await;
                let retention = crate::modules::config::load_app_config()
                    .map(|config| config.proxy.log_retention)
                    .unwrap_or_default();
                let result = tokio::task::spawn_blocking(move || {
                    crate::modules::proxy_db::apply_retention(&retention)
                })
                .await;
                match result {
                    Ok(Ok((cleared, deleted))) => tracing::info!(
                        "Proxy log retention: cleared {} bodies, deleted {} rows",
                        cleared,
                        deleted
                    ),
                    Ok(Err(error)) => {
                        tracing::error!("Failed to apply proxy log retention: {}", error)
                    }
                    Err(error) => tracing::error!("Proxy log retention task failed: {}", error),
                }
            }
        });

        Self {
            logs: RwLock::new(VecDeque::with_capacity(max_logs)),
            stats: RwLock::new(ProxyStats::default()),
            max_logs,
            enabled: AtomicBool::new(false), // Default to disabled
            app_handle,
        }
    }

    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub async fn log_request(&self, log: ProxyRequestLog) {
        self.log_request_with_permit(log, None).await;
    }

    pub(crate) async fn log_request_with_permit(
        &self,
        log: ProxyRequestLog,
        permit: Option<tokio::sync::SemaphorePermit<'static>>,
    ) {
        if let (Some(account), Some(input), Some(output)) =
            (&log.account_email, log.input_tokens, log.output_tokens)
        {
            let model = log.model.clone().unwrap_or_else(|| "unknown".to_string());
            let account = account.clone();
            let cached = log.cached_tokens.unwrap_or(0);
            tokio::task::spawn_blocking(move || {
                if let Err(e) = crate::modules::token_stats::record_usage(
                    &account, &model, input, output, cached,
                ) {
                    tracing::debug!("Failed to record token stats: {}", e);
                }
            });
        }

        if !self.is_enabled() {
            return;
        }
        tracing::info!("[Monitor] Logging request: {} {}", log.method, log.url);
        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.total_requests += 1;
            if log.status >= 200 && log.status < 400 {
                stats.success_count += 1;
            } else {
                stats.error_count += 1;
            }
        }

        // Add log to memory (with truncation for memory efficiency)
        {
            let memory_log = ProxyRequestLog {
                id: log.id.clone(),
                timestamp: log.timestamp,
                method: log.method.clone(),
                url: log.url.clone(),
                status: log.status,
                duration: log.duration,
                model: log.model.clone(),
                mapped_model: log.mapped_model.clone(),
                account_email: log.account_email.clone(),
                client_ip: log.client_ip.clone(),
                error: log
                    .error
                    .as_deref()
                    .map(|s| truncate_str_safe(s, MAX_MEMORY_BODY_BYTES)),
                request_body: log
                    .request_body
                    .as_deref()
                    .map(|s| truncate_str_safe(s, MAX_MEMORY_BODY_BYTES)),
                response_body: log
                    .response_body
                    .as_deref()
                    .map(|s| truncate_str_safe(s, MAX_MEMORY_BODY_BYTES)),
                input_tokens: log.input_tokens,
                output_tokens: log.output_tokens,
                cached_tokens: log.cached_tokens,
                protocol: log.protocol.clone(),
                username: log.username.clone(),
            };

            let mut logs = self.logs.write().await;
            if logs.len() >= self.max_logs {
                logs.pop_back();
            }
            logs.push_front(memory_log);
        }

        // Emit event (send summary only, without body to reduce memory)
        if let Some(app) = &self.app_handle {
            let log_summary = ProxyRequestLog {
                id: log.id.clone(),
                timestamp: log.timestamp,
                method: log.method.clone(),
                url: log.url.clone(),
                status: log.status,
                duration: log.duration,
                model: log.model.clone(),
                mapped_model: log.mapped_model.clone(),
                account_email: log.account_email.clone(),
                client_ip: log.client_ip.clone(),
                error: log
                    .error
                    .as_deref()
                    .map(|s| truncate_str_safe(s, MAX_MEMORY_BODY_BYTES)),
                request_body: None,  // Don't send body in event
                response_body: None, // Don't send body in event
                input_tokens: log.input_tokens,
                output_tokens: log.output_tokens,
                cached_tokens: log.cached_tokens,
                protocol: log.protocol.clone(),
                username: log.username.clone(),
            };
            let _ = app.emit("proxy://request", &log_summary);
        }
        // Save to DB
        let log_to_save = log;
        tokio::task::spawn_blocking(move || {
            // Cancellation of the awaiting request must not release admission
            // while this blocking writer still owns the full body.
            let _permit = permit;
            if let Err(e) = crate::modules::proxy_db::save_log(&log_to_save) {
                tracing::error!("Failed to save proxy log to DB: {}", e);
            }

            // Sync to Security DB (IpAccessLogs) so it appears in Security Monitor
            if let Some(ip) = &log_to_save.client_ip {
                let security_log = crate::modules::security_db::IpAccessLog {
                    id: uuid::Uuid::new_v4().to_string(),
                    client_ip: ip.clone(),
                    timestamp: log_to_save.timestamp / 1000, // ms to s
                    method: Some(log_to_save.method.clone()),
                    path: Some(log_to_save.url.clone()),
                    user_agent: None, // We don't have UA in ProxyRequestLog easily accessible here without plumbing
                    status: Some(log_to_save.status as i32),
                    duration: Some(log_to_save.duration as i64),
                    api_key_hash: None,
                    blocked: false, // This comes from monitor, so it wasn't blocked by IP filter
                    block_reason: None,
                    username: log_to_save.username.clone(),
                };

                if let Err(e) = crate::modules::security_db::save_ip_access_log(&security_log) {
                    tracing::error!("Failed to save security log: {}", e);
                }
            }
        })
        .await
        .unwrap_or_else(|error| tracing::error!("Proxy log writer failed: {}", error));
    }

    pub async fn get_logs(&self, limit: usize) -> Vec<ProxyRequestLog> {
        // Try to get from DB first for true history
        let db_result =
            tokio::task::spawn_blocking(move || crate::modules::proxy_db::get_logs(limit)).await;

        match db_result {
            Ok(Ok(logs)) => logs,
            Ok(Err(e)) => {
                tracing::error!("Failed to get logs from DB: {}", e);
                // Fallback to memory
                let logs = self.logs.read().await;
                logs.iter().take(limit).cloned().collect()
            }
            Err(e) => {
                tracing::error!("Spawn blocking failed for get_logs: {}", e);
                let logs = self.logs.read().await;
                logs.iter().take(limit).cloned().collect()
            }
        }
    }

    pub async fn get_stats(&self) -> ProxyStats {
        let db_result = tokio::task::spawn_blocking(|| crate::modules::proxy_db::get_stats()).await;

        match db_result {
            Ok(Ok(stats)) => stats,
            Ok(Err(e)) => {
                tracing::error!("Failed to get stats from DB: {}", e);
                self.stats.read().await.clone()
            }
            Err(e) => {
                tracing::error!("Spawn blocking failed for get_stats: {}", e);
                self.stats.read().await.clone()
            }
        }
    }

    pub async fn get_logs_filtered(
        &self,
        page: usize,
        page_size: usize,
        search_text: Option<String>,
        level: Option<String>,
    ) -> Result<Vec<ProxyRequestLog>, String> {
        let offset = (page.max(1) - 1) * page_size;
        let errors_only = level.as_deref() == Some("error");
        let search = search_text.unwrap_or_default();

        let res = tokio::task::spawn_blocking(move || {
            crate::modules::proxy_db::get_logs_filtered(&search, errors_only, page_size, offset)
        })
        .await;

        match res {
            Ok(r) => r,
            Err(e) => Err(format!("Spawn blocking failed: {}", e)),
        }
    }

    pub async fn clear(&self) {
        let mut logs = self.logs.write().await;
        logs.clear();
        let mut stats = self.stats.write().await;
        *stats = ProxyStats::default();

        let _ = tokio::task::spawn_blocking(|| {
            if let Err(e) = crate::modules::proxy_db::clear_logs() {
                tracing::error!("Failed to clear logs in DB: {}", e);
            }
        })
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncate_str_safe_a1_a2() {
        // A1: 10 MiB 正文得到合法预览，capacity 达到 G1 上限；原正文不变
        let orig_10m = "A".repeat(10 * 1024 * 1024);
        let prev_10m = truncate_str_safe(&orig_10m, MAX_MEMORY_BODY_BYTES);
        assert!(prev_10m.ends_with(TRUNCATION_SUFFIX));
        assert_eq!(
            prev_10m.len(),
            MAX_MEMORY_BODY_BYTES + TRUNCATION_SUFFIX.len()
        );
        assert!(prev_10m.capacity() <= MAX_MEMORY_BODY_BYTES + TRUNCATION_SUFFIX.len());
        assert_eq!(orig_10m.len(), 10 * 1024 * 1024, "原 10 MiB 正文不被修改");

        // A2: 空/None/短文本大 capacity / 恰好上限 / 中文与 emoji 4 字节边界
        assert_eq!(truncate_str_safe("", 16384), "");

        let exact_str = "X".repeat(MAX_MEMORY_BODY_BYTES);
        let exact_prev = truncate_str_safe(&exact_str, MAX_MEMORY_BODY_BYTES);
        assert_eq!(exact_prev.len(), MAX_MEMORY_BODY_BYTES);
        assert!(!exact_prev.contains(TRUNCATION_SUFFIX));

        // 短文本带 10MB capacity 被紧凑化构造，不带入空闲容量
        let mut short_with_huge_cap = String::with_capacity(10 * 1024 * 1024);
        short_with_huge_cap.push_str("short payload");
        let short_prev = truncate_str_safe(&short_with_huge_cap, MAX_MEMORY_BODY_BYTES);
        assert_eq!(short_prev, "short payload");
        assert!(short_prev.capacity() < 64);

        // 中文多字节截断 (3 字节/字符)
        let cn_str = "这是一段测试中文文本，用于验证多字节截断安全";
        let cn_prev = truncate_str_safe(cn_str, 10);
        assert!(cn_prev.ends_with(TRUNCATION_SUFFIX));

        // Emoji 边界截断 (4 字节/字符: 🦀 = 0xF0 0x9F 0xA6 0x80)
        let emoji_str = "🦀".repeat(5000); // 20,000 字节
        let emoji_prev = truncate_str_safe(&emoji_str, MAX_MEMORY_BODY_BYTES);
        assert!(emoji_prev.ends_with(TRUNCATION_SUFFIX));
        assert!(emoji_prev.len() <= MAX_MEMORY_BODY_BYTES + TRUNCATION_SUFFIX.len());
    }

    #[tokio::test]
    async fn test_monitor_memory_log_capacity_and_truncation() {
        let monitor = ProxyMonitor::new(2, None);
        monitor.set_enabled(true);

        let mut log1 = ProxyRequestLog {
            id: "1".to_string(),
            timestamp: 100,
            method: "POST".to_string(),
            url: "/v1/chat".to_string(),
            status: 200,
            duration: 50,
            model: Some("test".to_string()),
            mapped_model: Some("test".to_string()),
            account_email: None,
            client_ip: None,
            request_body: Some("B".repeat(20 * 1024)),
            response_body: Some("C".repeat(30 * 1024)),
            input_tokens: None,
            output_tokens: None,
            cached_tokens: None,
            protocol: None,
            username: None,
            error: Some("E".repeat(1024 * 1024)),
        };

        monitor.log_request(log1.clone()).await;

        {
            let logs = monitor.logs.read().await;
            assert_eq!(logs.len(), 1);
            let first = &logs[0];
            assert!(
                first.error.as_ref().unwrap().capacity()
                    <= MAX_MEMORY_BODY_BYTES + TRUNCATION_SUFFIX.len()
            );
            assert!(first.request_body.as_ref().unwrap().contains("[TRUNCATED"));
            assert!(first.response_body.as_ref().unwrap().contains("[TRUNCATED"));
        }

        // 插入第2和第3条，验证最大容量为 2
        log1.id = "2".to_string();
        monitor.log_request(log1.clone()).await;
        log1.id = "3".to_string();
        monitor.log_request(log1.clone()).await;

        {
            let logs = monitor.logs.read().await;
            assert_eq!(logs.len(), 2);
            assert_eq!(logs[0].id, "3");
            assert_eq!(logs[1].id, "2");
        }
    }

    #[tokio::test]
    async fn test_monitor_db_persists_and_reads_back_full_body() {
        let monitor = ProxyMonitor::new(10, None);
        monitor.set_enabled(true);

        let unique_id = format!("test_db_full_{}", uuid::Uuid::new_v4());
        let full_req_body = "REQ_DATA_".repeat(5000); // 45KB
        let full_resp_body = "RESP_DATA_".repeat(6000); // 60KB

        let log = ProxyRequestLog {
            id: unique_id.clone(),
            timestamp: chrono::Utc::now().timestamp_millis(),
            method: "POST".to_string(),
            url: "/v1/chat/completions".to_string(),
            status: 200,
            duration: 120,
            model: Some("claude-3-5-sonnet".to_string()),
            mapped_model: Some("claude-3-5-sonnet".to_string()),
            account_email: None,
            client_ip: None,
            request_body: Some(full_req_body.clone()),
            response_body: Some(full_resp_body.clone()),
            input_tokens: Some(100),
            output_tokens: Some(200),
            cached_tokens: None,
            protocol: Some("claude".to_string()),
            username: None,
            error: None,
        };

        // 确保测试环境数据库与表结构初始化
        let _ = crate::modules::proxy_db::init_db();

        monitor.log_request(log).await;

        // 轮询等待异步持久化写入完成（最多等待 5 秒，不用固定 sleep）
        let mut db_entry = None;
        for _ in 0..50 {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            if let Ok(detail) = crate::modules::proxy_db::get_log_detail(&unique_id) {
                db_entry = Some(detail);
                break;
            }
        }
        let db_detail = db_entry.expect("DB entry must be persisted within timeout");

        // 验证内存中保留的是截断预览
        {
            let logs = monitor.logs.read().await;
            let mem_entry = logs
                .iter()
                .find(|l| l.id == unique_id)
                .expect("Memory entry must exist");
            assert!(mem_entry
                .request_body
                .as_ref()
                .unwrap()
                .contains("[TRUNCATED"));
            assert!(mem_entry
                .response_body
                .as_ref()
                .unwrap()
                .contains("[TRUNCATED"));
        }

        // 验证从 SQLite 数据库读回的是未截断的完整正文
        assert_eq!(
            db_detail.request_body.as_deref(),
            Some(full_req_body.as_str())
        );
        assert_eq!(
            db_detail.response_body.as_deref(),
            Some(full_resp_body.as_str())
        );
    }

    #[tokio::test]
    async fn test_monitor_100_logs_window_capacity_bounded() {
        let monitor = ProxyMonitor::new(100, None);
        monitor.set_enabled(true);

        for i in 0..150 {
            let mut req = String::with_capacity(1024 * 1024);
            req.push_str(&format!("req_{}_", i));
            req.push_str(&"x".repeat(100_000));

            let mut resp = String::with_capacity(1024 * 1024);
            resp.push_str(&format!("resp_{}_", i));
            resp.push_str(&"y".repeat(100_000));

            let log = ProxyRequestLog {
                id: format!("log_{}", i),
                timestamp: i as i64,
                method: "POST".to_string(),
                url: "/test".to_string(),
                status: 200,
                duration: 10,
                model: None,
                mapped_model: None,
                account_email: None,
                client_ip: None,
                request_body: Some(req),
                response_body: Some(resp),
                input_tokens: None,
                output_tokens: None,
                cached_tokens: None,
                protocol: None,
                username: None,
                error: None,
            };
            monitor.log_request(log).await;
        }

        let logs = monitor.logs.read().await;
        assert_eq!(logs.len(), 100);

        let total_cap: usize = logs
            .iter()
            .map(|l| {
                l.request_body.as_ref().map(|s| s.capacity()).unwrap_or(0)
                    + l.response_body.as_ref().map(|s| s.capacity()).unwrap_or(0)
            })
            .sum();

        // 100 logs * 2 bodies * (16KB + suffix) ≈ 3.3 MB max
        let max_expected = 100 * 2 * (MAX_MEMORY_BODY_BYTES + 64);
        assert!(
            total_cap <= max_expected,
            "Total in-memory capacity must be <= {}, but was {}",
            max_expected,
            total_cap
        );
        assert!(total_cap < 4 * 1024 * 1024);
    }
}
