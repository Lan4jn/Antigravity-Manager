use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tauri::Emitter;
use tokio::sync::{RwLock, Semaphore};

// Admission happens before spawn_blocking, so queued tasks cannot retain unlimited bodies.
static LOG_WRITERS: Semaphore = Semaphore::const_new(4);

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
    pub upstream_request_body: Option<String>, // 网关转出给上游(Antigravity)的报文
    pub response_body: Option<String>,
    #[serde(default)]
    pub request_headers: Option<String>,
    #[serde(default)]
    pub upstream_request_headers: Option<String>,
    #[serde(default)]
    pub response_headers: Option<String>,
    pub input_tokens: Option<u32>,
    pub output_tokens: Option<u32>,
    pub cached_tokens: Option<u32>,
    pub protocol: Option<String>, // 协议类型: "openai", "anthropic", "gemini"
    pub username: Option<String>, // User token username
    #[serde(default)]
    pub session_id: Option<String>, // 会话标识 (如 sess-xxx)
}

#[cfg(test)]
pub(crate) mod prompt_log_tests {
    use super::*;

    pub(crate) fn sample_log(id: &str, bytes: usize) -> ProxyRequestLog {
        serde_json::from_value(serde_json::json!({
            "id": id, "timestamp": chrono::Utc::now().timestamp_millis(),
            "method": "POST", "url": "/v1/chat/completions", "status": 500, "duration": 10,
            "request_body": "q".repeat(bytes), "response_body": "错".repeat(bytes),
            "error": "错".repeat(2048)
        }))
        .unwrap()
    }

    // Tests using ABV_DATA_DIR run serially and restore the previous process setting.
    // Unified with crate::modules::account::SHARED_TEST_ENV_LOCK to prevent split-brain mutex race.
    thread_local! {
        static REENTRANCY_DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    pub(crate) struct TestDataDir {
        _guard: Option<std::sync::MutexGuard<'static, ()>>,
        _dir: tempfile::TempDir,
        previous: Option<std::ffi::OsString>,
    }
    impl TestDataDir {
        pub(crate) fn new() -> Self {
            let depth = REENTRANCY_DEPTH.get();
            let guard = if depth == 0 {
                Some(
                    crate::modules::account::SHARED_TEST_ENV_LOCK
                        .lock()
                        .unwrap_or_else(|e| e.into_inner()),
                )
            } else {
                None
            };
            REENTRANCY_DEPTH.set(depth + 1);

            let dir = tempfile::tempdir().unwrap();
            let previous = std::env::var_os("ABV_DATA_DIR");
            std::env::set_var("ABV_DATA_DIR", dir.path());
            Self {
                _guard: guard,
                _dir: dir,
                previous,
            }
        }
    }
    impl Drop for TestDataDir {
        fn drop(&mut self) {
            if let Some(previous) = &self.previous {
                std::env::set_var("ABV_DATA_DIR", previous);
            } else {
                std::env::remove_var("ABV_DATA_DIR");
            }
            let depth = REENTRANCY_DEPTH.get();
            if depth > 0 {
                REENTRANCY_DEPTH.set(depth - 1);
            }
        }
    }

    #[tokio::test]
    async fn prompt_log_memory_summary_and_database_detail() {
        let _dir = TestDataDir::new();
        crate::modules::proxy_db::init_db().unwrap();
        let monitor = ProxyMonitor {
            logs: RwLock::new(VecDeque::new()),
            stats: RwLock::new(ProxyStats::default()),
            max_logs: 2,
            enabled: Arc::new(AtomicBool::new(true)),
            capture_health_logs: Arc::new(AtomicBool::new(false)),
            app_handle: None,
        };
        let log = sample_log("detail", 4096);
        let response = crate::proxy::payload_audit::apply_storage_mode_to_body(
            log.response_body.clone(),
            &crate::proxy::config::get_payload_storage_mode(),
        );
        monitor.log_request(log).await;
        let _finished = LOG_WRITERS.acquire_many(4).await.unwrap();
        let logs = monitor.logs.read().await;
        assert!(logs[0].request_body.is_none() && logs[0].response_body.is_none());
        assert_eq!(logs[0].error.as_ref().unwrap().chars().count(), 1024);
        let detail = crate::modules::proxy_db::get_log_detail("detail").unwrap();
        assert_eq!(detail.response_body, response);
        assert_eq!(
            detail.request_body.as_deref(),
            Some("q".repeat(4096).as_str())
        );
        monitor.set_enabled(false);
        monitor.log_request(sample_log("disabled", 4096)).await;
        assert!(crate::modules::proxy_db::get_log_detail("disabled").is_err());
    }
}

impl ProxyRequestLog {
    pub(crate) fn summary(&self) -> Self {
        Self {
            id: self.id.clone(),
            timestamp: self.timestamp,
            method: self.method.clone(),
            url: self.url.clone(),
            status: self.status,
            duration: self.duration,
            model: self.model.clone(),
            mapped_model: self.mapped_model.clone(),
            account_email: self.account_email.clone(),
            client_ip: self.client_ip.clone(),
            error: self
                .error
                .as_ref()
                .map(|error| error.chars().take(1024).collect()),
            request_body: None,
            upstream_request_body: None,
            response_body: None,
            request_headers: None,
            upstream_request_headers: None,
            response_headers: None,
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            cached_tokens: self.cached_tokens,
            protocol: self.protocol.clone(),
            username: self.username.clone(),
            session_id: self.session_id.clone(),
        }
    }
}

#[derive(Default)]
pub(crate) struct UpstreamCapture {
    body: Option<String>,
    headers: Option<String>,
}

#[derive(Clone, Default)]
pub struct UpstreamRequestBodyHolder(pub std::sync::Arc<std::sync::Mutex<UpstreamCapture>>);

tokio::task_local! {
    pub static CURRENT_UPSTREAM_CAPTURE: UpstreamRequestBodyHolder;
}

impl UpstreamRequestBodyHolder {
    pub fn new() -> Self {
        Self(std::sync::Arc::new(std::sync::Mutex::new(
            UpstreamCapture::default(),
        )))
    }

    pub fn set(&self, body: String) {
        if let Ok(mut lock) = self.0.lock() {
            lock.body = Some(body);
        }
    }

    pub fn set_headers_json(&self, headers: String) {
        if let Ok(mut lock) = self.0.lock() {
            lock.headers = Some(headers);
        }
    }

    pub fn set_value(&self, val: &serde_json::Value) {
        let clean = sanitize_upstream_debug_value(val);
        if let Ok(s) = serde_json::to_string(&clean) {
            self.set(s);
        }
    }

    pub fn take(&self) -> Option<String> {
        self.0.lock().ok().and_then(|mut g| g.body.take())
    }

    pub fn take_headers(&self) -> Option<String> {
        self.0.lock().ok().and_then(|mut g| g.headers.take())
    }
}

fn sanitize_upstream_debug_value(val: &serde_json::Value) -> serde_json::Value {
    match val {
        serde_json::Value::String(s)
            if s.starts_with("data:image/") || s.starts_with("data:audio/") =>
        {
            serde_json::Value::String(format!("[inline data omitted: {} chars]", s.len()))
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(sanitize_upstream_debug_value).collect())
        }
        serde_json::Value::Object(map) => {
            let is_inline = map
                .get("mimeType")
                .and_then(serde_json::Value::as_str)
                .is_some()
                && map
                    .get("data")
                    .and_then(serde_json::Value::as_str)
                    .is_some();
            serde_json::Value::Object(
                map.iter()
                    .map(|(k, v)| {
                        let v = if is_inline && k == "data" {
                            serde_json::Value::String(format!(
                                "[inline data omitted: {} chars]",
                                v.as_str().map(str::len).unwrap_or_default()
                            ))
                        } else {
                            sanitize_upstream_debug_value(v)
                        };
                        (k.clone(), v)
                    })
                    .collect(),
            )
        }
        _ => val.clone(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProxyStats {
    pub total_requests: u64,
    pub success_count: u64,
    pub error_count: u64,
}

pub struct ProxyMonitor {
    pub logs: RwLock<VecDeque<ProxyRequestLog>>,
    pub stats: RwLock<ProxyStats>,
    pub max_logs: usize,
    pub enabled: Arc<AtomicBool>,
    pub capture_health_logs: Arc<AtomicBool>,
    app_handle: Option<tauri::AppHandle>,
}

impl ProxyMonitor {
    pub fn new(max_logs: usize, app_handle: Option<tauri::AppHandle>) -> Self {
        // Initialize DB
        if let Err(e) = crate::modules::proxy_db::init_db() {
            tracing::error!("Failed to initialize proxy DB: {}", e);
        }

        let thinking_days = crate::proxy::config::get_thinking_retention_days() as i64;
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
            match crate::modules::proxy_db::cleanup_old_thinking_records(thinking_days) {
                Ok(deleted) => {
                    if deleted > 0 {
                        tracing::info!(
                            "Auto cleanup: removed {} old thinking/signature records (>{} days)",
                            deleted,
                            thinking_days
                        );
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to cleanup thinking records: {}", e);
                }
            }
            crate::modules::logger::sync_internal_error_log_budget_from_config();
            if let Err(e) = crate::modules::logger::apply_internal_error_log_retention() {
                tracing::error!("Failed to apply internal error log retention: {}", e);
            }
        });

        tokio::spawn(async {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(3600));
            interval.tick().await;
            loop {
                interval.tick().await;
                let thinking_days = crate::proxy::config::get_thinking_retention_days() as i64;
                let retention = crate::modules::config::load_app_config()
                    .map(|config| config.proxy.log_retention)
                    .unwrap_or_default();
                let result = tokio::task::spawn_blocking(move || {
                    let retention_res = crate::modules::proxy_db::apply_retention(&retention);
                    let thinking_res =
                        crate::modules::proxy_db::cleanup_old_thinking_records(thinking_days);
                    crate::modules::logger::sync_internal_error_log_budget_from_config();
                    let error_log_res =
                        crate::modules::logger::apply_internal_error_log_retention();
                    (retention_res, thinking_res, error_log_res)
                })
                .await;
                match result {
                    Ok((retention_res, thinking_res, error_log_res)) => {
                        match retention_res {
                            Ok((cleared, deleted)) => {
                                if cleared > 0 || deleted > 0 {
                                    tracing::info!(
                                        "Proxy log retention: cleared {} bodies, deleted {} rows",
                                        cleared,
                                        deleted
                                    );
                                }
                            }
                            Err(error) => {
                                tracing::error!("Failed to apply proxy log retention: {}", error)
                            }
                        }
                        if let Ok(deleted) = thinking_res {
                            if deleted > 0 {
                                tracing::info!(
                                    "Auto cleanup: removed {} old thinking/signature records",
                                    deleted
                                );
                            }
                        } else if let Err(e) = thinking_res {
                            tracing::error!("Failed to cleanup thinking records: {}", e);
                        }
                        if let Err(e) = error_log_res {
                            tracing::error!("Failed to apply internal error log retention: {}", e);
                        }
                    }
                    Err(error) => tracing::error!("Proxy log retention task failed: {}", error),
                }
            }
        });

        Self {
            logs: RwLock::new(VecDeque::with_capacity(max_logs)),
            stats: RwLock::new(ProxyStats::default()),
            max_logs,
            enabled: Arc::new(AtomicBool::new(false)), // Default to disabled
            capture_health_logs: Arc::new(AtomicBool::new(false)), // Default to false
            app_handle,
        }
    }

    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn set_capture_health_logs(&self, enabled: bool) {
        self.capture_health_logs.store(enabled, Ordering::Relaxed);
    }

    pub fn is_capture_health_logs(&self) -> bool {
        self.capture_health_logs.load(Ordering::Relaxed)
    }

    pub async fn log_request(&self, log: ProxyRequestLog) {
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

        let summary = log.summary();
        // Add only the same summary used by the frontend to memory.
        {
            let mut logs = self.logs.write().await;
            if logs.len() >= self.max_logs {
                logs.pop_back();
            }
            logs.push_front(summary.clone());
        }

        if let Some(app) = &self.app_handle {
            let _ = app.emit("proxy://request", &summary);
        }

        let Ok(permit) = LOG_WRITERS.try_acquire() else {
            tracing::debug!("Skipping proxy log persistence: writers busy");
            return;
        };
        // Save to DB (respect server-side simple/full storage mode)
        let mut log_to_save = log;
        let storage_mode = crate::proxy::config::get_payload_storage_mode();
        log_to_save.request_body = crate::proxy::payload_audit::apply_storage_mode_to_body(
            log_to_save.request_body,
            &storage_mode,
        );
        log_to_save.upstream_request_body = crate::proxy::payload_audit::apply_storage_mode_to_body(
            log_to_save.upstream_request_body,
            &storage_mode,
        );
        log_to_save.response_body = crate::proxy::payload_audit::apply_storage_mode_to_body(
            log_to_save.response_body,
            &storage_mode,
        );

        let enabled = Arc::clone(&self.enabled);
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            if !enabled.load(Ordering::Relaxed) {
                return;
            }
            if let Err(e) = crate::modules::proxy_db::save_log(log_to_save) {
                tracing::error!("Failed to save proxy log to DB: {}", e);
            }

            // Sync to Security DB (IpAccessLogs) so it appears in Security Monitor
            if let Some(ip) = &summary.client_ip {
                let security_log = crate::modules::security_db::IpAccessLog {
                    id: uuid::Uuid::new_v4().to_string(),
                    client_ip: ip.clone(),
                    timestamp: summary.timestamp / 1000, // ms to s
                    method: Some(summary.method.clone()),
                    path: Some(summary.url.clone()),
                    user_agent: None, // We don't have UA in ProxyRequestLog easily accessible here without plumbing
                    status: Some(summary.status as i32),
                    duration: Some(summary.duration as i64),
                    api_key_hash: None,
                    blocked: false, // This comes from monitor, so it wasn't blocked by IP filter
                    block_reason: None,
                    username: summary.username.clone(),
                };

                if let Err(e) = crate::modules::security_db::save_ip_access_log(&security_log) {
                    tracing::error!("Failed to save security log: {}", e);
                }
            }
        });
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
    fn test_summary_preserves_metadata_without_payload_allocations() {
        let mut log = prompt_log_tests::sample_log("summary", 4096);
        log.upstream_request_body = Some("upstream".repeat(4096));
        log.request_headers = Some("request headers".to_string());
        log.upstream_request_headers = Some("upstream headers".to_string());
        log.response_headers = Some("response headers".to_string());
        log.session_id = Some("session-summary".to_string());
        log.input_tokens = Some(10000);
        log.output_tokens = Some(150);
        log.cached_tokens = Some(8800);
        let summary = log.summary();
        for body in [
            &summary.request_body,
            &summary.upstream_request_body,
            &summary.response_body,
            &summary.request_headers,
            &summary.upstream_request_headers,
            &summary.response_headers,
        ] {
            assert!(body.is_none());
        }
        assert_eq!(summary.session_id, log.session_id);
        assert_eq!(summary.duration, log.duration);
        assert_eq!(summary.input_tokens, log.input_tokens);
        assert_eq!(summary.output_tokens, log.output_tokens);
        assert_eq!(summary.cached_tokens, log.cached_tokens);
        assert_eq!(summary.error.as_ref().unwrap().chars().count(), 1024);
        assert!(log.request_body.is_some());
        assert!(log.upstream_request_headers.is_some());
    }

    #[tokio::test]
    async fn test_monitor_busy_writers_keep_summary_and_resume_persistence() {
        use crate::proxy::config;
        use std::time::Duration;
        use tokio::time::timeout;

        let _dir = prompt_log_tests::TestDataDir::new();
        struct RestoreAuditConfig(String, u32, bool, u32, u32);
        impl RestoreAuditConfig {
            fn set_storage_mode(&self, mode: &str) {
                config::update_global_audit_config(
                    mode.to_string(),
                    self.1,
                    self.2,
                    self.3,
                    Some(self.4),
                );
            }
        }
        impl Drop for RestoreAuditConfig {
            fn drop(&mut self) {
                self.set_storage_mode(&self.0);
            }
        }
        let audit_config = RestoreAuditConfig(
            config::get_payload_storage_mode(),
            config::get_log_retention_days(),
            config::is_thinking_store_enabled(),
            config::get_thinking_retention_days(),
            config::get_thinking_max_memory_turns() as u32,
        );
        crate::modules::proxy_db::init_db().unwrap();
        let monitor = ProxyMonitor {
            logs: RwLock::new(VecDeque::new()),
            stats: RwLock::new(ProxyStats::default()),
            max_logs: 4,
            enabled: Arc::new(AtomicBool::new(true)),
            capture_health_logs: Arc::new(AtomicBool::new(false)),
            app_handle: None,
        };
        let body = "原文🦀".repeat(4000);
        for mode in ["full", "simple"] {
            audit_config.set_storage_mode(mode);
            let busy_id = format!("busy_{mode}_{}", uuid::Uuid::new_v4());
            let permits = timeout(Duration::from_secs(5), LOG_WRITERS.acquire_many(4))
                .await
                .expect("existing log writers did not finish")
                .unwrap();
            let mut busy = prompt_log_tests::sample_log(&busy_id, 4096);
            busy.response_body = Some(body.clone());
            busy.cached_tokens = Some(8800);
            timeout(Duration::from_secs(1), monitor.log_request(busy))
                .await
                .expect("busy writers must not block log_request");
            {
                let logs = monitor.logs.read().await;
                let summary = logs
                    .iter()
                    .find(|log| log.id == busy_id)
                    .expect("writer admission must happen after memory summary insertion");
                assert!(summary.request_body.is_none());
                assert!(summary.response_body.is_none());
                assert_eq!(summary.cached_tokens, Some(8800));
            }
            assert!(crate::modules::proxy_db::get_log_detail(&busy_id).is_err());
            drop(permits);

            let accepted_id = format!("accepted_{mode}_{}", uuid::Uuid::new_v4());
            let mut accepted = prompt_log_tests::sample_log(&accepted_id, 4096);
            accepted.request_body = Some(body.clone());
            accepted.upstream_request_body = Some(body.clone());
            accepted.response_body = Some(body.clone());
            timeout(Duration::from_secs(1), monitor.log_request(accepted))
                .await
                .expect("free writers must accept a new log");
            let _finished = timeout(Duration::from_secs(5), LOG_WRITERS.acquire_many(4))
                .await
                .expect("accepted log writer did not finish")
                .unwrap();
            let detail = crate::modules::proxy_db::get_log_detail(&accepted_id)
                .expect("new request must persist after writer permits are released");
            let expected = if mode == "full" {
                body.clone()
            } else {
                format!("{}…", body.chars().take(8000).collect::<String>())
            };
            assert_eq!(detail.request_body.as_deref(), Some(expected.as_str()));
            assert_eq!(
                detail.upstream_request_body.as_deref(),
                Some(expected.as_str())
            );
            assert_eq!(detail.response_body.as_deref(), Some(expected.as_str()));
            assert!(
                crate::modules::proxy_db::get_log_detail(&busy_id).is_err(),
                "the dropped busy log must not be queued for later persistence"
            );
        }
    }

    #[tokio::test]
    async fn test_monitor_memory_log_capacity_and_summary() {
        let _dir = prompt_log_tests::TestDataDir::new();
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
            upstream_request_body: None,
            request_headers: None,
            upstream_request_headers: None,
            response_headers: None,
            session_id: None,
            input_tokens: None,
            output_tokens: None,
            cached_tokens: None,
            protocol: None,
            username: None,
            error: None,
        };

        monitor.log_request(log1.clone()).await;

        {
            let logs = monitor.logs.read().await;
            assert_eq!(logs.len(), 1);
            let first = &logs[0];
            assert!(first.request_body.is_none());
            assert!(first.response_body.is_none());
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
        let _finished = LOG_WRITERS.acquire_many(4).await.unwrap();
    }

    #[tokio::test]
    async fn test_monitor_db_persists_and_reads_back_full_body() {
        let _dir = prompt_log_tests::TestDataDir::new();
        let monitor = ProxyMonitor::new(10, None);
        monitor.set_enabled(true);

        let unique_id = format!("test_db_full_{}", uuid::Uuid::new_v4());
        let full_req_body =
            serde_json::to_string_pretty(&serde_json::json!({"content": "REQ_DATA_".repeat(5000)}))
                .unwrap(); // 45KB
        let full_resp_body = serde_json::to_string_pretty(
            &serde_json::json!({"content": "RESP_DATA_".repeat(6000)}),
        )
        .unwrap(); // 60KB

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
            upstream_request_body: Some("upstream complete body".to_string()),
            request_headers: Some("{\"x-request\":\"full\"}".to_string()),
            upstream_request_headers: Some("{\"x-upstream\":\"full\"}".to_string()),
            response_headers: Some("{\"x-timing-total-ms\":\"120.000\"}".to_string()),
            session_id: Some("session-full".to_string()),
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

        // 内存与事件统一为summary；DB保留完整正文和headers。
        {
            let logs = monitor.logs.read().await;
            let mem_entry = logs
                .iter()
                .find(|l| l.id == unique_id)
                .expect("Memory entry must exist");
            assert!(mem_entry.request_body.is_none());
            assert!(mem_entry.response_body.is_none());
            assert!(mem_entry.response_headers.is_none());
            assert_eq!(mem_entry.session_id.as_deref(), Some("session-full"));
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
        assert_eq!(
            db_detail.upstream_request_body.as_deref(),
            Some("upstream complete body")
        );
        assert_eq!(
            db_detail.request_headers.as_deref(),
            Some("{\"x-request\":\"full\"}")
        );
        assert_eq!(
            db_detail.upstream_request_headers.as_deref(),
            Some("{\"x-upstream\":\"full\"}")
        );
        assert_eq!(
            db_detail.response_headers.as_deref(),
            Some("{\"x-timing-total-ms\":\"120.000\"}")
        );
        assert_eq!(db_detail.session_id.as_deref(), Some("session-full"));
    }

    #[tokio::test]
    async fn test_monitor_100_logs_window_capacity_bounded() {
        let _dir = prompt_log_tests::TestDataDir::new();
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
                upstream_request_body: None,
                request_headers: None,
                upstream_request_headers: None,
                response_headers: None,
                session_id: None,
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

        assert_eq!(
            total_cap, 0,
            "Summary window must retain no body allocations"
        );
        drop(logs);
        let _finished = LOG_WRITERS.acquire_many(4).await.unwrap();
    }
}
