use crate::proxy::middleware::auth::UserTokenIdentity;
use crate::proxy::monitor::{ProxyRequestLog, UpstreamRequestBodyHolder};
use crate::proxy::server::AppState;
use axum::{
    body::Body,
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use base64::Engine as _;
use futures::{Stream, StreamExt};
use serde_json::Value;
use std::time::Instant;
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamFailureInjection {
    None,
    FailCreate,
    FailWrite,
    FailFlush,
    FailSeek,
    FailRead,
}

#[cfg(test)]
pub struct FailureInjectionGuard(StreamFailureInjection);

#[cfg(test)]
impl FailureInjectionGuard {
    pub fn new(mode: StreamFailureInjection) -> Self {
        set_stream_failure_injection(mode);
        Self(mode)
    }
}

#[cfg(test)]
impl Drop for FailureInjectionGuard {
    fn drop(&mut self) {
        set_stream_failure_injection(StreamFailureInjection::None);
    }
}

#[cfg(test)]
static STREAM_FAILURE_INJECTION: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

#[cfg(test)]
pub fn set_stream_failure_injection(mode: StreamFailureInjection) {
    let val = match mode {
        StreamFailureInjection::None => 0,
        StreamFailureInjection::FailCreate => 1,
        StreamFailureInjection::FailWrite => 2,
        StreamFailureInjection::FailFlush => 3,
        StreamFailureInjection::FailSeek => 4,
        StreamFailureInjection::FailRead => 5,
    };
    STREAM_FAILURE_INJECTION.store(val, std::sync::atomic::Ordering::SeqCst);
}

#[cfg(test)]
fn get_stream_failure_injection() -> StreamFailureInjection {
    match STREAM_FAILURE_INJECTION.load(std::sync::atomic::Ordering::SeqCst) {
        1 => StreamFailureInjection::FailCreate,
        2 => StreamFailureInjection::FailWrite,
        3 => StreamFailureInjection::FailFlush,
        4 => StreamFailureInjection::FailSeek,
        5 => StreamFailureInjection::FailRead,
        _ => StreamFailureInjection::None,
    }
}

#[cfg(test)]
pub static CREATED_TEMP_PATHS: parking_lot::Mutex<Vec<std::path::PathBuf>> =
    parking_lot::Mutex::new(Vec::new());

#[cfg(test)]
pub fn get_created_temp_paths() -> Vec<std::path::PathBuf> {
    CREATED_TEMP_PATHS.lock().clone()
}

#[cfg(test)]
pub fn clear_created_temp_paths() {
    CREATED_TEMP_PATHS.lock().clear();
}

fn create_named_temp_file() -> std::io::Result<tempfile::NamedTempFile> {
    #[cfg(test)]
    if get_stream_failure_injection() == StreamFailureInjection::FailCreate {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "Simulated tempfile creation failure",
        ));
    }
    let file = tempfile::NamedTempFile::new()?;
    #[cfg(test)]
    {
        CREATED_TEMP_PATHS.lock().push(file.path().to_path_buf());
    }
    Ok(file)
}

struct SpillWriter {
    inner: std::io::BufWriter<tempfile::NamedTempFile>,
    bytes_written: usize,
}

impl std::io::Write for SpillWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        #[cfg(test)]
        if get_stream_failure_injection() == StreamFailureInjection::FailWrite
            && self.bytes_written > 1024 * 1024
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Simulated disk write failure",
            ));
        }
        let n = self.inner.write(buf)?;
        self.bytes_written += n;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        #[cfg(test)]
        if get_stream_failure_injection() == StreamFailureInjection::FailFlush {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Simulated flush failure",
            ));
        }
        self.inner.flush()
    }
}

fn seek_and_open_reader(
    mut temp_file: tempfile::NamedTempFile,
) -> std::io::Result<Box<dyn std::io::BufRead + Send>> {
    #[cfg(test)]
    if get_stream_failure_injection() == StreamFailureInjection::FailSeek {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Simulated seek failure",
        ));
    }
    use std::io::Seek;
    temp_file.seek(std::io::SeekFrom::Start(0))?;

    #[cfg(test)]
    if get_stream_failure_injection() == StreamFailureInjection::FailRead {
        struct FailingRead(tempfile::NamedTempFile);
        impl std::io::Read for FailingRead {
            fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Simulated read failure mid-stream",
                ))
            }
        }
        return Ok(Box::new(std::io::BufReader::new(FailingRead(temp_file))));
    }

    Ok(Box::new(std::io::BufReader::new(temp_file)))
}

const MAX_REQUEST_LOG_SIZE: usize = 100 * 1024 * 1024; // 100MB: HTTP payload forwarding limit (not logging budget)
const MAX_RESPONSE_LOG_SIZE: usize = 100 * 1024 * 1024; // 100MB: HTTP payload forwarding limit (not logging budget)
const MAX_LOGGED_FIELD_CHARS: usize = 500;

async fn next_chunk_while_receiver_open<S, T>(
    stream: &mut S,
    tx: &tokio::sync::mpsc::Sender<T>,
) -> Option<S::Item>
where
    S: Stream + Unpin,
{
    tokio::select! {
        biased;
        _ = tx.closed() => None,
        chunk = stream.next() => chunk,
    }
}

fn truncate_for_log(value: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for (idx, ch) in value.chars().enumerate() {
        if idx >= max_chars {
            out.push_str("...");
            return out;
        }
        out.push(ch);
    }
    out
}

fn extract_quoted_param(header: &str, key: &str) -> Option<String> {
    let needle = format!("{}=\"", key);
    let start = header.find(&needle)? + needle.len();
    let rest = &header[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

fn extract_boundary(content_type: &str) -> Option<String> {
    content_type.split(';').find_map(|part| {
        let trimmed = part.trim();
        let value = trimmed.strip_prefix("boundary=")?;
        Some(value.trim_matches('"').to_string())
    })
}

/// 噪音请求日志抑制判定。
///
/// 面板「捕获健康请求」胶囊开关（`capture_health_logs`）**默认关闭**。关闭时：
/// **所有 GET 成功请求一律不记录、不落库**（含 `/v1/models`、`/v1beta/models`、`/v1/models/claude`
/// 等模型列表轮询，以及 `/health` `/healthz` `/api/health` 探针）。
///
/// 之所以从"仅过滤 `/health` 探针"放宽到"全部 GET 成功"：真实客户端的健康/能力探测会高频轮询
/// **模型列表**接口（issue #3498 的探针刷屏 + 面板中 `/v1/models` 200 连片淹没业务日志），
/// 这类请求没有对话语义、无业务价值，却会把真实业务日志挤出视图。
///
/// 边界（刻意保守，绝不影响排障）：
/// - 失败请求（非 2xx）**始终记录**，无论路径与方法；
/// - 非 GET（POST 等真实业务请求）**始终记录**；
/// - 开关开启（或环境变量 `ABV_LOG_HEALTH_CHECKS` 为真）时全部记录并落库。
fn should_skip_request_log(
    method: &str,
    status: axum::http::StatusCode,
    capture_enabled: bool,
) -> bool {
    !capture_enabled && method.eq_ignore_ascii_case("GET") && status.is_success()
}

fn should_log_health_checks() -> bool {
    std::env::var("ABV_LOG_HEALTH_CHECKS")
        .map(|val| {
            let v = val.trim().to_ascii_lowercase();
            v == "1" || v == "true" || v == "yes" || v == "on"
        })
        .unwrap_or(false)
}

fn find_subslice(haystack: &[u8], needle: &[u8], start: usize) -> Option<usize> {
    if needle.is_empty() || start >= haystack.len() {
        return None;
    }
    haystack[start..]
        .windows(needle.len())
        .position(|window| window == needle)
        .map(|idx| start + idx)
}

fn trim_part_tail(mut part: &[u8]) -> &[u8] {
    if part.ends_with(b"\r\n") {
        part = &part[..part.len() - 2];
    } else if part.ends_with(b"\n") {
        part = &part[..part.len() - 1];
    }
    part
}

fn summarize_multipart_request(
    bytes: &[u8],
    content_type: &str,
    uri: &str,
) -> Option<(String, Option<String>)> {
    let boundary = extract_boundary(content_type)?;
    let marker = format!("--{}", boundary).into_bytes();
    let mut cursor = find_subslice(bytes, &marker, 0)?;
    let mut fields = serde_json::Map::new();
    let mut files = Vec::new();
    let mut model = None;

    loop {
        cursor += marker.len();
        if cursor >= bytes.len() || bytes[cursor..].starts_with(b"--") {
            break;
        }
        if bytes[cursor..].starts_with(b"\r\n") {
            cursor += 2;
        } else if bytes[cursor..].starts_with(b"\n") {
            cursor += 1;
        }

        let Some(next_boundary) = find_subslice(bytes, &marker, cursor) else {
            break;
        };
        let part = trim_part_tail(&bytes[cursor..next_boundary]);
        cursor = next_boundary;

        let (headers, body) = if let Some(idx) = find_subslice(part, b"\r\n\r\n", 0) {
            (&part[..idx], &part[idx + 4..])
        } else if let Some(idx) = find_subslice(part, b"\n\n", 0) {
            (&part[..idx], &part[idx + 2..])
        } else {
            continue;
        };

        let headers_text = String::from_utf8_lossy(headers);
        let disposition = headers_text
            .lines()
            .find(|line| {
                line.to_ascii_lowercase()
                    .starts_with("content-disposition:")
            })
            .unwrap_or("");
        let content_type = headers_text
            .lines()
            .find(|line| line.to_ascii_lowercase().starts_with("content-type:"))
            .and_then(|line| {
                line.split_once(':')
                    .map(|(_, value)| value.trim().to_string())
            });
        let Some(name) = extract_quoted_param(disposition, "name") else {
            continue;
        };
        let filename = extract_quoted_param(disposition, "filename");

        if filename.is_some() || content_type.as_deref().unwrap_or("").starts_with("image/") {
            files.push(serde_json::json!({
                "field": name,
                "filename": filename,
                "content_type": content_type,
                "bytes": body.len()
            }));
        } else if let Ok(text) = std::str::from_utf8(body) {
            let value = truncate_for_log(text.trim(), MAX_LOGGED_FIELD_CHARS);
            if name == "model" {
                model = Some(value.clone());
            }
            fields.insert(name, Value::String(value));
        } else {
            fields.insert(
                name,
                Value::String(format!("[binary field: {} bytes]", body.len())),
            );
        }
    }

    let summary = serde_json::json!({
        "content_type": "multipart/form-data",
        "path": uri,
        "fields": fields,
        "files": files,
        "raw_bytes": bytes.len()
    });
    let rendered = serde_json::to_string_pretty(&summary).ok()?;
    Some((rendered, model))
}

fn image_mime_and_dimensions(bytes: &[u8]) -> (String, Option<(u32, u32)>) {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") && bytes.len() >= 24 {
        let width = u32::from_be_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]);
        let height = u32::from_be_bytes([bytes[20], bytes[21], bytes[22], bytes[23]]);
        return ("image/png".to_string(), Some((width, height)));
    }

    if bytes.starts_with(b"\xff\xd8") {
        let mut idx = 2;
        while idx + 9 < bytes.len() {
            if bytes[idx] != 0xff {
                idx += 1;
                continue;
            }
            let marker = bytes[idx + 1];
            if marker == 0xd9 || marker == 0xda {
                break;
            }
            if idx + 4 > bytes.len() {
                break;
            }
            let segment_len = u16::from_be_bytes([bytes[idx + 2], bytes[idx + 3]]) as usize;
            if segment_len < 2 || idx + 2 + segment_len > bytes.len() {
                break;
            }
            if matches!(
                marker,
                0xc0 | 0xc1
                    | 0xc2
                    | 0xc3
                    | 0xc5
                    | 0xc6
                    | 0xc7
                    | 0xc9
                    | 0xca
                    | 0xcb
                    | 0xcd
                    | 0xce
                    | 0xcf
            ) && segment_len >= 7
            {
                let height = u16::from_be_bytes([bytes[idx + 5], bytes[idx + 6]]) as u32;
                let width = u16::from_be_bytes([bytes[idx + 7], bytes[idx + 8]]) as u32;
                return ("image/jpeg".to_string(), Some((width, height)));
            }
            idx += 2 + segment_len;
        }
        return ("image/jpeg".to_string(), None);
    }

    ("application/octet-stream".to_string(), None)
}

fn summarize_image_json_response(json: &Value) -> Option<String> {
    let mut summary = json.clone();
    let data = summary.get_mut("data")?.as_array_mut()?;
    for item in data.iter_mut() {
        if let Some(obj) = item.as_object_mut() {
            if let Some(b64) = obj.get("b64_json").and_then(|v| v.as_str()) {
                let decoded = base64::engine::general_purpose::STANDARD.decode(b64).ok();
                let (mime_type, dimensions, bytes) = decoded
                    .as_deref()
                    .map(|bytes| {
                        let (mime, dims) = image_mime_and_dimensions(bytes);
                        (mime, dims, bytes.len())
                    })
                    .unwrap_or_else(|| ("unknown".to_string(), None, b64.len() * 3 / 4));
                obj.remove("b64_json");
                obj.insert(
                    "image".to_string(),
                    serde_json::json!({
                        "encoding": "base64",
                        "redacted": true,
                        "mime_type": mime_type,
                        "bytes": bytes,
                        "dimensions": dimensions.map(|(width, height)| serde_json::json!({
                            "width": width,
                            "height": height
                        }))
                    }),
                );
            } else if let Some(url) = obj.get("url").and_then(|v| v.as_str()) {
                if url.starts_with("data:image/") {
                    obj.insert(
                        "url".to_string(),
                        Value::String(format!("[data URL redacted: {} chars]", url.len())),
                    );
                }
            }
        }
    }
    serde_json::to_string_pretty(&summary).ok()
}

/// Helper function to record User Token usage
fn record_user_token_usage(
    user_token_identity: &Option<UserTokenIdentity>,
    log: &ProxyRequestLog,
    user_agent: Option<String>,
) {
    if let Some(identity) = user_token_identity {
        let _ = crate::modules::user_token_db::record_token_usage_and_ip(
            &identity.token_id,
            log.client_ip.as_deref().unwrap_or("127.0.0.1"),
            log.model.as_deref().unwrap_or("unknown"),
            log.input_tokens.unwrap_or(0) as i32,
            log.output_tokens.unwrap_or(0) as i32,
            log.status as u16,
            user_agent,
        );
    }
}

fn extract_cached_tokens(usage: &Value) -> Option<u32> {
    let c = crate::proxy::pipeline::CanonicalUsage::from_gemini(usage);
    if c.cached_tokens > 0 {
        Some(c.cached_tokens)
    } else {
        None
    }
}

fn extract_input_tokens(usage: &Value) -> Option<u32> {
    let has_field = usage.get("prompt_tokens").is_some()
        || usage.get("input_tokens").is_some()
        || usage.get("total_input_tokens").is_some()
        || usage.get("promptTokenCount").is_some();
    if !has_field {
        return None;
    }
    let c = crate::proxy::pipeline::CanonicalUsage::from_gemini(usage);
    Some(c.total_input_tokens)
}

fn extract_reasoning_tokens(usage: &Value) -> Option<u32> {
    let c = crate::proxy::pipeline::CanonicalUsage::from_gemini(usage);
    if c.reasoning_tokens > 0 {
        Some(c.reasoning_tokens)
    } else {
        None
    }
}

fn extract_output_tokens(usage: &Value) -> Option<u32> {
    let has_field = usage.get("completion_tokens").is_some()
        || usage.get("output_tokens").is_some()
        || usage.get("total_output_tokens").is_some()
        || usage.get("candidatesTokenCount").is_some();
    if !has_field {
        return None;
    }
    let c = crate::proxy::pipeline::CanonicalUsage::from_gemini(usage);
    Some(c.output_tokens)
}

fn build_canonical_consolidated_response(
    thinking_content: String,
    mut thinking_signature: String,
    response_content: String,
    tool_calls: Vec<Value>,
    session_id: Option<&str>,
    log_id: &str,
    timing_obj: serde_json::Map<String, Value>,
    usage_obj: Option<serde_json::Map<String, Value>>,
) -> Value {
    // 权威网关签名回填：若当前签名为空，尝试通过工具调用ID或会话ID从网关状态机(内存 L1 / SQLite L2 DB 思考持久化)恢复
    if thinking_signature.is_empty() {
        for tc in &tool_calls {
            if let Some(call_id) = tc.get("id").and_then(|v| v.as_str()) {
                if !call_id.is_empty() {
                    if let Some(sig) = session_id.and_then(|sid| {
                        crate::proxy::SignatureCache::global().get_tool_signature(sid, call_id)
                    }) {
                        thinking_signature = sig;
                        break;
                    }
                }
            }
        }
        if thinking_signature.is_empty() {
            // 1. 优先按当前轮次的思考文本片段精准直捞专属签名
            if !thinking_content.is_empty() {
                let trimmed = thinking_content.trim();
                let snippet = crate::proxy::mappers::common_utils::safe_truncate_str(trimmed, 32);
                if let Some(sig) =
                    crate::modules::proxy_db::lookup_signature_by_thought_snippet(snippet)
                {
                    thinking_signature = sig;
                }
            }

            // 2. 兜底按会话状态机与会话数据库查找最新签名
            if thinking_signature.is_empty() {
                if let Some(sid) = session_id {
                    if let Some(sig) =
                        crate::proxy::SignatureCache::global().get_session_signature(sid)
                    {
                        thinking_signature = sig;
                    } else if let Some(sig) =
                        crate::modules::proxy_db::lookup_latest_thinking_signature(sid)
                    {
                        thinking_signature = sig;
                    }
                }
            }
        }
    }

    let mut consolidated = serde_json::Map::new();

    // 1. 会话/思考唯一标识
    let candidate_id = session_id.unwrap_or(log_id);
    consolidated.insert(
        "_session_thinking_id".to_string(),
        Value::String(candidate_id.to_string()),
    );

    // 2. 思考文本与权威签名
    if !thinking_content.is_empty() {
        consolidated.insert("thinking".to_string(), Value::String(thinking_content));
    }
    if !thinking_signature.is_empty() {
        consolidated.insert(
            "thinking_signature".to_string(),
            Value::String(thinking_signature),
        );
    }

    // 3. 正文输出
    if !response_content.is_empty() {
        consolidated.insert("content".to_string(), Value::String(response_content));
    }

    // 4. 工具调用列表 (过滤 Null)
    let clean_tool_calls: Vec<Value> = tool_calls.into_iter().filter(|v| !v.is_null()).collect();
    if !clean_tool_calls.is_empty() {
        consolidated.insert("tool_calls".to_string(), Value::Array(clean_tool_calls));
    }

    // 5. 耗时诊断
    if !timing_obj.is_empty() {
        consolidated.insert("_timing".to_string(), Value::Object(timing_obj));
    }

    // 6. 用量统计
    if let Some(usage) = usage_obj {
        if !usage.is_empty() {
            consolidated.insert("usage".to_string(), Value::Object(usage));
        }
    }

    Value::Object(consolidated)
}

fn consolidate_non_streaming_response(
    raw_json: &Value,
    log: &ProxyRequestLog,
    headers_map: &serde_json::Map<String, Value>,
) -> Option<Value> {
    // 若本就已是统一的 consolidated 格式 (例如带有顶层 thinking/tool_calls 且无 choices/candidates)，直接返回 None
    if (raw_json.get("thinking").is_some()
        || raw_json.get("tool_calls").is_some()
        || raw_json.get("_session_thinking_id").is_some())
        && raw_json.get("choices").is_none()
        && raw_json.get("candidates").is_none()
    {
        return None;
    }

    let mut thinking_content = String::new();
    let mut thinking_signature = String::new();
    let mut response_content = String::new();
    let mut tool_calls = Vec::new();

    // 1. OpenAI 格式 (choices)
    if let Some(choices) = raw_json.get("choices").and_then(|c| c.as_array()) {
        for choice in choices {
            if let Some(msg) = choice.get("message") {
                if let Some(rc) = msg.get("reasoning_content").and_then(|v| v.as_str()) {
                    thinking_content.push_str(rc);
                }
                if let Some(th) = msg.get("thinking").and_then(|v| v.as_str()) {
                    thinking_content.push_str(th);
                }
                if let Some(sig) = msg
                    .get("thoughtSignature")
                    .or_else(|| msg.get("thought_signature"))
                    .or_else(|| msg.get("signature"))
                    .and_then(|v| v.as_str())
                {
                    thinking_signature = sig.to_string();
                }
                if let Some(c) = msg.get("content").and_then(|v| v.as_str()) {
                    response_content.push_str(c);
                }
                if let Some(tcs) = msg.get("tool_calls").and_then(|t| t.as_array()) {
                    for tc in tcs {
                        let id = tc.get("id").and_then(|v| v.as_str()).unwrap_or("");
                        let name = tc
                            .get("function")
                            .and_then(|f| f.get("name"))
                            .and_then(|n| n.as_str())
                            .unwrap_or("");
                        let args = tc
                            .get("function")
                            .and_then(|f| f.get("arguments"))
                            .map(|a| {
                                if let Some(s) = a.as_str() {
                                    s.to_string()
                                } else {
                                    a.to_string()
                                }
                            })
                            .unwrap_or_else(|| "{}".to_string());
                        tool_calls.push(serde_json::json!({
                            "id": id,
                            "type": "function",
                            "function": { "name": name, "arguments": args }
                        }));
                    }
                }
            }
        }
    }
    // 2. Gemini 格式 (candidates)
    else if let Some(candidates) = raw_json.get("candidates").and_then(|c| c.as_array()) {
        for cand in candidates {
            if let Some(parts) = cand
                .get("content")
                .and_then(|c| c.get("parts"))
                .and_then(|p| p.as_array())
            {
                for part in parts {
                    if let Some(text) = part.get("text").and_then(|t| t.as_str()) {
                        if part
                            .get("thought")
                            .and_then(|t| t.as_bool())
                            .unwrap_or(false)
                        {
                            thinking_content.push_str(text);
                        } else {
                            response_content.push_str(text);
                        }
                    }
                    if let Some(sig) = part
                        .get("thoughtSignature")
                        .or_else(|| part.get("thought_signature"))
                        .or_else(|| part.get("signature"))
                        .or_else(|| {
                            part.get("functionCall")
                                .and_then(|fc| fc.get("thoughtSignature"))
                        })
                        .or_else(|| {
                            part.get("functionCall")
                                .and_then(|fc| fc.get("thought_signature"))
                        })
                        .and_then(|s| s.as_str())
                    {
                        thinking_signature = sig.to_string();
                    }
                    if let Some(fc) = part.get("functionCall") {
                        if let Some(name) = fc.get("name").and_then(|n| n.as_str()) {
                            let call_id = if let Some(id_str) = fc
                                .get("id")
                                .and_then(|i| i.as_str())
                                .filter(|s| !s.is_empty())
                            {
                                id_str.to_string()
                            } else {
                                crate::proxy::thinking_store::synthesize_tool_id(
                                    name,
                                    fc.get("args"),
                                    "root",
                                    tool_calls.len(),
                                )
                            };
                            let args = fc
                                .get("args")
                                .map(|a| a.to_string())
                                .unwrap_or_else(|| "{}".to_string());
                            tool_calls.push(serde_json::json!({
                                "id": call_id,
                                "type": "function",
                                "function": { "name": name, "arguments": args }
                            }));
                        }
                    }
                }
            }
        }
    }
    // 3. Claude 格式 (content 数组)
    else if let Some(content) = raw_json.get("content").and_then(|c| c.as_array()) {
        for block in content {
            match block.get("type").and_then(|t| t.as_str()) {
                Some("thinking") => {
                    if let Some(th) = block.get("thinking").and_then(|t| t.as_str()) {
                        thinking_content.push_str(th);
                    }
                    if let Some(sig) = block
                        .get("signature")
                        .or_else(|| block.get("thought_signature"))
                        .or_else(|| block.get("thoughtSignature"))
                        .and_then(|s| s.as_str())
                    {
                        thinking_signature = sig.to_string();
                    }
                }
                Some("text") => {
                    if let Some(txt) = block.get("text").and_then(|t| t.as_str()) {
                        response_content.push_str(txt);
                    }
                }
                Some("tool_use") => {
                    let id = block.get("id").and_then(|v| v.as_str()).unwrap_or("");
                    let name = block.get("name").and_then(|v| v.as_str()).unwrap_or("");
                    let args = block
                        .get("input")
                        .map(|a| a.to_string())
                        .unwrap_or_else(|| "{}".to_string());
                    tool_calls.push(serde_json::json!({
                        "id": id,
                        "type": "function",
                        "function": { "name": name, "arguments": args }
                    }));
                }
                _ => {}
            }
        }
    }

    if thinking_content.is_empty()
        && thinking_signature.is_empty()
        && response_content.is_empty()
        && tool_calls.is_empty()
    {
        return None;
    }

    // 提取耗时诊断
    let mut timing_obj = serde_json::Map::new();
    for key in [
        "clean_ms",
        "norm_ms",
        "thinking_ms",
        "ttft_ms",
        "stream_ms",
        "total_ms",
    ] {
        let hdr_key = format!("x-timing-{}", key);
        if let Some(val) = headers_map.get(&hdr_key).and_then(|v| v.as_str()) {
            if let Ok(n) = val.parse::<f64>() {
                let out_key = key.replace("_ms", "_s");
                timing_obj.insert(out_key, serde_json::json!(n / 1000.0));
            }
        }
    }
    if !timing_obj.contains_key("total_s") {
        timing_obj.insert(
            "total_s".to_string(),
            serde_json::json!(log.duration as f64 / 1000.0),
        );
    }

    // 提取 Token 用量与缓存命中率
    let usage_source = raw_json
        .get("usage")
        .or_else(|| raw_json.get("usageMetadata"));
    let mut usage_obj = serde_json::Map::new();
    let input_toks = log
        .input_tokens
        .or_else(|| usage_source.and_then(extract_input_tokens))
        .unwrap_or(0);
    let output_toks = log
        .output_tokens
        .or_else(|| usage_source.and_then(extract_output_tokens))
        .unwrap_or(0);
    let cached_toks = log
        .cached_tokens
        .or_else(|| usage_source.and_then(extract_cached_tokens))
        .unwrap_or(0);
    let reasoning_toks = usage_source.and_then(extract_reasoning_tokens).unwrap_or(0);

    let total_in = if cached_toks > input_toks {
        input_toks + cached_toks
    } else {
        input_toks
    };
    let total_toks = total_in + output_toks;

    usage_obj.insert("input_tokens".to_string(), serde_json::json!(total_in));
    usage_obj.insert("output_tokens".to_string(), serde_json::json!(output_toks));
    usage_obj.insert("total_tokens".to_string(), serde_json::json!(total_toks));
    if cached_toks > 0 {
        usage_obj.insert("cached_tokens".to_string(), serde_json::json!(cached_toks));
        if total_in > 0 {
            let hit_rate = (cached_toks as f64 / total_in as f64 * 100.0)
                .min(100.0)
                .max(0.0);
            usage_obj.insert(
                "cache_hit_rate".to_string(),
                serde_json::json!(format!("{:.1}%", hit_rate)),
            );
        }
    }
    if reasoning_toks > 0 {
        usage_obj.insert(
            "reasoning_tokens".to_string(),
            serde_json::json!(reasoning_toks),
        );
    }

    Some(build_canonical_consolidated_response(
        thinking_content,
        thinking_signature,
        response_content,
        tool_calls,
        log.session_id.as_deref(),
        &log.id,
        timing_obj,
        Some(usage_obj),
    ))
}

pub async fn monitor_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let _logging_enabled = state.monitor.is_enabled();

    let method = request.method().to_string();
    let uri = request.uri().to_string();

    if uri.contains("event_logging") || uri.contains("/api/") || uri.starts_with("/internal/") {
        return next.run(request).await;
    }

    let start = Instant::now();

    // Extract client IP from headers (X-Forwarded-For or X-Real-IP)
    // IMPORTANT: Extract from Request headers, not Response headers (since we want the client's IP)
    // Note: We need to do this BEFORE consuming the request body if possible, or extract it from the original request
    let client_ip = request
        .headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').next().unwrap_or(s).trim().to_string())
        .or_else(|| {
            request
                .headers()
                .get("x-real-ip")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
        });

    let user_agent = request
        .headers()
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let request_content_type = request
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let mut model = if uri.contains("/v1beta/models/") {
        uri.split("/v1beta/models/")
            .nth(1)
            .and_then(|s| s.split(':').next())
            .and_then(|s| s.split('?').next())
            .map(|s| s.to_string())
    } else if uri.contains("/v1/models/claude/") {
        uri.split("/v1/models/claude/")
            .nth(1)
            .and_then(|s| s.split('?').next())
            .map(|s| s.to_string())
    } else if uri.contains("/v1/models/") {
        uri.split("/v1/models/")
            .nth(1)
            .and_then(|s| s.split('?').next())
            .map(|s| s.to_string())
    } else if uri.contains("/v1/model/") {
        uri.split("/v1/model/")
            .nth(1)
            .and_then(|s| s.split('?').next())
            .map(|s| s.to_string())
    } else if uri.contains("/responses/models/") {
        uri.split("/responses/models/")
            .nth(1)
            .and_then(|s| s.split('?').next())
            .map(|s| s.to_string())
    } else {
        None
    };

    let request_headers_json =
        crate::proxy::payload_audit::headers_to_redacted_json(request.headers());

    let request_body_str;

    // [FIX] 从请求 extensions 提取 UserTokenIdentity (由 Auth 中间件注入)
    // 必须在处理 request body 之前提取，因为 into_parts() 后需要保留这个值
    let user_token_identity = request.extensions().get::<UserTokenIdentity>().cloned();

    let request = if method == "POST" {
        let (parts, body) = request.into_parts();
        match axum::body::to_bytes(body, MAX_REQUEST_LOG_SIZE).await {
            Ok(bytes) => {
                request_body_str = if request_content_type.starts_with("multipart/form-data") {
                    if let Some((summary, multipart_model)) =
                        summarize_multipart_request(&bytes, &request_content_type, &uri)
                    {
                        if model.is_none() {
                            model = multipart_model;
                        }
                        Some(summary)
                    } else {
                        Some(format!(
                            "[Multipart Request Data: {} bytes, failed to parse summary]",
                            bytes.len()
                        ))
                    }
                } else if let Ok(s) = std::str::from_utf8(&bytes) {
                    if model.is_none() {
                        model = serde_json::from_slice::<Value>(&bytes).ok().and_then(|v| {
                            v.get("model")
                                .and_then(|m| m.as_str())
                                .map(|s| s.to_string())
                        });
                    }
                    Some(s.to_string())
                } else {
                    Some("[Binary Request Data]".to_string())
                };
                Request::from_parts(parts, Body::from(bytes))
            }
            Err(_) => {
                request_body_str = None;
                Request::from_parts(parts, Body::empty())
            }
        }
    } else {
        request_body_str = None;
        request
    };

    let upstream_holder = UpstreamRequestBodyHolder::new();
    let mut request = request;
    request.extensions_mut().insert(upstream_holder.clone());

    let response = crate::proxy::monitor::CURRENT_UPSTREAM_CAPTURE
        .scope(upstream_holder.clone(), next.run(request))
        .await;
    let upstream_request_body = upstream_holder.take();
    let upstream_request_headers = upstream_holder.take_headers();
    let response_headers_json =
        crate::proxy::payload_audit::headers_to_redacted_json(response.headers());

    // user_token_identity 已在上面从请求 extensions 中提取

    let duration = start.elapsed().as_millis() as u64;
    let status = response.status().as_u16();

    // 过滤噪音请求，避免客户端探针刷屏淹没真实业务日志 (Issue #3498 + `/v1/models` 轮询刷屏)
    // 胶囊开关关闭时（默认）：所有 GET 成功请求不记录、不落库；失败请求与非 GET 业务请求始终记录。
    // 详见 `should_skip_request_log` 的行为边界说明。
    let capture_health_enabled =
        state.monitor.is_capture_health_logs() || should_log_health_checks();
    if should_skip_request_log(&method, response.status(), capture_health_enabled) {
        return response;
    }

    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    // Extract account email from X-Account-Email header if present
    let account_email = response
        .headers()
        .get("X-Account-Email")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Extract mapped model from X-Mapped-Model header if present
    let mapped_model = response
        .headers()
        .get("X-Mapped-Model")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Determine protocol from URL path
    let protocol = if uri.contains("/v1/messages") || uri.contains("/v1/models/claude") {
        Some("anthropic".to_string())
    } else if uri.contains("/v1beta/models") {
        Some("gemini".to_string())
    } else if uri.contains("/responses") {
        Some("responses".to_string())
    } else if uri.starts_with("/v1/") {
        Some("openai".to_string())
    } else {
        None
    };

    // Client IP has been extracted at the beginning of the function

    // Extract username from UserTokenIdentity if present
    let username = user_token_identity
        .as_ref()
        .map(|identity| identity.username.clone());
    let is_image_route = uri.contains("/v1/images/");

    // Extract session ID from response headers (e.g. X-Session-Id or X-Antigravity-Session-Id)
    let session_id = response
        .headers()
        .get("X-Session-Id")
        .or_else(|| response.headers().get("X-Antigravity-Session-Id"))
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let monitor = state.monitor.clone();
    let mut log = ProxyRequestLog {
        id: uuid::Uuid::new_v4().to_string(),
        timestamp: chrono::Utc::now().timestamp_millis(),
        method,
        url: uri,
        status,
        duration,
        model,
        mapped_model,
        account_email,
        client_ip,
        error: None,
        request_body: request_body_str,
        upstream_request_body,
        response_body: None,
        request_headers: Some(request_headers_json),
        upstream_request_headers,
        response_headers: Some(response_headers_json),
        input_tokens: None,
        output_tokens: None,
        cached_tokens: None,
        protocol,
        username,
        session_id,
    };

    if content_type.contains("text/event-stream") {
        let (parts, body) = response.into_parts();
        let mut stream = body.into_data_stream();
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let start_instant = start;

        tokio::spawn(async move {
            let stream_start = Instant::now();
            enum StreamCaptureSink {
                Memory(Vec<u8>),
                Spill(SpillWriter),
                Failed,
            }

            const STREAM_MEMORY_SPILL_BYTES: usize = 1024 * 1024; // 1MB 内存缓冲，超过溢出到临时文件
            const MAX_STREAM_PERSIST_BYTES: usize = 100 * 1024 * 1024; // 100MB 磁盘流式日志安全上限

            let mut sink = StreamCaptureSink::Memory(Vec::with_capacity(64 * 1024));
            let mut bytes_forwarded = 0usize;
            let mut bytes_captured = 0usize;
            let mut last_few_bytes = Vec::new();
            let mut stream_truncated = false;
            let mut capture_error: Option<String> = None;
            let mut client_disconnected = false;
            let mut upstream_error: Option<String> = None;

            while let Some(chunk_res) = next_chunk_while_receiver_open(&mut stream, &tx).await {
                if let Ok(chunk) = chunk_res {
                    // 1. 优先转发客户端：日志系统任何 I/O 故障绝不阻塞或截断客户端数据流
                    if tx.send(Ok::<_, axum::Error>(chunk.clone())).await.is_err() {
                        client_disconnected = true;
                        break;
                    }
                    bytes_forwarded += chunk.len();

                    // 2. 捕获并落盘
                    if capture_error.is_none()
                        && bytes_captured + chunk.len() <= MAX_STREAM_PERSIST_BYTES
                    {
                        match &mut sink {
                            StreamCaptureSink::Memory(buf) => {
                                if buf.len() + chunk.len() <= STREAM_MEMORY_SPILL_BYTES {
                                    buf.extend_from_slice(&chunk);
                                    bytes_captured += chunk.len();
                                } else {
                                    match create_named_temp_file() {
                                        Ok(mut temp) => {
                                            use std::io::Write;
                                            if let Err(e) = temp
                                                .write_all(buf)
                                                .and_then(|_| temp.write_all(&chunk))
                                            {
                                                capture_error = Some(format!("Failed to write initial chunks to spill file: {}", e));
                                                sink = StreamCaptureSink::Failed;
                                            } else {
                                                bytes_captured += chunk.len();
                                                sink = StreamCaptureSink::Spill(SpillWriter {
                                                    inner: std::io::BufWriter::new(temp),
                                                    bytes_written: buf.len() + chunk.len(),
                                                });
                                            }
                                        }
                                        Err(e) => {
                                            capture_error = Some(format!(
                                                "Failed to create temporary spill file: {}",
                                                e
                                            ));
                                            sink = StreamCaptureSink::Failed;
                                        }
                                    }
                                }
                            }
                            StreamCaptureSink::Spill(writer) => {
                                use std::io::Write;
                                if let Err(e) = writer.write_all(&chunk) {
                                    capture_error =
                                        Some(format!("Failed to write chunk to spill file: {}", e));
                                    sink = StreamCaptureSink::Failed;
                                } else {
                                    bytes_captured += chunk.len();
                                }
                            }
                            StreamCaptureSink::Failed => {}
                        }
                    } else if bytes_captured + chunk.len() > MAX_STREAM_PERSIST_BYTES {
                        stream_truncated = true;
                    }

                    if chunk.len() > 8192 {
                        last_few_bytes = chunk.slice(chunk.len() - 8192..).to_vec();
                    } else {
                        last_few_bytes.extend_from_slice(&chunk);
                        if last_few_bytes.len() > 8192 {
                            last_few_bytes.drain(0..last_few_bytes.len() - 8192);
                        }
                    }
                } else if let Err(e) = chunk_res {
                    upstream_error = Some(e.to_string());
                    let _ = tx.send(Err(axum::Error::new(e))).await;
                    break;
                }
            }
            if upstream_error.is_none() && tx.is_closed() {
                client_disconnected = true;
            }
            drop(stream);

            let stream_ms = stream_start.elapsed().as_micros() as f64 / 1000.0;
            let total_ms = start_instant.elapsed().as_micros() as f64 / 1000.0;
            log.duration = total_ms.round() as u64;

            let mut headers_map: serde_json::Map<String, Value> = log
                .response_headers
                .as_ref()
                .and_then(|h| serde_json::from_str(h).ok())
                .unwrap_or_default();

            headers_map.insert(
                "x-timing-stream-ms".to_string(),
                serde_json::json!(format!("{:.3}", stream_ms)),
            );
            headers_map.insert(
                "x-timing-total-ms".to_string(),
                serde_json::json!(format!("{:.3}", total_ms)),
            );
            log.response_headers = serde_json::to_string(&Value::Object(headers_map.clone())).ok();

            // Parse and consolidate stream data into readable format using buffered reader
            use std::io::BufRead;
            let lines_reader: Box<dyn std::io::BufRead + Send> = match sink {
                StreamCaptureSink::Memory(buf) => Box::new(std::io::Cursor::new(buf)),
                StreamCaptureSink::Spill(mut writer) => {
                    use std::io::Write;
                    if let Err(e) = writer.flush() {
                        capture_error = Some(format!("Failed to flush spill file: {}", e));
                        Box::new(std::io::Cursor::new(Vec::new()))
                    } else {
                        match writer.inner.into_inner() {
                            Ok(temp_file) => match seek_and_open_reader(temp_file) {
                                Ok(reader) => reader,
                                Err(e) => {
                                    capture_error =
                                        Some(format!("Failed to seek spill file: {}", e));
                                    Box::new(std::io::Cursor::new(Vec::new()))
                                }
                            },
                            Err(e) => {
                                capture_error =
                                    Some(format!("Failed to unwrap spill writer: {}", e));
                                Box::new(std::io::Cursor::new(Vec::new()))
                            }
                        }
                    }
                }
                StreamCaptureSink::Failed => Box::new(std::io::Cursor::new(Vec::new())),
            };

            let mut response_content = String::new();
            let mut thinking_content = String::new();
            let mut thinking_signature = String::new();
            let mut tool_calls: Vec<Value> = Vec::new();
            let mut cached_tokens: Option<u32> = None;
            let mut reasoning_tokens: Option<u32> = None;

            for line_res in lines_reader.lines() {
                let line = match line_res {
                    Ok(l) => l,
                    Err(e) => {
                        capture_error =
                            Some(format!("Failed to read stream line from spill file: {}", e));
                        break;
                    }
                };
                if !line.starts_with("data: ") {
                    continue;
                }
                let json_str = line.trim_start_matches("data: ").trim();
                if json_str == "[DONE]" {
                    continue;
                }

                if let Ok(json) = serde_json::from_str::<Value>(json_str) {
                    // OpenAI format: choices[0].delta.content / reasoning_content / tool_calls
                    if let Some(choices) = json.get("choices").and_then(|c| c.as_array()) {
                        for choice in choices {
                            if let Some(delta) = choice.get("delta") {
                                // Thinking/reasoning content
                                if let Some(thinking) =
                                    delta.get("reasoning_content").and_then(|v| v.as_str())
                                {
                                    thinking_content.push_str(thinking);
                                }
                                // Thinking signature in OpenAI delta
                                if let Some(sig) = delta
                                    .get("signature")
                                    .or_else(|| delta.get("thought_signature"))
                                    .and_then(|v| v.as_str())
                                {
                                    thinking_signature = sig.to_string();
                                }
                                // Main response content
                                if let Some(content) = delta.get("content").and_then(|v| v.as_str())
                                {
                                    response_content.push_str(content);
                                }
                                // Tool calls
                                if let Some(delta_tool_calls) =
                                    delta.get("tool_calls").and_then(|t| t.as_array())
                                {
                                    for tc in delta_tool_calls {
                                        if let Some(index) =
                                            tc.get("index").and_then(|i| i.as_u64())
                                        {
                                            let idx = index as usize;
                                            while tool_calls.len() <= idx {
                                                tool_calls.push(serde_json::json!({
                                                    "id": "",
                                                    "type": "function",
                                                    "function": { "name": "", "arguments": "" }
                                                }));
                                            }
                                            let current_tc = &mut tool_calls[idx];
                                            if let Some(id) = tc.get("id").and_then(|v| v.as_str())
                                            {
                                                current_tc["id"] = Value::String(id.to_string());
                                            }
                                            if let Some(func) = tc.get("function") {
                                                if let Some(name) =
                                                    func.get("name").and_then(|v| v.as_str())
                                                {
                                                    current_tc["function"]["name"] =
                                                        Value::String(name.to_string());
                                                }
                                                if let Some(args) =
                                                    func.get("arguments").and_then(|v| v.as_str())
                                                {
                                                    let old_args = current_tc["function"]
                                                        ["arguments"]
                                                        .as_str()
                                                        .unwrap_or("");
                                                    current_tc["function"]["arguments"] =
                                                        Value::String(format!(
                                                            "{}{}",
                                                            old_args, args
                                                        ));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Gemini format: candidates[0].content.parts
                    if let Some(candidates) = json.get("candidates").and_then(|c| c.as_array()) {
                        for cand in candidates {
                            if let Some(content) = cand.get("content") {
                                if let Some(parts) = content.get("parts").and_then(|p| p.as_array())
                                {
                                    for part in parts {
                                        if let Some(text) =
                                            part.get("text").and_then(|t| t.as_str())
                                        {
                                            if part
                                                .get("thought")
                                                .and_then(|t| t.as_bool())
                                                .unwrap_or(false)
                                            {
                                                thinking_content.push_str(text);
                                            } else {
                                                response_content.push_str(text);
                                            }
                                        }
                                        let sig = part
                                            .get("thoughtSignature")
                                            .or_else(|| part.get("thought_signature"))
                                            .or_else(|| part.get("signature"))
                                            .or_else(|| {
                                                part.get("functionCall")
                                                    .and_then(|fc| fc.get("thoughtSignature"))
                                            })
                                            .or_else(|| {
                                                part.get("functionCall")
                                                    .and_then(|fc| fc.get("thought_signature"))
                                            })
                                            .and_then(|s| s.as_str());
                                        if let Some(s) = sig {
                                            thinking_signature = s.to_string();
                                        }
                                        if let Some(fc) = part.get("functionCall") {
                                            if let Some(name) =
                                                fc.get("name").and_then(|n| n.as_str())
                                            {
                                                let call_id = if let Some(id_str) = fc
                                                    .get("id")
                                                    .and_then(|i| i.as_str())
                                                    .filter(|s| !s.is_empty())
                                                {
                                                    id_str.to_string()
                                                } else {
                                                    crate::proxy::thinking_store::synthesize_tool_id(
                                                        name,
                                                        fc.get("args"),
                                                        "root",
                                                        tool_calls.len(),
                                                    )
                                                };
                                                let args = fc
                                                    .get("args")
                                                    .map(|a| a.to_string())
                                                    .unwrap_or_else(|| "{}".to_string());
                                                tool_calls.push(serde_json::json!({
                                                    "id": call_id,
                                                    "type": "function",
                                                    "function": { "name": name, "arguments": args }
                                                }));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Claude/Anthropic format: content_block_start, content_block_delta, etc.
                    let msg_type = json.get("type").and_then(|t| t.as_str());
                    match msg_type {
                        Some("content_block_start") => {
                            if let (Some(index), Some(block)) = (
                                json.get("index").and_then(|i| i.as_u64()),
                                json.get("content_block"),
                            ) {
                                let idx = index as usize;
                                if block.get("type").and_then(|t| t.as_str()) == Some("tool_use") {
                                    let id = block.get("id").and_then(|v| v.as_str()).unwrap_or("");
                                    let name =
                                        block.get("name").and_then(|v| v.as_str()).unwrap_or("");
                                    while tool_calls.len() <= idx {
                                        tool_calls.push(Value::Null);
                                    }
                                    tool_calls[idx] = serde_json::json!({
                                        "id": id,
                                        "type": "function",
                                        "function": { "name": name, "arguments": "" }
                                    });
                                }
                                if let Some(thinking) =
                                    block.get("thinking").and_then(|v| v.as_str())
                                {
                                    thinking_content.push_str(thinking);
                                }
                                if let Some(sig) = block
                                    .get("signature")
                                    .or_else(|| block.get("thought_signature"))
                                    .or_else(|| block.get("thoughtSignature"))
                                    .and_then(|v| v.as_str())
                                {
                                    thinking_signature = sig.to_string();
                                }
                            }
                        }
                        Some("content_block_delta") => {
                            if let (Some(index), Some(delta)) = (
                                json.get("index").and_then(|i| i.as_u64()),
                                json.get("delta"),
                            ) {
                                let idx = index as usize;

                                // Tool use input delta (Anthropic 官方协议为 partial_json 增量)
                                let delta_json = delta
                                    .get("partial_json")
                                    .or_else(|| delta.get("input_json_delta"))
                                    .or_else(|| delta.get("text"))
                                    .and_then(|v| v.as_str());

                                if let Some(delta_str) = delta_json {
                                    if idx < tool_calls.len() && !tool_calls[idx].is_null() {
                                        let old_args = tool_calls[idx]["function"]["arguments"]
                                            .as_str()
                                            .unwrap_or("");
                                        tool_calls[idx]["function"]["arguments"] =
                                            Value::String(format!("{}{}", old_args, delta_str));
                                    }
                                }
                                // Legacy/Native thinking block
                                if let Some(thinking) =
                                    delta.get("thinking").and_then(|v| v.as_str())
                                {
                                    thinking_content.push_str(thinking);
                                }
                                // Thinking signature in delta (Claude signature_delta or direct signature)
                                if let Some(sig) = delta
                                    .get("signature")
                                    .or_else(|| delta.get("thought_signature"))
                                    .or_else(|| delta.get("thoughtSignature"))
                                    .and_then(|v| v.as_str())
                                {
                                    thinking_signature = sig.to_string();
                                }
                                // Text content
                                if let Some(text) = delta.get("text").and_then(|v| v.as_str()) {
                                    response_content.push_str(text);
                                }
                            }
                        }
                        Some("message_delta") => {
                            if let Some(delta) = json.get("delta") {
                                if let Some(usage) = delta.get("usage") {
                                    if let Some(output_tokens) =
                                        usage.get("output_tokens").and_then(|v| v.as_u64())
                                    {
                                        log.output_tokens = Some(output_tokens as u32);
                                    }
                                }
                            }
                        }
                        Some("response.output_text.delta") => {
                            if let Some(text) = json.get("delta").and_then(|v| v.as_str()) {
                                response_content.push_str(text);
                            }
                        }
                        Some("response.reasoning_summary_text.delta") => {
                            if let Some(text) = json.get("delta").and_then(|v| v.as_str()) {
                                thinking_content.push_str(text);
                            }
                        }
                        Some("response.output_item.added") => {
                            if let Some(item) = json.get("item") {
                                if item.get("type").and_then(|t| t.as_str())
                                    == Some("function_call")
                                {
                                    let name =
                                        item.get("name").and_then(|v| v.as_str()).unwrap_or("");
                                    let id =
                                        item.get("call_id").and_then(|v| v.as_str()).unwrap_or("");
                                    tool_calls.push(serde_json::json!({
                                        "id": id,
                                        "type": "function",
                                        "function": { "name": name, "arguments": "" }
                                    }));
                                }
                            }
                        }
                        Some("response.function_call_arguments.delta") => {
                            if let Some(delta) = json.get("delta").and_then(|v| v.as_str()) {
                                if let Some(last_tc) = tool_calls.last_mut() {
                                    let old_args =
                                        last_tc["function"]["arguments"].as_str().unwrap_or("");
                                    last_tc["function"]["arguments"] =
                                        Value::String(format!("{}{}", old_args, delta));
                                }
                            }
                        }
                        _ => {}
                    }

                    // Legacy Claude delta (for older implementations or simplified streams)
                    if msg_type.is_none() {
                        if let Some(delta) = json.get("delta") {
                            // Thinking block
                            if let Some(thinking) = delta.get("thinking").and_then(|v| v.as_str()) {
                                thinking_content.push_str(thinking);
                            }
                            // Thinking signature
                            if let Some(sig) = delta.get("signature").and_then(|v| v.as_str()) {
                                thinking_signature = sig.to_string();
                            }
                            // Text content
                            if let Some(text) = delta.get("text").and_then(|v| v.as_str()) {
                                response_content.push_str(text);
                            }
                        }
                    }

                    // Token usage extraction
                    if let Some(usage) = json
                        .get("usage")
                        .or(json.get("usageMetadata"))
                        .or(json.get("response").and_then(|r| r.get("usage")))
                        .or(json.get("response").and_then(|r| r.get("usageMetadata")))
                    {
                        log.input_tokens = extract_input_tokens(usage);
                        log.output_tokens = extract_output_tokens(usage);
                        cached_tokens = cached_tokens.or_else(|| extract_cached_tokens(usage));
                        log.cached_tokens = log.cached_tokens.or(cached_tokens);
                        reasoning_tokens =
                            reasoning_tokens.or_else(|| extract_reasoning_tokens(usage));

                        if log.input_tokens.is_none() && log.output_tokens.is_none() {
                            log.output_tokens = usage
                                .get("total_tokens")
                                .or(usage.get("totalTokenCount"))
                                .and_then(|v| v.as_u64())
                                .map(|v| v as u32);
                        }
                    }
                }
            }

            // [Timing Diagnostics] 注入耗时诊断元数据 (秒)
            let mut timing_obj = serde_json::Map::new();
            if let Some(clean) = headers_map
                .get("x-timing-clean-ms")
                .and_then(|v| v.as_str())
            {
                if let Ok(n) = clean.parse::<f64>() {
                    timing_obj.insert("clean_s".to_string(), serde_json::json!(n / 1000.0));
                }
            }
            if let Some(norm) = headers_map.get("x-timing-norm-ms").and_then(|v| v.as_str()) {
                if let Ok(n) = norm.parse::<f64>() {
                    timing_obj.insert("norm_s".to_string(), serde_json::json!(n / 1000.0));
                }
            }
            if let Some(th) = headers_map
                .get("x-timing-thinking-ms")
                .and_then(|v| v.as_str())
            {
                if let Ok(n) = th.parse::<f64>() {
                    timing_obj.insert("thinking_s".to_string(), serde_json::json!(n / 1000.0));
                }
            }
            if let Some(ttft) = headers_map.get("x-timing-ttft-ms").and_then(|v| v.as_str()) {
                if let Ok(n) = ttft.parse::<f64>() {
                    timing_obj.insert("ttft_s".to_string(), serde_json::json!(n / 1000.0));
                }
            }
            timing_obj.insert(
                "stream_s".to_string(),
                serde_json::json!(stream_ms / 1000.0),
            );
            timing_obj.insert("total_s".to_string(), serde_json::json!(total_ms / 1000.0));

            // 优先从 tail 兜底提取 token（若当前尚无 token 数据）
            if log.input_tokens.is_none() && log.output_tokens.is_none() {
                if let Ok(full_tail) = std::str::from_utf8(&last_few_bytes) {
                    for line in full_tail.lines().rev() {
                        if line.starts_with("data: ")
                            && (line.contains("\"usage\"") || line.contains("\"usageMetadata\""))
                        {
                            let json_str = line.trim_start_matches("data: ").trim();
                            if let Ok(json) = serde_json::from_str::<Value>(json_str) {
                                if let Some(usage) = json
                                    .get("usage")
                                    .or(json.get("usageMetadata"))
                                    .or(json.get("response").and_then(|r| r.get("usage")))
                                    .or(json.get("response").and_then(|r| r.get("usageMetadata")))
                                {
                                    log.input_tokens = extract_input_tokens(usage);
                                    log.output_tokens = extract_output_tokens(usage);
                                    cached_tokens =
                                        cached_tokens.or_else(|| extract_cached_tokens(usage));
                                    log.cached_tokens = log.cached_tokens.or(cached_tokens);
                                    reasoning_tokens = reasoning_tokens
                                        .or_else(|| extract_reasoning_tokens(usage));
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            let has_actual_content = !response_content.is_empty()
                || !tool_calls.is_empty()
                || !thinking_content.is_empty();

            let mut usage_obj = serde_json::Map::new();
            if has_actual_content || log.input_tokens.is_some() || log.output_tokens.is_some() {
                let input_toks = log.input_tokens.unwrap_or(0);
                let output_toks = log.output_tokens.unwrap_or(0);
                let cached_toks = cached_tokens.or(log.cached_tokens).unwrap_or(0);
                let reasoning_toks = reasoning_tokens.unwrap_or(0);

                let total_in = if cached_toks > input_toks {
                    input_toks + cached_toks
                } else {
                    input_toks
                };
                let total_toks = total_in + output_toks;

                usage_obj.insert("input_tokens".to_string(), serde_json::json!(total_in));
                usage_obj.insert("output_tokens".to_string(), serde_json::json!(output_toks));
                usage_obj.insert("total_tokens".to_string(), serde_json::json!(total_toks));
                if cached_toks > 0 {
                    usage_obj.insert("cached_tokens".to_string(), serde_json::json!(cached_toks));
                    if total_in > 0 {
                        let hit_rate = (cached_toks as f64 / total_in as f64 * 100.0)
                            .min(100.0)
                            .max(0.0);
                        usage_obj.insert(
                            "cache_hit_rate".to_string(),
                            serde_json::json!(format!("{:.1}%", hit_rate)),
                        );
                    }
                }
                if reasoning_toks > 0 {
                    usage_obj.insert(
                        "reasoning_tokens".to_string(),
                        serde_json::json!(reasoning_toks),
                    );
                }
            }

            let mut canonical = build_canonical_consolidated_response(
                thinking_content,
                thinking_signature,
                response_content,
                tool_calls,
                log.session_id.as_deref(),
                &log.id,
                timing_obj,
                if usage_obj.is_empty() {
                    None
                } else {
                    Some(usage_obj)
                },
            );

            let consolidated = canonical
                .as_object_mut()
                .expect("canonical response is an object");

            consolidated.insert(
                "bytes_forwarded".to_string(),
                serde_json::json!(bytes_forwarded),
            );
            consolidated.insert(
                "bytes_captured".to_string(),
                serde_json::json!(bytes_captured),
            );

            // 1. 流终止原因 (stream_termination_reason)
            let termination_reason = if let Some(ref uerr) = upstream_error {
                consolidated.insert("upstream_error".to_string(), serde_json::json!(uerr));
                if log.error.is_none() {
                    log.error = Some(format!("Upstream error during stream: {}", uerr));
                }
                "upstream_error"
            } else if client_disconnected {
                if log.error.is_none() {
                    log.error = Some(format!(
                        "Client disconnected prematurely after {} bytes forwarded (captured: {})",
                        bytes_forwarded, bytes_captured
                    ));
                }
                "client_cancelled"
            } else if stream_truncated {
                "truncated"
            } else {
                "normal_eof"
            };
            consolidated.insert(
                "stream_termination_reason".to_string(),
                serde_json::json!(termination_reason),
            );

            // 2. 日志捕获状态 (stream_capture_status)
            let capture_status = if let Some(ref err) = capture_error {
                consolidated.insert("stream_capture_error".to_string(), serde_json::json!(err));
                if log.error.is_none() {
                    log.error = Some(format!(
                        "Stream capture failed: {} (forwarded: {}, captured: {})",
                        err, bytes_forwarded, bytes_captured
                    ));
                }
                "failed"
            } else if stream_truncated {
                "truncated"
            } else if client_disconnected || upstream_error.is_some() {
                "partial"
            } else {
                "completed"
            };
            consolidated.insert(
                "stream_capture_status".to_string(),
                serde_json::json!(capture_status),
            );

            log.response_body = Some(serde_json::to_string_pretty(&canonical).unwrap_or_else(
                |_| format!("[Consolidated Stream Data: {} bytes]", bytes_forwarded),
            ));

            if log.status >= 400 && log.error.is_none() {
                log.error = Some("Stream Error or Failed".to_string());
            }

            // Fallback input token estimation prefers the transit (upstream) body
            if log.input_tokens.is_none() {
                let estimated = log
                    .upstream_request_body
                    .as_ref()
                    .or(log.request_body.as_ref())
                    .map(|body| {
                        crate::proxy::mappers::context_manager::estimate_raw_tokens_from_payload(
                            body,
                        )
                    })
                    .unwrap_or(0);
                if estimated > 0 {
                    log.input_tokens = Some(estimated);
                }
            }

            // Record User Token Usage
            record_user_token_usage(&user_token_identity, &log, user_agent.clone());

            monitor.log_request(log).await;
        });

        Response::from_parts(
            parts,
            Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx)),
        )
    } else if content_type.contains("application/json") || content_type.contains("text/") {
        let total_ms = start.elapsed().as_micros() as f64 / 1000.0;
        log.duration = total_ms.round() as u64;

        let mut headers_map: serde_json::Map<String, Value> = log
            .response_headers
            .as_ref()
            .and_then(|h| serde_json::from_str(h).ok())
            .unwrap_or_default();

        headers_map.insert(
            "x-timing-total-ms".to_string(),
            serde_json::json!(format!("{:.3}", total_ms)),
        );
        log.response_headers = serde_json::to_string(&Value::Object(headers_map.clone())).ok();

        let (parts, body) = response.into_parts();
        match axum::body::to_bytes(body, MAX_RESPONSE_LOG_SIZE).await {
            Ok(bytes) => {
                if let Ok(s) = std::str::from_utf8(&bytes) {
                    if let Ok(json) = serde_json::from_str::<Value>(&s) {
                        // 支持 OpenAI "usage" 或 Gemini "usageMetadata"
                        if let Some(usage) = json
                            .get("usage")
                            .or(json.get("usageMetadata"))
                            .or(json.get("response").and_then(|r| r.get("usage")))
                            .or(json.get("response").and_then(|r| r.get("usageMetadata")))
                        {
                            log.input_tokens = extract_input_tokens(usage);
                            log.output_tokens = extract_output_tokens(usage);
                            log.cached_tokens =
                                log.cached_tokens.or_else(|| extract_cached_tokens(usage));

                            if log.input_tokens.is_none() && log.output_tokens.is_none() {
                                log.output_tokens = usage
                                    .get("total_tokens")
                                    .or(usage.get("totalTokenCount"))
                                    .and_then(|v| v.as_u64())
                                    .map(|v| v as u32);
                            }
                        }
                    }
                    if is_image_route {
                        log.response_body = serde_json::from_str::<Value>(&s)
                            .ok()
                            .and_then(|json| summarize_image_json_response(&json))
                            .or_else(|| Some(s.to_string()));
                    } else if let Ok(json) = serde_json::from_str::<Value>(&s) {
                        // 🌟 非流式响应入库统一转换为满血简要版 (包含网关权威签名回填与防雪崩确定性 Tool ID)
                        if let Some(canonical) =
                            consolidate_non_streaming_response(&json, &log, &headers_map)
                        {
                            log.response_body = serde_json::to_string_pretty(&canonical)
                                .ok()
                                .or_else(|| Some(s.to_string()));
                        } else {
                            log.response_body = serde_json::to_string_pretty(&json)
                                .ok()
                                .or_else(|| Some(s.to_string()));
                        }
                    } else {
                        log.response_body = Some(s.to_string());
                    }
                } else {
                    log.response_body = Some("[Binary Response Data]".to_string());
                }

                if log.status >= 400 {
                    log.error = log.response_body.clone();
                }

                // Fallback input token estimation prefers the transit (upstream) body
                if log.input_tokens.is_none() {
                    let estimated = log
                        .upstream_request_body
                        .as_ref()
                        .or(log.request_body.as_ref())
                        .map(|body| {
                            crate::proxy::mappers::context_manager::estimate_raw_tokens_from_payload(body)
                        })
                        .unwrap_or(0);
                    if estimated > 0 {
                        log.input_tokens = Some(estimated);
                    }
                }

                // Record User Token Usage
                record_user_token_usage(&user_token_identity, &log, user_agent.clone());

                monitor.log_request(log).await;
                Response::from_parts(parts, Body::from(bytes))
            }
            Err(_) => {
                log.response_body = Some("[Response too large (>100MB)]".to_string());

                // Record User Token Usage (even if too large)
                record_user_token_usage(&user_token_identity, &log, user_agent.clone());

                monitor.log_request(log).await;
                Response::from_parts(parts, Body::empty())
            }
        }
    } else {
        log.response_body = Some(format!("[{}]", content_type));

        // Record User Token Usage
        record_user_token_usage(&user_token_identity, &log, user_agent);

        monitor.log_request(log).await;
        response
    }
}

#[cfg(test)]
mod tests {
    use super::next_chunk_while_receiver_open;
    use futures::stream;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use std::task::Poll;

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn receiver_close_stops_waiting_and_drops_source_stream() {
        let dropped = Arc::new(AtomicBool::new(false));
        let drop_flag = DropFlag(dropped.clone());
        let (polled_tx, polled_rx) = tokio::sync::oneshot::channel();
        let (tx, rx) = tokio::sync::mpsc::channel::<()>(1);

        let task = tokio::spawn(async move {
            let mut polled_tx = Some(polled_tx);
            let mut source = stream::poll_fn(move |_| {
                let _ = &drop_flag;
                if let Some(polled_tx) = polled_tx.take() {
                    let _ = polled_tx.send(());
                }
                Poll::<Option<()>>::Pending
            });

            assert!(next_chunk_while_receiver_open(&mut source, &tx)
                .await
                .is_none());
        });

        polled_rx.await.expect("source stream was not polled");
        drop(rx);
        tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .expect("forwarder did not stop after receiver closed")
            .expect("forwarder task panicked");
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn test_stream_and_response_log_size_constants() {
        assert_eq!(super::MAX_RESPONSE_LOG_SIZE, 100 * 1024 * 1024);
    }

    #[tokio::test]
    async fn test_monitor_middleware_large_payload_forwarding() {
        use axum::{routing::post, Router};
        use tower::ServiceExt;

        let monitor = std::sync::Arc::new(crate::proxy::monitor::ProxyMonitor::new(10, None));
        monitor.set_enabled(true);
        monitor.set_capture_health_logs(true);
        let app_state = crate::proxy::server::AppState::create_test_state(monitor);

        let app = Router::new()
            .route(
                "/echo",
                post(|body: axum::body::Body| async move {
                    let bytes = axum::body::to_bytes(body, 50 * 1024 * 1024).await.unwrap();
                    (
                        [(axum::http::header::CONTENT_TYPE, "application/json")],
                        axum::body::Body::from(bytes),
                    )
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                app_state,
                super::monitor_middleware,
            ))
            .layer(axum::extract::DefaultBodyLimit::disable());

        // 11 MiB 请求测试（复现 issue: 11 MiB 请求原先被清空为 0 字节）
        let large_payload = vec![b'x'; 11 * 1024 * 1024];
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/echo")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(large_payload.clone()))
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        let resp_bytes = axum::body::to_bytes(resp.into_body(), 50 * 1024 * 1024)
            .await
            .unwrap();
        // 验证正文完整转发，非 0 字节
        assert_eq!(resp_bytes.len(), 11 * 1024 * 1024);
    }

    #[tokio::test]
    async fn test_monitor_middleware_disabled_logging_passthrough() {
        use axum::{routing::post, Router};
        use tower::ServiceExt;

        let monitor = std::sync::Arc::new(crate::proxy::monitor::ProxyMonitor::new(10, None));
        monitor.set_enabled(false); // 日志关闭场景
        let app_state = crate::proxy::server::AppState::create_test_state(monitor.clone());

        let app = Router::new()
            .route(
                "/echo",
                post(|body: axum::body::Body| async move {
                    let bytes = axum::body::to_bytes(body, 50 * 1024 * 1024).await.unwrap();
                    (
                        [(axum::http::header::CONTENT_TYPE, "application/json")],
                        axum::body::Body::from(bytes),
                    )
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                app_state,
                super::monitor_middleware,
            ))
            .layer(axum::extract::DefaultBodyLimit::disable());

        let payload = vec![b'y'; 3 * 1024 * 1024]; // 3 MiB
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/echo")
            .header("content-type", "application/json")
            .body(axum::body::Body::from(payload))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        let resp_bytes = axum::body::to_bytes(resp.into_body(), 50 * 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(resp_bytes.len(), 3 * 1024 * 1024);

        // 验证关闭日志时内存中无多余日志堆积
        let logs = monitor.logs.read().await;
        assert_eq!(logs.len(), 0);
    }

    #[tokio::test]
    async fn test_monitor_middleware_large_image_response() {
        use axum::{routing::get, Router};
        use tower::ServiceExt;

        let monitor = std::sync::Arc::new(crate::proxy::monitor::ProxyMonitor::new(10, None));
        monitor.set_enabled(true);
        monitor.set_capture_health_logs(true);
        let app_state = crate::proxy::server::AppState::create_test_state(monitor.clone());

        let app = Router::new()
            .route(
                "/image",
                get(|| async move {
                    (
                        [(axum::http::header::CONTENT_TYPE, "image/png")],
                        axum::body::Body::from(vec![0x89; 5 * 1024 * 1024]),
                    )
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                app_state,
                super::monitor_middleware,
            ));

        let req = axum::http::Request::builder()
            .method("GET")
            .uri("/image")
            .body(axum::body::Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        let resp_bytes = axum::body::to_bytes(resp.into_body(), 50 * 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(resp_bytes.len(), 5 * 1024 * 1024);
    }

    static STREAM_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[tokio::test]
    async fn test_monitor_middleware_sse_large_stream_persists_full_body() {
        let _test_guard = STREAM_TEST_LOCK.lock().await;
        let _dir = crate::proxy::monitor::prompt_log_tests::TestDataDir::new();
        super::clear_created_temp_paths();
        super::set_stream_failure_injection(super::StreamFailureInjection::None);
        use axum::{routing::get, Router};
        use futures::stream;
        use std::sync::Arc;
        use tower::ServiceExt;

        let _ = crate::modules::proxy_db::init_db();
        let monitor = Arc::new(crate::proxy::monitor::ProxyMonitor::new(10, None));
        monitor.set_enabled(true);
        monitor.set_capture_health_logs(true);
        let app_state = crate::proxy::server::AppState::create_test_state(monitor.clone());

        let marker = "_TAIL_MARKER_98765_";
        // 构造多个 chunk，总共 > 2.5 MiB
        let mut raw_chunks = Vec::new();
        for i in 0..50 {
            let data = format!(
                "data: {{\"choices\":[{{\"delta\":{{\"content\":\"chunk_{}_{}\"}}}}]}}\n\n",
                i,
                "A".repeat(50_000)
            );
            raw_chunks.push(axum::body::Bytes::from(data));
        }
        let tail_data = format!("data: {{\"choices\":[{{\"delta\":{{\"content\":\"{}\"}}}}]}}\n\ndata: {{\"usage\":{{\"prompt_tokens\":10000,\"completion_tokens\":150,\"prompt_tokens_details\":{{\"cached_tokens\":8800}}}}}}\n\ndata: [DONE]\n\n", marker);
        raw_chunks.push(axum::body::Bytes::from(tail_data));
        let chunks_arc = Arc::new(raw_chunks);
        let chunks_for_route = chunks_arc.clone();
        let sse_path = format!("/sse_test_{}", uuid::Uuid::new_v4());
        let sse_path_req = sse_path.clone();
        let sse_path_find = sse_path.clone();
        let app = Router::new()
            .route(
                &sse_path,
                get(move || {
                    let chunks = chunks_for_route.clone();
                    async move {
                        let items: Vec<Result<axum::body::Bytes, std::io::Error>> =
                            (*chunks).clone().into_iter().map(Ok).collect();
                        let stream = stream::iter(items);
                        (
                            [
                                ("content-type", "text/event-stream"),
                                ("x-session-id", "session-spill"),
                                ("x-timing-ttft-ms", "25.000"),
                            ],
                            axum::body::Body::from_stream(stream),
                        )
                    }
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                app_state,
                super::monitor_middleware,
            ));

        let req = axum::http::Request::builder()
            .method("GET")
            .uri(&sse_path_req)
            .body(axum::body::Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        let client_bytes = axum::body::to_bytes(resp.into_body(), 50 * 1024 * 1024)
            .await
            .unwrap();
        // 1. 客户端完整数据逐字节比对
        let expected_client_bytes: Vec<u8> =
            (*chunks_arc).iter().flat_map(|b| b.to_vec()).collect();
        assert_eq!(
            client_bytes.to_vec(),
            expected_client_bytes,
            "Client bytes must match expected stream byte-for-byte"
        );

        let mut db_entry = None;
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if let Ok(logs) =
                crate::modules::proxy_db::get_logs_filtered(&sse_path_find, false, 5, 0)
            {
                if let Some(l) = logs.iter().find(|l| l.url == sse_path_find) {
                    if let Ok(detail) = crate::modules::proxy_db::get_log_detail(&l.id) {
                        if detail.response_body.is_some() {
                            db_entry = Some(detail);
                            break;
                        }
                    }
                }
            }
        }
        let detail = db_entry.expect("DB log must be persisted");
        assert_eq!(detail.input_tokens, Some(10000));
        assert_eq!(detail.output_tokens, Some(150));
        assert_eq!(detail.cached_tokens, Some(8800));
        assert_eq!(detail.session_id.as_deref(), Some("session-spill"));
        let headers: serde_json::Value =
            serde_json::from_str(detail.response_headers.as_deref().unwrap()).unwrap();
        assert!(
            headers["x-timing-stream-ms"]
                .as_str()
                .unwrap()
                .parse::<f64>()
                .unwrap()
                >= 0.0
        );
        assert!(
            headers["x-timing-total-ms"]
                .as_str()
                .unwrap()
                .parse::<f64>()
                .unwrap()
                >= 0.0
        );
        assert_eq!(headers["x-session-id"], "session-spill");
        let body = detail.response_body.expect("DB response_body must exist");
        let db_json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(db_json["_session_thinking_id"], "session-spill");
        assert_eq!(db_json["usage"]["cached_tokens"], 8800);
        assert_eq!(db_json["_timing"]["ttft_s"], 0.025);
        assert!(
            db_json["_timing"]["total_s"].as_f64().unwrap()
                >= db_json["_timing"]["stream_s"].as_f64().unwrap()
        );
        assert_eq!(
            detail.duration,
            (db_json["_timing"]["total_s"].as_f64().unwrap() * 1000.0).round() as u64
        );
        let resp_text = db_json["content"].as_str().unwrap();

        // 2. 数据库全文严格逐字符比对
        let mut expected_business_content = String::new();
        for i in 0..50 {
            expected_business_content.push_str(&format!("chunk_{}_{}", i, "A".repeat(50_000)));
        }
        expected_business_content.push_str(marker);
        assert_eq!(
            resp_text, &expected_business_content,
            "DB content must match full business text character-for-character"
        );
        assert_eq!(
            db_json["bytes_forwarded"].as_u64().unwrap() as usize,
            expected_client_bytes.len()
        );
        assert_eq!(
            db_json["bytes_captured"].as_u64().unwrap() as usize,
            expected_client_bytes.len()
        );
        assert_eq!(
            db_json["stream_capture_status"].as_str().unwrap(),
            "completed"
        );

        // 3. 内存与事件只保留summary，完整正文在DB中。
        let mem_logs = monitor.logs.read().await;
        let sse_log = mem_logs
            .iter()
            .find(|l| l.url == sse_path_find)
            .expect("Memory log must exist");
        assert!(sse_log.response_body.is_none());
        assert!(sse_log.response_headers.is_none());
        assert_eq!(sse_log.cached_tokens, Some(8800));
        assert_eq!(sse_log.session_id.as_deref(), Some("session-spill"));
        drop(mem_logs);

        // 4. 验证临时文件在正常完成后已被自动删除
        let paths = super::get_created_temp_paths();
        assert!(
            !paths.is_empty(),
            "Large SSE stream must create temporary spill files"
        );
        for p in &paths {
            assert!(
                !p.exists(),
                "Temporary spill file at {:?} must be unlinked/deleted from disk after completion",
                p
            );
        }
    }

    #[tokio::test]
    async fn test_stream_failure_injection_does_not_break_client() {
        let _test_guard = STREAM_TEST_LOCK.lock().await;
        let _dir = crate::proxy::monitor::prompt_log_tests::TestDataDir::new();
        super::clear_created_temp_paths();
        use axum::{routing::get, Router};
        use futures::stream;
        use std::sync::Arc;
        use tower::ServiceExt;

        let _ = crate::modules::proxy_db::init_db();
        for mode in [
            super::StreamFailureInjection::FailCreate,
            super::StreamFailureInjection::FailWrite,
            super::StreamFailureInjection::FailFlush,
            super::StreamFailureInjection::FailSeek,
            super::StreamFailureInjection::FailRead,
        ] {
            super::clear_created_temp_paths();
            let _guard = super::FailureInjectionGuard::new(mode);

            let monitor = Arc::new(crate::proxy::monitor::ProxyMonitor::new(10, None));
            monitor.set_enabled(true);
            monitor.set_capture_health_logs(true);
            let app_state = crate::proxy::server::AppState::create_test_state(monitor.clone());

            let mut raw_chunks = Vec::new();
            for i in 0..25 {
                let data = format!(
                    "data: {{\"choices\":[{{\"delta\":{{\"content\":\"part_{}_{}\"}}}}]}}\n\n",
                    i,
                    "X".repeat(50_000)
                );
                raw_chunks.push(axum::body::Bytes::from(data));
            }
            raw_chunks.push(axum::body::Bytes::from("data: [DONE]\n\n"));
            let chunks = Arc::new(raw_chunks);
            let chunks_clone = chunks.clone();
            let fail_url = format!("/sse_fail_{:?}_{}", mode, uuid::Uuid::new_v4());
            let fail_url_req = fail_url.clone();
            let fail_url_find = fail_url.clone();

            let app = Router::new()
                .route(
                    &fail_url,
                    get(move || {
                        let c = chunks_clone.clone();
                        async move {
                            let items: Vec<Result<axum::body::Bytes, std::io::Error>> =
                                (*c).clone().into_iter().map(Ok).collect();
                            (
                                [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                                axum::body::Body::from_stream(stream::iter(items)),
                            )
                        }
                    }),
                )
                .layer(axum::middleware::from_fn_with_state(
                    app_state,
                    super::monitor_middleware,
                ));

            let req = axum::http::Request::builder()
                .method("GET")
                .uri(&fail_url_req)
                .body(axum::body::Body::empty())
                .unwrap();
            let resp = app.oneshot(req).await.unwrap();
            assert_eq!(resp.status(), axum::http::StatusCode::OK);

            let client_bytes = axum::body::to_bytes(resp.into_body(), 10 * 1024 * 1024)
                .await
                .unwrap();
            // 逐字节完整比对，彻底替换仅检查 part_0 的方式
            let expected_client_bytes: Vec<u8> =
                (*chunks).iter().flat_map(|b| b.to_vec()).collect();
            assert_eq!(
                client_bytes.to_vec(),
                expected_client_bytes,
                "Client must receive 100% stream bytes byte-for-byte under failure mode {:?}",
                mode
            );

            // 验证 SQLite 中落盘了失败状态并检查准确计数
            let mut db_entry = None;
            for _ in 0..50 {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                if let Ok(logs) =
                    crate::modules::proxy_db::get_logs_filtered(&fail_url_find, false, 5, 0)
                {
                    if let Some(l) = logs.iter().find(|l| l.url == fail_url_find) {
                        if let Ok(detail) = crate::modules::proxy_db::get_log_detail(&l.id) {
                            db_entry = Some(detail);
                            break;
                        }
                    }
                }
            }
            let detail = db_entry.expect("DB log must be persisted on failure");
            let body = detail.response_body.unwrap_or_default();
            let err = detail.error.unwrap_or_default();
            let diagnostics: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert_eq!(diagnostics["stream_capture_status"], "failed");
            assert_eq!(diagnostics["stream_termination_reason"], "normal_eof");
            assert!(diagnostics["stream_capture_error"]
                .as_str()
                .unwrap()
                .contains("Simulated"));
            assert!(
                body.contains("failed")
                    || body.contains("FAILED")
                    || err.contains("failed")
                    || err.contains("Simulated"),
                "Log must record failure for mode {:?}, got body={}, err={}",
                mode,
                body,
                err
            );

            // 如果是中途写入失败，验证准确计数（部分成功写入之后捕获停止，但转发持续到末尾）
            if mode == super::StreamFailureInjection::FailWrite {
                if let Ok(db_json) = serde_json::from_str::<serde_json::Value>(&body) {
                    let fwd = db_json["bytes_forwarded"].as_u64().unwrap_or(0) as usize;
                    let cap = db_json["bytes_captured"].as_u64().unwrap_or(0) as usize;
                    assert_eq!(
                        fwd,
                        expected_client_bytes.len(),
                        "Forwarded bytes must equal client bytes"
                    );
                    assert!(
                        cap < fwd,
                        "Captured bytes must be strictly less than forwarded on FailWrite: {} < {}",
                        cap,
                        fwd
                    );
                    assert!(
                        cap >= 1024 * 1024,
                        "FailWrite must have successfully written > 1MB before failure, got {}",
                        cap
                    );
                }
            }

            // 验证临时文件资源释放与清理
            let paths = super::get_created_temp_paths();
            if mode == super::StreamFailureInjection::FailCreate {
                assert!(
                    paths.is_empty(),
                    "FailCreate must NOT create any spill files on disk"
                );
            } else {
                assert!(
                    !paths.is_empty(),
                    "Mode {:?} must have created a spill file",
                    mode
                );
                for p in &paths {
                    assert!(
                        !p.exists(),
                        "Spill file at {:?} must be unlinked/deleted from disk under mode {:?}",
                        p,
                        mode
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn test_client_disconnect_marks_interrupted_not_completed() {
        let _test_guard = STREAM_TEST_LOCK.lock().await;
        let _dir = crate::proxy::monitor::prompt_log_tests::TestDataDir::new();
        super::clear_created_temp_paths();
        use axum::{routing::get, Router};
        use futures::{stream, StreamExt};
        use std::sync::Arc;
        use tower::ServiceExt;

        let _ = crate::modules::proxy_db::init_db();
        let monitor = Arc::new(crate::proxy::monitor::ProxyMonitor::new(10, None));
        monitor.set_enabled(true);
        monitor.set_capture_health_logs(true);
        let app_state = crate::proxy::server::AppState::create_test_state(monitor.clone());

        let url = format!("/sse_disconnect_{}", uuid::Uuid::new_v4());
        let url_req = url.clone();
        let url_find = url.clone();

        let chunks: Vec<axum::body::Bytes> = (0..100)
            .map(|i| {
                axum::body::Bytes::from(format!(
                    "data: {{\"choices\":[{{\"delta\":{{\"content\":\"chunk_{}\"}}}}]}}\n\n",
                    i
                ))
            })
            .collect();
        let chunks_arc = Arc::new(chunks);

        let app = Router::new()
            .route(
                &url,
                get(move || {
                    let c = chunks_arc.clone();
                    async move {
                        let items: Vec<Result<axum::body::Bytes, std::io::Error>> =
                            (*c).clone().into_iter().map(Ok).collect();
                        (
                            [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                            axum::body::Body::from_stream(stream::iter(items)),
                        )
                    }
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                app_state,
                super::monitor_middleware,
            ));

        let req = axum::http::Request::builder()
            .method("GET")
            .uri(&url_req)
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        let mut body_stream = resp.into_body().into_data_stream();
        let first = body_stream.next().await.unwrap().unwrap();
        assert!(String::from_utf8_lossy(&first).contains("chunk_0"));
        drop(body_stream); // 客户端主动断开

        let mut db_entry = None;
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if let Ok(logs) = crate::modules::proxy_db::get_logs_filtered(&url_find, false, 5, 0) {
                if let Some(l) = logs.iter().find(|l| l.url == url_find) {
                    if let Ok(detail) = crate::modules::proxy_db::get_log_detail(&l.id) {
                        if detail.response_body.is_some() {
                            db_entry = Some(detail);
                            break;
                        }
                    }
                }
            }
        }
        let detail = db_entry.expect("DB log must be persisted");
        let body = detail.response_body.expect("Response body must exist");
        let db_json: serde_json::Value = serde_json::from_str(&body).unwrap();
        let status = db_json["stream_capture_status"].as_str().unwrap();
        assert_ne!(
            status, "completed",
            "Client disconnect must NOT be marked as completed!"
        );
        assert_eq!(
            db_json["stream_termination_reason"].as_str().unwrap(),
            "client_cancelled"
        );
        assert_eq!(status, "partial");
        assert!(db_json.get("stream_capture_error").is_none());
        assert!(db_json["_timing"].is_object());

        // 验证客户端断开后临时文件已被自动删除
        let paths = super::get_created_temp_paths();
        for p in &paths {
            assert!(
                !p.exists(),
                "Temporary spill file must be unlinked/deleted from disk after client disconnect"
            );
        }
    }

    #[tokio::test]
    async fn test_upstream_error_marks_upstream_error_not_completed() {
        let _test_guard = STREAM_TEST_LOCK.lock().await;
        let _dir = crate::proxy::monitor::prompt_log_tests::TestDataDir::new();
        super::clear_created_temp_paths();
        use axum::{routing::get, Router};
        use futures::{stream, StreamExt};
        use std::sync::Arc;
        use tower::ServiceExt;

        let _ = crate::modules::proxy_db::init_db();
        let monitor = Arc::new(crate::proxy::monitor::ProxyMonitor::new(10, None));
        monitor.set_enabled(true);
        monitor.set_capture_health_logs(true);
        let app_state = crate::proxy::server::AppState::create_test_state(monitor.clone());

        let url = format!("/sse_upstream_err_{}", uuid::Uuid::new_v4());
        let url_req = url.clone();
        let url_find = url.clone();

        let app = Router::new()
            .route(
                &url,
                get(|| async move {
                    let items = vec![
                        Ok(axum::body::Bytes::from(
                            "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n",
                        )),
                        Err(std::io::Error::new(
                            std::io::ErrorKind::ConnectionReset,
                            "Upstream died unexpectedly",
                        )),
                    ];
                    (
                        [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                        axum::body::Body::from_stream(stream::iter(items)),
                    )
                }),
            )
            .layer(axum::middleware::from_fn_with_state(
                app_state,
                super::monitor_middleware,
            ));

        let req = axum::http::Request::builder()
            .method("GET")
            .uri(&url_req)
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        let mut body_stream = resp.into_body().into_data_stream();
        let _ = body_stream.next().await;
        let second = body_stream.next().await;
        assert!(
            second.is_some() && second.unwrap().is_err(),
            "Client must observe upstream error"
        );

        let mut db_entry = None;
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if let Ok(logs) = crate::modules::proxy_db::get_logs_filtered(&url_find, false, 5, 0) {
                if let Some(l) = logs.iter().find(|l| l.url == url_find) {
                    if let Ok(detail) = crate::modules::proxy_db::get_log_detail(&l.id) {
                        if detail.response_body.is_some() {
                            db_entry = Some(detail);
                            break;
                        }
                    }
                }
            }
        }
        let detail = db_entry.expect("DB log must be persisted");
        let body = detail.response_body.expect("Response body must exist");
        let db_json: serde_json::Value = serde_json::from_str(&body).unwrap();
        let status = db_json["stream_capture_status"].as_str().unwrap();
        assert_ne!(
            status, "completed",
            "Upstream error must NOT be marked as completed!"
        );
        assert_eq!(
            db_json["stream_termination_reason"].as_str().unwrap(),
            "upstream_error"
        );
        assert_eq!(status, "partial");
        assert!(db_json.get("stream_capture_error").is_none());
        assert!(db_json["_timing"].is_object());

        // 验证上游错误后临时文件已被自动删除
        let paths = super::get_created_temp_paths();
        for p in &paths {
            assert!(
                !p.exists(),
                "Temporary spill file must be unlinked/deleted from disk after upstream error"
            );
        }
    }

    #[test]
    fn test_extract_tokens_anthropic() {
        use serde_json::json;
        let usage = json!({
            "input_tokens": 1200,
            "cache_read_input_tokens": 8800,
            "cache_creation_input_tokens": 0,
            "output_tokens": 150
        });
        assert_eq!(super::extract_input_tokens(&usage), Some(10000));
        assert_eq!(super::extract_cached_tokens(&usage), Some(8800));
        assert_eq!(super::extract_output_tokens(&usage), Some(150));
    }

    #[test]
    fn test_extract_tokens_openai_chat() {
        use serde_json::json;
        let usage = json!({
            "prompt_tokens": 10000,
            "completion_tokens": 150,
            "total_tokens": 10150,
            "prompt_tokens_details": {
                "cached_tokens": 8800
            }
        });
        assert_eq!(super::extract_input_tokens(&usage), Some(10000));
        assert_eq!(super::extract_cached_tokens(&usage), Some(8800));
        assert_eq!(super::extract_output_tokens(&usage), Some(150));
    }

    #[test]
    fn test_extract_tokens_openai_responses() {
        use serde_json::json;
        let usage = json!({
            "input_tokens": 10000,
            "output_tokens": 150,
            "total_tokens": 10150,
            "input_tokens_details": {
                "cached_tokens": 8800
            }
        });
        assert_eq!(super::extract_input_tokens(&usage), Some(10000));
        assert_eq!(super::extract_cached_tokens(&usage), Some(8800));
        assert_eq!(super::extract_output_tokens(&usage), Some(150));
    }

    #[test]
    fn test_extract_tokens_gemini_raw() {
        use serde_json::json;
        let usage = json!({
            "promptTokenCount": 10000,
            "candidatesTokenCount": 150,
            "totalTokenCount": 10150,
            "cachedContentTokenCount": 8800
        });
        assert_eq!(super::extract_input_tokens(&usage), Some(10000));
        assert_eq!(super::extract_cached_tokens(&usage), Some(8800));
        assert_eq!(super::extract_output_tokens(&usage), Some(150));
    }

    #[test]
    fn test_should_skip_request_log_only_suppresses_successful_get() {
        use axum::http::StatusCode;

        // 胶囊开关关闭（默认）：GET 成功请求一律不记录 —— 含 /v1/models 轮询（本次修复的核心场景）
        // 与 /health 探针，且与路径无关（GET 成功即噪音）
        for path in [
            "/v1/models",
            "/v1/models?limit=100",
            "/v1beta/models",
            "/v1/models/claude",
            "/health",
            "/healthz?t=123",
            "/api/health/",
            "/stats/overview",
        ] {
            for status in [200, 204, 201] {
                assert!(
                    super::should_skip_request_log(
                        "GET",
                        StatusCode::from_u16(status).unwrap(),
                        false
                    ),
                    "GET {status} {path} 应被抑制"
                );
            }
        }

        // 大写的 GET 同样识别
        assert!(super::should_skip_request_log("get", StatusCode::OK, false));

        // 失败请求始终记录（供排障）—— 无论路径与方法
        for status in [400, 401, 403, 404, 429, 500, 502, 503, 504] {
            assert!(
                !super::should_skip_request_log(
                    "GET",
                    StatusCode::from_u16(status).unwrap(),
                    false
                ),
                "GET {status} 必须记录"
            );
        }

        // 非 GET 业务请求始终记录
        for method in ["POST", "PUT", "PATCH", "DELETE", "post"] {
            assert!(
                !super::should_skip_request_log(method, StatusCode::OK, false),
                "{method} 200 必须记录"
            );
        }

        // 开关开启（或 ABV_LOG_HEALTH_CHECKS=true）→ 全部记录并落库
        for (method, status) in [("GET", 200), ("GET", 503), ("POST", 200)] {
            assert!(
                !super::should_skip_request_log(
                    method,
                    StatusCode::from_u16(status).unwrap(),
                    true
                ),
                "开关开启时 {method} {status} 必须记录"
            );
        }
    }
}
