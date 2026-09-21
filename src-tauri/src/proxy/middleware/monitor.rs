use crate::proxy::middleware::auth::UserTokenIdentity;
use crate::proxy::monitor::ProxyRequestLog;
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


fn create_named_temp_file() -> std::io::Result<tempfile::NamedTempFile> {
    #[cfg(test)]
    if get_stream_failure_injection() == StreamFailureInjection::FailCreate {
        return Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "Simulated tempfile creation failure"));
    }
    tempfile::NamedTempFile::new()
}

struct SpillWriter {
    inner: std::io::BufWriter<tempfile::NamedTempFile>,
    bytes_written: usize,
}

impl std::io::Write for SpillWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        #[cfg(test)]
        if get_stream_failure_injection() == StreamFailureInjection::FailWrite && self.bytes_written > 1024 * 1024 {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "Simulated disk write failure"));
        }
        let n = self.inner.write(buf)?;
        self.bytes_written += n;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        #[cfg(test)]
        if get_stream_failure_injection() == StreamFailureInjection::FailFlush {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "Simulated flush failure"));
        }
        self.inner.flush()
    }
}

fn seek_and_open_reader(mut temp_file: tempfile::NamedTempFile) -> std::io::Result<Box<dyn std::io::BufRead + Send>> {
    #[cfg(test)]
    if get_stream_failure_injection() == StreamFailureInjection::FailSeek {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Simulated seek failure"));
    }
    use std::io::Seek;
    temp_file.seek(std::io::SeekFrom::Start(0))?;

    #[cfg(test)]
    if get_stream_failure_injection() == StreamFailureInjection::FailRead {
        struct FailingRead(tempfile::NamedTempFile);
        impl std::io::Read for FailingRead {
            fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Simulated read failure mid-stream"))
            }
        }
        return Ok(Box::new(std::io::BufReader::new(FailingRead(temp_file))));
    }

    Ok(Box::new(std::io::BufReader::new(temp_file)))
}

const MAX_REQUEST_LOG_SIZE: usize = 100 * 1024 * 1024; // 100MB: HTTP payload forwarding limit (not logging budget)
const MAX_RESPONSE_LOG_SIZE: usize = 100 * 1024 * 1024; // 100MB: HTTP payload forwarding limit (not logging budget)
const MAX_STREAM_CAPTURE_BYTES: usize = 2 * 1024 * 1024; // 2MB: stream logging capture buffer (separate from forwarding)
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
    usage
        .get("cache_read_input_tokens")
        .or_else(|| usage.get("total_cached_tokens"))
        .or_else(|| usage.get("cachedContentTokenCount"))
        .or_else(|| {
            usage
                .get("prompt_tokens_details")
                .and_then(|details| details.get("cached_tokens"))
        })
        .or_else(|| {
            usage
                .get("input_tokens_details")
                .and_then(|details| details.get("cached_tokens"))
        })
        .and_then(|v| v.as_u64())
        .map(|v| v as u32)
}

fn value_as_u32(value: Option<&Value>) -> Option<u32> {
    value.and_then(|v| v.as_u64()).map(|v| v as u32)
}

fn extract_input_tokens(usage: &Value) -> Option<u32> {
    value_as_u32(
        usage
            .get("prompt_tokens")
            .or_else(|| usage.get("input_tokens"))
            .or_else(|| usage.get("total_input_tokens"))
            .or_else(|| usage.get("promptTokenCount")),
    )
}

fn extract_reasoning_tokens(usage: &Value) -> Option<u32> {
    value_as_u32(
        usage
            .get("reasoning_tokens")
            .or_else(|| {
                usage
                    .get("output_tokens_details")
                    .and_then(|details| details.get("reasoning_tokens"))
            })
            .or_else(|| {
                usage
                    .get("completion_tokens_details")
                    .and_then(|details| details.get("reasoning_tokens"))
            })
            .or_else(|| usage.get("total_thought_tokens"))
            .or_else(|| usage.get("totalThoughtTokens"))
            .or_else(|| usage.get("thoughtsTokenCount")),
    )
}

fn extract_output_tokens(usage: &Value) -> Option<u32> {
    if let Some(tokens) = value_as_u32(
        usage
            .get("completion_tokens")
            .or_else(|| usage.get("output_tokens")),
    ) {
        return Some(tokens);
    }

    let base = value_as_u32(
        usage
            .get("total_output_tokens")
            .or_else(|| usage.get("candidatesTokenCount")),
    )?;
    let has_new_format = usage.get("total_output_tokens").is_some();
    if has_new_format {
        let reasoning = extract_reasoning_tokens(usage).unwrap_or(0);
        let tool_use = value_as_u32(usage.get("total_tool_use_tokens")).unwrap_or(0);
        Some(base + reasoning + tool_use)
    } else {
        Some(base)
    }
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
            .map(|s| s.to_string())
    } else {
        None
    };

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

    let response = next.run(request).await;

    // user_token_identity 已在上面从请求 extensions 中提取

    let duration = start.elapsed().as_millis() as u64;
    let status = response.status().as_u16();

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
    let protocol = if uri.contains("/v1/messages") {
        Some("anthropic".to_string())
    } else if uri.contains("/v1beta/models") {
        Some("gemini".to_string())
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
        response_body: None,
        input_tokens: None,
        output_tokens: None,
        cached_tokens: None,
        protocol,
        username,
    };

    if content_type.contains("text/event-stream") {
        let (parts, body) = response.into_parts();
        let mut stream = body.into_data_stream();
        let (tx, rx) = tokio::sync::mpsc::channel(64);

        tokio::spawn(async move {
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

            #[cfg(test)]
            let fail_mode = get_stream_failure_injection();

            while let Some(chunk_res) = next_chunk_while_receiver_open(&mut stream, &tx).await {
                if let Ok(chunk) = chunk_res {
                    // 1. 优先转发客户端：日志系统任何 I/O 故障绝不阻塞或截断客户端数据流
                    if tx.send(Ok::<_, axum::Error>(chunk.clone())).await.is_err() {
                        client_disconnected = true;
                        break;
                    }
                    bytes_forwarded += chunk.len();

                    // 2. 捕获并落盘
                    if capture_error.is_none() && bytes_captured + chunk.len() <= MAX_STREAM_PERSIST_BYTES {
                        match &mut sink {
                            StreamCaptureSink::Memory(buf) => {
                                if buf.len() + chunk.len() <= STREAM_MEMORY_SPILL_BYTES {
                                    buf.extend_from_slice(&chunk);
                                    bytes_captured += chunk.len();
                                } else {
                                    #[cfg(test)]
                                    if fail_mode == StreamFailureInjection::FailCreate {
                                        capture_error = Some("Simulated tempfile creation failure".to_string());
                                        sink = StreamCaptureSink::Failed;
                                        continue;
                                    }

                                    match tempfile::NamedTempFile::new() {
                                        Ok(mut temp) => {
                                            use std::io::Write;
                                            if let Err(e) = temp.write_all(buf).and_then(|_| temp.write_all(&chunk)) {
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
                                            capture_error = Some(format!("Failed to create temporary spill file: {}", e));
                                            sink = StreamCaptureSink::Failed;
                                        }
                                    }
                                }
                            }
                            StreamCaptureSink::Spill(writer) => {
                                use std::io::Write;
                                #[cfg(test)]
                                if fail_mode == StreamFailureInjection::FailWrite && bytes_captured > 1024 * 1024 {
                                    capture_error = Some("Simulated disk write failure".to_string());
                                    sink = StreamCaptureSink::Failed;
                                    continue;
                                }

                                if let Err(e) = writer.write_all(&chunk) {
                                    capture_error = Some(format!("Failed to write chunk to spill file: {}", e));
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
                                    capture_error = Some(format!("Failed to seek spill file: {}", e));
                                    Box::new(std::io::Cursor::new(Vec::new()))
                                }
                            },
                            Err(e) => {
                                capture_error = Some(format!("Failed to unwrap spill writer: {}", e));
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
                        capture_error = Some(format!("Failed to read stream line from spill file: {}", e));
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
                                    // Main response content
                                    if let Some(content) =
                                        delta.get("content").and_then(|v| v.as_str())
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
                                                if let Some(id) =
                                                    tc.get("id").and_then(|v| v.as_str())
                                                {
                                                    current_tc["id"] =
                                                        Value::String(id.to_string());
                                                }
                                                if let Some(func) = tc.get("function") {
                                                    if let Some(name) =
                                                        func.get("name").and_then(|v| v.as_str())
                                                    {
                                                        current_tc["function"]["name"] =
                                                            Value::String(name.to_string());
                                                    }
                                                    if let Some(args) = func
                                                        .get("arguments")
                                                        .and_then(|v| v.as_str())
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

                        // Claude/Anthropic format: content_block_start, content_block_delta, etc.
                        let msg_type = json.get("type").and_then(|t| t.as_str());
                        match msg_type {
                            Some("content_block_start") => {
                                if let (Some(index), Some(block)) = (
                                    json.get("index").and_then(|i| i.as_u64()),
                                    json.get("content_block"),
                                ) {
                                    let idx = index as usize;
                                    if block.get("type").and_then(|t| t.as_str())
                                        == Some("tool_use")
                                    {
                                        let id =
                                            block.get("id").and_then(|v| v.as_str()).unwrap_or("");
                                        let name = block
                                            .get("name")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or("");
                                        while tool_calls.len() <= idx {
                                            tool_calls.push(Value::Null);
                                        }
                                        tool_calls[idx] = serde_json::json!({
                                            "id": id,
                                            "type": "function",
                                            "function": { "name": name, "arguments": "" }
                                        });
                                    }
                                }
                            }
                            Some("content_block_delta") => {
                                if let (Some(index), Some(delta)) = (
                                    json.get("index").and_then(|i| i.as_u64()),
                                    json.get("delta"),
                                ) {
                                    let idx = index as usize;

                                    // Tool use input delta
                                    if let Some(delta_json) =
                                        delta.get("input_json_delta").and_then(|v| v.as_str())
                                    {
                                        if idx < tool_calls.len() && !tool_calls[idx].is_null() {
                                            let old_args = tool_calls[idx]["function"]["arguments"]
                                                .as_str()
                                                .unwrap_or("");
                                            tool_calls[idx]["function"]["arguments"] =
                                                Value::String(format!(
                                                    "{}{}",
                                                    old_args, delta_json
                                                ));
                                        }
                                    }
                                    // Legacy/Native thinking block
                                    if let Some(thinking) =
                                        delta.get("thinking").and_then(|v| v.as_str())
                                    {
                                        thinking_content.push_str(thinking);
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
                                        let id = item
                                            .get("call_id")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or("");
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
                                if let Some(thinking) =
                                    delta.get("thinking").and_then(|v| v.as_str())
                                {
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

                // Build consolidated response object
                let mut consolidated = serde_json::Map::new();
                let has_actual_content = !response_content.is_empty()
                    || !tool_calls.is_empty()
                    || !thinking_content.is_empty();

                if !thinking_content.is_empty() {
                    consolidated.insert("thinking".to_string(), Value::String(thinking_content));
                }
                if !thinking_signature.is_empty() {
                    consolidated.insert(
                        "thinking_signature".to_string(),
                        Value::String(thinking_signature),
                    );
                }
                if !response_content.is_empty() {
                    consolidated.insert("content".to_string(), Value::String(response_content));
                }

                if !tool_calls.is_empty() {
                    let clean_tool_calls: Vec<Value> =
                        tool_calls.into_iter().filter(|v| !v.is_null()).collect();
                    if !clean_tool_calls.is_empty() {
                        consolidated
                            .insert("tool_calls".to_string(), Value::Array(clean_tool_calls));
                    }
                }
                if has_actual_content {
                    let mut usage_obj = serde_json::Map::new();
                    let input_toks = log.input_tokens.unwrap_or(0);
                    let output_toks = log.output_tokens.unwrap_or(0);
                    let cached_toks = cached_tokens.unwrap_or(0);
                    let reasoning_toks = reasoning_tokens.unwrap_or(0);
                    let is_responses_api =
                        log.url.contains("/responses") || log.url.contains("/interactions");

                    if is_responses_api {
                        usage_obj
                            .insert("input_tokens".to_string(), Value::Number(input_toks.into()));
                        usage_obj.insert(
                            "input_tokens_details".to_string(),
                            serde_json::json!({ "cached_tokens": cached_toks }),
                        );
                        usage_obj.insert(
                            "output_tokens".to_string(),
                            Value::Number(output_toks.into()),
                        );
                        usage_obj.insert(
                            "output_tokens_details".to_string(),
                            serde_json::json!({ "reasoning_tokens": reasoning_toks }),
                        );
                        usage_obj.insert(
                            "total_tokens".to_string(),
                            Value::Number((input_toks + output_toks).into()),
                        );
                    } else {
                        usage_obj.insert(
                            "prompt_tokens".to_string(),
                            Value::Number(input_toks.into()),
                        );
                        usage_obj.insert(
                            "completion_tokens".to_string(),
                            Value::Number(output_toks.into()),
                        );
                        usage_obj.insert(
                            "total_tokens".to_string(),
                            Value::Number((input_toks + output_toks).into()),
                        );
                        if cached_tokens.is_some() {
                            usage_obj.insert(
                                "cache_read_input_tokens".to_string(),
                                Value::Number(cached_toks.into()),
                            );
                            usage_obj.insert(
                                "prompt_tokens_details".to_string(),
                                serde_json::json!({ "cached_tokens": cached_toks }),
                            );
                        }
                        if reasoning_tokens.is_some() {
                            usage_obj.insert(
                                "completion_tokens_details".to_string(),
                                serde_json::json!({ "reasoning_tokens": reasoning_toks }),
                            );
                        }
                    }
                    consolidated.insert("usage".to_string(), Value::Object(usage_obj));
                }

                consolidated.insert("bytes_forwarded".to_string(), serde_json::json!(bytes_forwarded));
                consolidated.insert("bytes_captured".to_string(), serde_json::json!(bytes_captured));

                if let Some(ref err) = capture_error {
                    consolidated.insert("stream_capture_status".to_string(), serde_json::json!("failed"));
                    consolidated.insert("stream_capture_error".to_string(), serde_json::json!(err));
                    if log.error.is_none() {
                        log.error = Some(format!("Stream capture failed: {} (forwarded: {}, captured: {})", err, bytes_forwarded, bytes_captured));
                    }
                } else if client_disconnected {
                    consolidated.insert("stream_capture_status".to_string(), serde_json::json!("client_disconnected"));
                    if log.error.is_none() {
                        log.error = Some(format!("Client disconnected prematurely after {} bytes forwarded (captured: {})", bytes_forwarded, bytes_captured));
                    }
                } else if let Some(ref uerr) = upstream_error {
                    consolidated.insert("stream_capture_status".to_string(), serde_json::json!("upstream_error"));
                    consolidated.insert("upstream_error".to_string(), serde_json::json!(uerr));
                    if log.error.is_none() {
                        log.error = Some(format!("Upstream error during stream: {}", uerr));
                    }
                } else if stream_truncated {
                    consolidated.insert("stream_capture_status".to_string(), serde_json::json!("truncated"));
                    consolidated.insert("stream_truncated".to_string(), Value::Bool(true));
                } else {
                    consolidated.insert("stream_capture_status".to_string(), serde_json::json!("completed"));
                }

                if consolidated.is_empty() {
                    if let Some(ref err) = capture_error {
                        log.response_body = Some(format!("[STREAM CAPTURE FAILED: {} (forwarded: {}, captured: {})]", err, bytes_forwarded, bytes_captured));
                    } else {
                        log.response_body = Some(format!("[Stream Data: forwarded {} bytes, captured {} bytes]", bytes_forwarded, bytes_captured));
                    }
                } else {
                    log.response_body = Some(
                        serde_json::to_string_pretty(&Value::Object(consolidated))
                            .unwrap_or_else(|_| format!("[Consolidated Stream Data: {} bytes]", bytes_forwarded)),
                    );
                }

            // Fallback token extraction from tail if not already extracted
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
                                    log.cached_tokens =
                                        log.cached_tokens.or_else(|| extract_cached_tokens(usage));
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            if log.status >= 400 {
                log.error = Some("Stream Error or Failed".to_string());
            }

            // [FIX #3325] Fallback input token estimation for stream responses
            if log.input_tokens.is_none() {
                if let Some(ref req_body) = log.request_body {
                    let estimated = crate::proxy::mappers::context_manager::estimate_raw_tokens_from_payload(req_body);
                    if estimated > 0 {
                        log.input_tokens = Some(estimated);
                    }
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
                    } else {
                        log.response_body = Some(s.to_string());
                    }
                } else {
                    log.response_body = Some("[Binary Response Data]".to_string());
                }

                if log.status >= 400 {
                    log.error = log.response_body.clone();
                }

                // [FIX #3325] Fallback input token estimation if upstream returned an error (no usage metadata)
                if log.input_tokens.is_none() {
                    if let Some(ref req_body) = log.request_body {
                        let estimated = crate::proxy::mappers::context_manager::estimate_raw_tokens_from_payload(req_body);
                        if estimated > 0 {
                            log.input_tokens = Some(estimated);
                        }
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
        assert_eq!(super::MAX_STREAM_CAPTURE_BYTES, 2 * 1024 * 1024);
        assert_eq!(super::MAX_RESPONSE_LOG_SIZE, 100 * 1024 * 1024);
    }

    #[tokio::test]
    async fn test_monitor_middleware_large_payload_forwarding() {
        use axum::{routing::post, Router};
        use tower::ServiceExt;

        let monitor = std::sync::Arc::new(crate::proxy::monitor::ProxyMonitor::new(10, None));
        monitor.set_enabled(true);
        let app_state = crate::proxy::server::AppState::create_test_state(monitor);

        let app = Router::new()
            .route(
                "/echo",
                post(|body: axum::body::Body| async move {
                    let bytes = axum::body::to_bytes(body, 50 * 1024 * 1024)
                        .await
                        .unwrap();
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
                    let bytes = axum::body::to_bytes(body, 50 * 1024 * 1024)
                        .await
                        .unwrap();
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
        super::set_stream_failure_injection(super::StreamFailureInjection::None);
        use axum::{routing::get, Router};
        use tower::ServiceExt;
        use futures::stream;
        use std::sync::Arc;

        let _ = crate::modules::proxy_db::init_db();
        let monitor = Arc::new(crate::proxy::monitor::ProxyMonitor::new(10, None));
        monitor.set_enabled(true);
        let app_state = crate::proxy::server::AppState::create_test_state(monitor.clone());

        let marker = "_TAIL_MARKER_98765_";
        // 构造多个 chunk，总共 > 2.5 MiB
        let mut raw_chunks = Vec::new();
        for i in 0..50 {
            let data = format!("data: {{\"choices\":[{{\"delta\":{{\"content\":\"chunk_{}_{}\"}}}}]}}\n\n", i, "A".repeat(50_000));
            raw_chunks.push(axum::body::Bytes::from(data));
        }
        let tail_data = format!("data: {{\"choices\":[{{\"delta\":{{\"content\":\"{}\"}}}}]}}\n\ndata: [DONE]\n\n", marker);
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
                        let items: Vec<Result<axum::body::Bytes, std::io::Error>> = (*chunks).clone().into_iter().map(Ok).collect();
                        let stream = stream::iter(items);
                        (
                            [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
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

        let client_bytes = axum::body::to_bytes(resp.into_body(), 50 * 1024 * 1024).await.unwrap();
        // 1. 客户端完整数据逐字节比对
        let expected_client_bytes: Vec<u8> = (*chunks_arc).iter().flat_map(|b| b.to_vec()).collect();
        assert_eq!(client_bytes.to_vec(), expected_client_bytes, "Client bytes must match expected stream byte-for-byte");

        let mut db_entry = None;
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if let Ok(logs) = crate::modules::proxy_db::get_logs_filtered(&sse_path_find, false, 5, 0) {
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
        let body = detail.response_body.expect("DB response_body must exist");
        let db_json: serde_json::Value = serde_json::from_str(&body).unwrap();
        let resp_text = db_json["content"].as_str().unwrap();

        // 2. 数据库全文严格逐字符比对
        let mut expected_business_content = String::new();
        for i in 0..50 {
            expected_business_content.push_str(&format!("chunk_{}_{}", i, "A".repeat(50_000)));
        }
        expected_business_content.push_str(marker);
        assert_eq!(resp_text, &expected_business_content, "DB content must match full business text character-for-character");
        assert_eq!(db_json["bytes_forwarded"].as_u64().unwrap() as usize, expected_client_bytes.len());
        assert_eq!(db_json["bytes_captured"].as_u64().unwrap() as usize, expected_client_bytes.len());
        assert_eq!(db_json["stream_capture_status"].as_str().unwrap(), "completed");

        // 3. 验证内存中预览日志的容量上限与截断标识
        let mem_logs = monitor.logs.read().await;
        let sse_log = mem_logs.iter().find(|l| l.url == sse_path_find).expect("Memory log must exist");
        let mem_body = sse_log.response_body.as_ref().expect("Memory response_body must exist");
        assert!(mem_body.len() <= 16 * 1024 + 64);
        assert!(mem_body.capacity() <= 16 * 1024 + 64);
        assert!(mem_body.contains("[TRUNCATED FOR IN-MEMORY CACHE]"));
    }

    #[tokio::test]
    async fn test_stream_failure_injection_does_not_break_client() {
        let _test_guard = STREAM_TEST_LOCK.lock().await;
        use axum::{routing::get, Router};
        use tower::ServiceExt;
        use futures::stream;
        use std::sync::Arc;

        let _ = crate::modules::proxy_db::init_db();
        for mode in [
            super::StreamFailureInjection::FailCreate,
            super::StreamFailureInjection::FailWrite,
            super::StreamFailureInjection::FailFlush,
            super::StreamFailureInjection::FailSeek,
            super::StreamFailureInjection::FailRead,
        ] {
            let _guard = super::FailureInjectionGuard::new(mode);

        let monitor = Arc::new(crate::proxy::monitor::ProxyMonitor::new(10, None));
        monitor.set_enabled(true);
        let app_state = crate::proxy::server::AppState::create_test_state(monitor.clone());

        let mut raw_chunks = Vec::new();
        for i in 0..25 {
            let data = format!("data: {{\"choices\":[{{\"delta\":{{\"content\":\"part_{}_{}\"}}}}]}}\n\n", i, "X".repeat(50_000));
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
                        let items: Vec<Result<axum::body::Bytes, std::io::Error>> = (*c).clone().into_iter().map(Ok).collect();
                        ([(axum::http::header::CONTENT_TYPE, "text/event-stream")], axum::body::Body::from_stream(stream::iter(items)))
                    }
                }),
            )
            .layer(axum::middleware::from_fn_with_state(app_state, super::monitor_middleware));

        let req = axum::http::Request::builder().method("GET").uri(&fail_url_req).body(axum::body::Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        let client_bytes = axum::body::to_bytes(resp.into_body(), 10 * 1024 * 1024).await.unwrap();
        // 逐字节完整比对，彻底替换仅检查 part_0 的方式
        let expected_client_bytes: Vec<u8> = (*chunks).iter().flat_map(|b| b.to_vec()).collect();
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
            if let Ok(logs) = crate::modules::proxy_db::get_logs_filtered(&fail_url_find, false, 5, 0) {
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
        assert!(
            body.contains("failed") || body.contains("FAILED") || err.contains("failed") || err.contains("Simulated"),
            "Log must record failure for mode {:?}, got body={}, err={}", mode, body, err
        );

        // 如果是中途写入失败，验证准确计数（部分成功写入之后捕获停止，但转发持续到末尾）
        if mode == super::StreamFailureInjection::FailWrite {
            if let Ok(db_json) = serde_json::from_str::<serde_json::Value>(&body) {
                let fwd = db_json["bytes_forwarded"].as_u64().unwrap_or(0) as usize;
                let cap = db_json["bytes_captured"].as_u64().unwrap_or(0) as usize;
                assert_eq!(fwd, expected_client_bytes.len(), "Forwarded bytes must equal client bytes");
                assert!(cap < fwd, "Captured bytes must be strictly less than forwarded on FailWrite: {} < {}", cap, fwd);
                assert!(cap >= 1024 * 1024, "FailWrite must have successfully written > 1MB before failure, got {}", cap);
            }
        }
        }
    }


    #[tokio::test]
    async fn test_client_disconnect_marks_interrupted_not_completed() {
        let _test_guard = STREAM_TEST_LOCK.lock().await;
        use axum::{routing::get, Router};
        use tower::ServiceExt;
        use futures::{stream, StreamExt};
        use std::sync::Arc;

        let _ = crate::modules::proxy_db::init_db();
        let monitor = Arc::new(crate::proxy::monitor::ProxyMonitor::new(10, None));
        monitor.set_enabled(true);
        let app_state = crate::proxy::server::AppState::create_test_state(monitor.clone());

        let url = format!("/sse_disconnect_{}", uuid::Uuid::new_v4());
        let url_req = url.clone();
        let url_find = url.clone();

        let chunks: Vec<axum::body::Bytes> = (0..100)
            .map(|i| axum::body::Bytes::from(format!("data: {{\"choices\":[{{\"delta\":{{\"content\":\"chunk_{}\"}}}}]}}\n\n", i)))
            .collect();
        let chunks_arc = Arc::new(chunks);

        let app = Router::new()
            .route(
                &url,
                get(move || {
                    let c = chunks_arc.clone();
                    async move {
                        let items: Vec<Result<axum::body::Bytes, std::io::Error>> = (*c).clone().into_iter().map(Ok).collect();
                        ([(axum::http::header::CONTENT_TYPE, "text/event-stream")], axum::body::Body::from_stream(stream::iter(items)))
                    }
                }),
            )
            .layer(axum::middleware::from_fn_with_state(app_state, super::monitor_middleware));

        let req = axum::http::Request::builder().method("GET").uri(&url_req).body(axum::body::Body::empty()).unwrap();
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
        assert_ne!(status, "completed", "Client disconnect must NOT be marked as completed!");
        assert_eq!(status, "client_disconnected");
    }


    #[tokio::test]
    async fn test_upstream_error_marks_upstream_error_not_completed() {
        let _test_guard = STREAM_TEST_LOCK.lock().await;
        use axum::{routing::get, Router};
        use tower::ServiceExt;
        use futures::{stream, StreamExt};
        use std::sync::Arc;

        let _ = crate::modules::proxy_db::init_db();
        let monitor = Arc::new(crate::proxy::monitor::ProxyMonitor::new(10, None));
        monitor.set_enabled(true);
        let app_state = crate::proxy::server::AppState::create_test_state(monitor.clone());

        let url = format!("/sse_upstream_err_{}", uuid::Uuid::new_v4());
        let url_req = url.clone();
        let url_find = url.clone();

        let app = Router::new()
            .route(
                &url,
                get(|| async move {
                    let items = vec![
                        Ok(axum::body::Bytes::from("data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n")),
                        Err(std::io::Error::new(std::io::ErrorKind::ConnectionReset, "Upstream died unexpectedly")),
                    ];
                    ([(axum::http::header::CONTENT_TYPE, "text/event-stream")], axum::body::Body::from_stream(stream::iter(items)))
                }),
            )
            .layer(axum::middleware::from_fn_with_state(app_state, super::monitor_middleware));

        let req = axum::http::Request::builder().method("GET").uri(&url_req).body(axum::body::Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), axum::http::StatusCode::OK);

        let mut body_stream = resp.into_body().into_data_stream();
        let _ = body_stream.next().await;
        let second = body_stream.next().await;
        assert!(second.is_some() && second.unwrap().is_err(), "Client must observe upstream error");

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
        assert_ne!(status, "completed", "Upstream error must NOT be marked as completed!");
        assert_eq!(status, "upstream_error");
    }

}
