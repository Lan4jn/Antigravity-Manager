use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime};

// Node.js proxy uses 2 hours TTL
const SIGNATURE_TTL: Duration = Duration::from_secs(2 * 60 * 60);
const MIN_SIGNATURE_LENGTH: usize = 50;

// Different cache limits for different layers
const TOOL_CACHE_LIMIT: usize = 500; // Layer 1: Tool-specific signatures
const FAMILY_CACHE_LIMIT: usize = 200; // Layer 2: Model family mappings
const SESSION_CACHE_LIMIT: usize = 1000; // Layer 3: Session-based signatures (largest)

/// 单个会话内保留的最大签名轮次数（spec G4）
const SESSION_SIGNATURE_HISTORY_LIMIT: usize = 256;
/// 单个会话内签名 String::capacity() 合计上限（spec G4: 1 MiB）
const SESSION_SIGNATURE_HISTORY_BYTES: usize = 1024 * 1024;
/// 单条 reasoning 文本最大允许字节数（128KB）；超出则拒绝缓存（不截断）
const MAX_REASONING_BYTES: usize = 128 * 1024;

/// Cache entry with timestamp for TTL
#[derive(Clone, Debug)]
struct CacheEntry<T> {
    data: T,
    created_at: SystemTime,
    last_accessed: SystemTime,
}

/// Specialized entry for session-based signatures to track message count
#[derive(Clone, Debug)]
struct SessionSignatureEntry {
    signature: String,
    message_count: usize,
}

impl<T> CacheEntry<T> {
    fn new(data: T) -> Self {
        let now = SystemTime::now();
        Self {
            data,
            created_at: now,
            last_accessed: now,
        }
    }

    fn touch(&mut self) {
        self.last_accessed = SystemTime::now();
    }

    fn is_expired(&self) -> bool {
        self.created_at.elapsed().unwrap_or(Duration::ZERO) > SIGNATURE_TTL
    }
}

/// Evict the oldest entries by timestamp when cache exceeds limit,
/// even if none of the entries are expired yet.
fn evict_oldest_if_needed<V, F>(cache: &mut HashMap<String, V>, limit: usize, mut get_time: F)
where
    F: FnMut(&V) -> SystemTime,
{
    if cache.len() <= limit {
        return;
    }
    let excess = cache.len() - limit;
    let mut items: Vec<(String, SystemTime)> = cache
        .iter()
        .map(|(k, v)| (k.clone(), get_time(v)))
        .collect();
    items.sort_unstable_by_key(|(_, t)| *t);
    for (k, _) in items.into_iter().take(excess) {
        cache.remove(&k);
    }
}

// Payload capacity budgets across four layers total 32 MiB. Map metadata is separate.
const TOOL_BYTES: usize = 2 * 1024 * 1024;
const FAMILY_BYTES: usize = 2 * 1024 * 1024;
const SESSION_BYTES: usize = 24 * 1024 * 1024;
const REASONING_BYTES: usize = 4 * 1024 * 1024;

fn enforce_bytes<T>(
    cache: &mut HashMap<String, CacheEntry<T>>,
    budget: usize,
    size: impl Fn(&T) -> usize,
) {
    cache.retain(|_, entry| !entry.is_expired());
    let mut total: usize = cache
        .iter()
        .map(|(k, v)| k.capacity().saturating_add(size(&v.data)))
        .sum();
    if total <= budget {
        return;
    }
    let mut oldest: Vec<_> = cache
        .iter()
        .map(|(k, v)| (k.clone(), v.last_accessed))
        .collect();
    oldest.sort_unstable_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
    for (key, _) in oldest {
        if total <= budget {
            break;
        }
        if let Some((key, entry)) = cache.remove_entry(&key) {
            total = total.saturating_sub(key.capacity().saturating_add(size(&entry.data)));
        }
    }
}

/// Triple-layer signature cache to handle:
/// 1. Signature recovery for tool calls (when clients strip them)
/// 2. Cross-model compatibility checks (preventing Claude signatures on Gemini models)
/// 3. Session-based signature tracking (preventing cross-session pollution)
pub struct SignatureCache {
    /// Layer 1: Tool Use ID -> Thinking Signature
    /// Key: tool_use_id (e.g., "toolu_01...")
    /// Value: The thought signature that generated this tool call
    tool_signatures: Mutex<HashMap<String, CacheEntry<String>>>,

    /// Layer 2: Signature -> Model Family
    /// Key: thought signature string
    /// Value: Model family identifier (e.g., "claude-3-5-sonnet", "gemini-2.0-flash")
    thinking_families: Mutex<HashMap<String, CacheEntry<String>>>,

    /// Layer 3: Session ID -> Map of Message Count -> Thinking Signature (NEW)
    /// Key: session fingerprint (e.g., "sid-a1b2c3d4...")
    /// Value: A map of message count to thought signature
    /// This prevents signature pollution between different conversations and preserves history
    session_signatures: Mutex<HashMap<String, CacheEntry<HashMap<usize, SessionSignatureEntry>>>>,

    /// Layer 4: Session ID -> Assistant Reasoning Text History (NEW v4.2.0)
    /// Key: session fingerprint
    /// Value: A vector of reasoning contents (index corresponds to assistant turn index)
    session_reasonings: Mutex<HashMap<String, CacheEntry<Vec<String>>>>,
}

impl SignatureCache {
    fn new() -> Self {
        Self {
            tool_signatures: Mutex::new(HashMap::new()),
            thinking_families: Mutex::new(HashMap::new()),
            session_signatures: Mutex::new(HashMap::new()),
            session_reasonings: Mutex::new(HashMap::new()),
        }
    }

    /// Global singleton instance
    pub fn global() -> &'static SignatureCache {
        static INSTANCE: OnceLock<SignatureCache> = OnceLock::new();
        INSTANCE.get_or_init(SignatureCache::new)
    }

    /// Store a tool call signature
    pub fn cache_tool_signature(&self, tool_use_id: &str, signature: String) {
        if signature.len() < MIN_SIGNATURE_LENGTH {
            return;
        }

        if let Ok(mut cache) = self.tool_signatures.lock() {
            tracing::debug!(
                "[SignatureCache] Caching tool signature for id: {}",
                tool_use_id
            );
            if tool_use_id.len().saturating_add(signature.len()) > TOOL_BYTES {
                return;
            }
            cache.insert(
                tool_use_id.to_string(),
                CacheEntry::new(signature.as_str().to_owned()),
            );
            enforce_bytes(&mut cache, TOOL_BYTES, String::capacity);

            // Clean up expired entries and enforce capacity limit via LRU eviction
            if cache.len() > TOOL_CACHE_LIMIT {
                let before = cache.len();
                cache.retain(|_, v| !v.is_expired());
                evict_oldest_if_needed(&mut cache, TOOL_CACHE_LIMIT, |v| v.last_accessed);
                let after = cache.len();
                if before != after {
                    tracing::debug!(
                        "[SignatureCache] Tool cache cleanup: {} -> {} entries",
                        before,
                        after
                    );
                }
            }
        }
    }

    /// Retrieve a signature for a tool_use_id
    pub fn get_tool_signature(&self, tool_use_id: &str) -> Option<String> {
        if let Ok(mut cache) = self.tool_signatures.lock() {
            if let Some(entry) = cache.get_mut(tool_use_id) {
                if !entry.is_expired() {
                    entry.touch();
                    tracing::debug!(
                        "[SignatureCache] Hit tool signature for id: {}",
                        tool_use_id
                    );
                    return Some(entry.data.clone());
                }
            }
        }
        None
    }

    /// Store model family for a signature
    pub fn cache_thinking_family(&self, signature: String, family: String) {
        if signature.len() < MIN_SIGNATURE_LENGTH {
            return;
        }

        if let Ok(mut cache) = self.thinking_families.lock() {
            tracing::debug!(
                "[SignatureCache] Caching thinking family for sig (len={}): {}",
                signature.len(),
                family
            );
            if signature.len().saturating_add(family.len()) > FAMILY_BYTES {
                return;
            }
            cache.insert(
                signature.as_str().to_owned(),
                CacheEntry::new(family.as_str().to_owned()),
            );
            enforce_bytes(&mut cache, FAMILY_BYTES, String::capacity);

            if cache.len() > FAMILY_CACHE_LIMIT {
                let before = cache.len();
                cache.retain(|_, v| !v.is_expired());
                evict_oldest_if_needed(&mut cache, FAMILY_CACHE_LIMIT, |v| v.last_accessed);
                let after = cache.len();
                if before != after {
                    tracing::debug!(
                        "[SignatureCache] Family cache cleanup: {} -> {} entries",
                        before,
                        after
                    );
                }
            }
        }
    }

    /// Alias for cache_thinking_family
    #[inline]
    pub fn set_thinking_family(&self, signature: String, family: String) {
        self.cache_thinking_family(signature, family);
    }

    /// Get model family for a signature
    pub fn get_signature_family(&self, signature: &str) -> Option<String> {
        if let Ok(mut cache) = self.thinking_families.lock() {
            if let Some(entry) = cache.get_mut(signature) {
                if !entry.is_expired() {
                    entry.touch();
                    return Some(entry.data.clone());
                } else {
                    tracing::debug!("[SignatureCache] Signature family entry expired");
                }
            }
        }
        None
    }

    // ===== Layer 3: Session-based Signature Storage =====

    /// Store the thinking signature for a session at a specific message count.
    /// This is the preferred method for tracking signatures across tool loops.
    ///
    /// # Arguments
    /// * `session_id` - Session fingerprint (e.g., "sid-a1b2c3d4...")
    /// * `signature` - The thought signature to store
    /// * `message_count` - The current message count of the conversation (to detect Rewind)
    pub fn cache_session_signature(
        &self,
        session_id: &str,
        signature: String,
        message_count: usize,
    ) {
        if signature.len() < MIN_SIGNATURE_LENGTH {
            return;
        }

        // 单条签名过大防护：超过会话总预算则拒绝（不截断）
        if signature.len() > SESSION_SIGNATURE_HISTORY_BYTES
            || session_id.len().saturating_add(signature.len()) > SESSION_BYTES
        {
            tracing::warn!(
                "[SignatureCache] Signature for session {} exceeds budget ({} bytes), rejecting",
                session_id,
                signature.len()
            );
            return;
        }

        if let Ok(mut cache) = self.session_signatures.lock() {
            let entry = cache
                .entry(session_id.to_string())
                .or_insert_with(|| CacheEntry::new(HashMap::new()));

            // P1-2: 写入过期会话时重建空历史
            if entry.is_expired() {
                let now = SystemTime::now();
                entry.created_at = now;
                entry.last_accessed = now;
                entry.data.clear();
            }

            // Detect if a rewind happened (e.g. if we have cached signatures with message_count
            // greater than the current message_count, those should be cleared since that future is gone).
            entry.data.retain(|&mc, _| {
                if mc > message_count {
                    tracing::info!(
                        "[SignatureCache] Rewind detected for {} at count {}: removing future signature at count {}.",
                        session_id,
                        message_count,
                        mc
                    );
                    false
                } else {
                    true
                }
            });

            let should_store = match entry.data.get(&message_count) {
                None => true,
                Some(existing) => {
                    // Same message count: only update if the new signature is longer (more complete)
                    signature.len() > existing.signature.len()
                }
            };

            if should_store {
                // 有效写入刷新 TTL 和 LRU（spec 目标 2）
                let now = SystemTime::now();
                entry.created_at = now;
                entry.last_accessed = now;

                // 紧凑存储：消除输入 String 的多余 capacity
                let compact_sig = signature.as_str().to_owned();

                tracing::debug!(
                    "[SignatureCache] Session {} (msg_count={}) -> storing signature (len={})",
                    session_id,
                    message_count,
                    compact_sig.len()
                );
                entry.data.insert(
                    message_count,
                    SessionSignatureEntry {
                        signature: compact_sig,
                        message_count,
                    },
                );

                // 双预算淘汰（spec G4）：轮次上限 + 字节上限，优先淘汰最早 message_count
                loop {
                    let turn_ok = entry.data.len() <= SESSION_SIGNATURE_HISTORY_LIMIT;
                    let bytes_total: usize =
                        entry.data.values().map(|e| e.signature.capacity()).sum();
                    let bytes_ok = bytes_total <= SESSION_SIGNATURE_HISTORY_BYTES;
                    if turn_ok && bytes_ok {
                        break;
                    }
                    if let Some(&oldest_mc) = entry.data.keys().min() {
                        entry.data.remove(&oldest_mc);
                    } else {
                        break;
                    }
                }
            }

            enforce_bytes(&mut cache, SESSION_BYTES, |history| {
                history.values().map(|e| e.signature.capacity()).sum()
            });
            // Cleanup when limit is reached (Session cache has largest limit)
            if cache.len() > SESSION_CACHE_LIMIT {
                let before = cache.len();
                cache.retain(|_, v| !v.is_expired());
                evict_oldest_if_needed(&mut cache, SESSION_CACHE_LIMIT, |v| v.last_accessed);
                let after = cache.len();
                if before != after {
                    tracing::info!(
                        "[SignatureCache] Session cache cleanup: {} -> {} entries (limit: {})",
                        before,
                        after,
                        SESSION_CACHE_LIMIT
                    );
                }
            }
        }
    }

    /// Retrieve the latest thinking signature for a session.
    /// Returns None if not found or expired.
    pub fn get_session_signature(&self, session_id: &str) -> Option<String> {
        if let Ok(mut cache) = self.session_signatures.lock() {
            if let Some(entry) = cache.get_mut(session_id) {
                if !entry.is_expired() {
                    let found = entry
                        .data
                        .values()
                        .max_by_key(|e| e.message_count)
                        .map(|e| (e.message_count, e.signature.clone()));
                    if let Some((msg_count, sig)) = found {
                        entry.touch();
                        tracing::debug!(
                            "[SignatureCache] Session {} (latest, msg_count={}) -> HIT (len={})",
                            session_id,
                            msg_count,
                            sig.len()
                        );
                        return Some(sig);
                    }
                } else {
                    tracing::debug!("[SignatureCache] Session {} -> EXPIRED", session_id);
                }
            }
        }
        None
    }

    /// Retrieve the thinking signature for a session at a specific message count.
    /// Returns None if not found or expired.
    pub fn get_session_signature_at(
        &self,
        session_id: &str,
        message_count: usize,
    ) -> Option<String> {
        if let Ok(mut cache) = self.session_signatures.lock() {
            if let Some(entry) = cache.get_mut(session_id) {
                if !entry.is_expired() {
                    let sig = entry.data.get(&message_count).map(|e| e.signature.clone());
                    if let Some(sig_str) = sig {
                        entry.touch();
                        tracing::debug!(
                            "[SignatureCache] Session {} (msg_count={}) -> HIT (len={})",
                            session_id,
                            message_count,
                            sig_str.len()
                        );
                        return Some(sig_str);
                    }
                }
            }
        }
        None
    }

    /// Store reasoning text for a specific assistant turn in a session
    pub fn cache_session_reasoning(&self, session_id: &str, reasoning: String, turn_index: usize) {
        if reasoning.trim().is_empty() {
            return;
        }

        // Prevent excessively large vector allocations from abnormal/malicious turn_index
        if turn_index > 500 {
            tracing::warn!(
                "[SignatureCache] turn_index {} exceeds limit (500) for session {}, ignoring reasoning",
                turn_index,
                session_id
            );
            return;
        }

        // P1 目标 1: 超预算 reasoning 前置校验！在任何缓存插入或分配之前拒绝！
        if reasoning.len() > MAX_REASONING_BYTES
            || session_id.len().saturating_add(reasoning.len()) > REASONING_BYTES
        {
            tracing::warn!(
                "[SignatureCache] Reasoning for session {} turn {} exceeds max bytes ({} > {}), rejecting",
                session_id,
                turn_index,
                reasoning.len(),
                MAX_REASONING_BYTES
            );
            return;
        }

        if let Ok(mut cache) = self.session_reasonings.lock() {
            let entry = cache
                .entry(session_id.to_string())
                .or_insert_with(|| CacheEntry::new(Vec::new()));

            // P1-2: 写入过期会话时重建空历史，刷新 TTL
            if entry.is_expired() {
                let now = SystemTime::now();
                entry.created_at = now;
                entry.last_accessed = now;
                entry.data.clear();
            }

            if turn_index >= entry.data.len() {
                entry.data.resize(turn_index + 1, String::new());
            }

            // Only update if the new reasoning is longer to prevent overwriting with partial content
            let old_len = entry.data[turn_index].len();
            if reasoning.len() > old_len {
                // 有效写入刷新 TTL 和 LRU（spec 目标 2）
                let now = SystemTime::now();
                entry.created_at = now;
                entry.last_accessed = now;

                // 紧凑存储：消除输入 String 的多余 capacity
                let compact_reasoning = reasoning.as_str().to_owned();
                tracing::debug!(
                    "[SignatureCache] Session {} (turn={}) -> caching reasoning text (len: {} -> {})",
                    session_id,
                    turn_index,
                    old_len,
                    compact_reasoning.len()
                );
                entry.data[turn_index] = compact_reasoning;
            }

            enforce_bytes(&mut cache, REASONING_BYTES, |history| {
                history.iter().map(String::capacity).sum()
            });
            // Session cache cleanup if limit exceeded
            if cache.len() > SESSION_CACHE_LIMIT {
                let before = cache.len();
                cache.retain(|_, v| !v.is_expired());
                evict_oldest_if_needed(&mut cache, SESSION_CACHE_LIMIT, |v| v.last_accessed);
                let after = cache.len();
                if before != after {
                    tracing::debug!(
                        "[SignatureCache] Session reasoning cache cleanup: {} -> {} entries",
                        before,
                        after
                    );
                }
            }
        }
    }

    /// Alias for cache_session_reasoning
    #[inline]
    pub fn append_reasoning(&self, session_id: &str, reasoning: String, turn_index: usize) {
        self.cache_session_reasoning(session_id, reasoning, turn_index);
    }

    /// Retrieve reasoning text for a specific assistant turn in a session
    pub fn get_session_reasoning(&self, session_id: &str, turn_index: usize) -> Option<String> {
        if let Ok(mut cache) = self.session_reasonings.lock() {
            if let Some(entry) = cache.get_mut(session_id) {
                if !entry.is_expired() && turn_index < entry.data.len() {
                    let text = if !entry.data[turn_index].trim().is_empty() {
                        Some(entry.data[turn_index].clone())
                    } else {
                        None
                    };
                    if let Some(t) = text {
                        entry.touch();
                        tracing::debug!(
                            "[SignatureCache] Session {} (turn={}) -> Hit reasoning text cache (len: {})",
                            session_id,
                            turn_index,
                            t.len()
                        );
                        return Some(t);
                    }
                }
            }
        }
        None
    }

    /// 删除指定会话的缓存签名
    #[allow(dead_code)] // 预留给管理接口或调试使用
    pub fn delete_session_signature(&self, session_id: &str) {
        if let Ok(mut cache) = self.session_signatures.lock() {
            if cache.remove(session_id).is_some() {
                tracing::debug!(
                    "[SignatureCache] Deleted session signature for: {}",
                    session_id
                );
            }
        }
    }

    /// Called by the cancellable proxy maintenance loop, including during idle periods.
    pub fn evict_expired(&self) {
        self.tool_signatures
            .lock()
            .unwrap()
            .retain(|_, e| !e.is_expired());
        self.thinking_families
            .lock()
            .unwrap()
            .retain(|_, e| !e.is_expired());
        self.session_signatures
            .lock()
            .unwrap()
            .retain(|_, e| !e.is_expired());
        self.session_reasonings
            .lock()
            .unwrap()
            .retain(|_, e| !e.is_expired());
    }

    /// Clear all caches (for testing or manual reset)
    #[allow(dead_code)] // Used in tests
    pub fn clear(&self) {
        if let Ok(mut cache) = self.tool_signatures.lock() {
            cache.clear();
        }
        if let Ok(mut cache) = self.thinking_families.lock() {
            cache.clear();
        }
        if let Ok(mut cache) = self.session_signatures.lock() {
            cache.clear();
        }
        if let Ok(mut cache) = self.session_reasonings.lock() {
            cache.clear();
        }
    }

    #[cfg(test)]
    pub fn tool_cache_len(&self) -> usize {
        self.tool_signatures.lock().unwrap().len()
    }

    #[cfg(test)]
    pub fn family_cache_len(&self) -> usize {
        self.thinking_families.lock().unwrap().len()
    }

    #[cfg(test)]
    pub fn session_cache_len(&self) -> usize {
        self.session_signatures.lock().unwrap().len()
    }

    #[cfg(test)]
    pub fn reasoning_cache_len(&self) -> usize {
        self.session_reasonings.lock().unwrap().len()
    }

    #[cfg(test)]
    pub fn inject_session_signature_created_at(&self, session_id: &str, past_time: SystemTime) {
        if let Ok(mut cache) = self.session_signatures.lock() {
            if let Some(entry) = cache.get_mut(session_id) {
                entry.created_at = past_time;
            }
        }
    }

    #[cfg(test)]
    pub fn inject_session_reasoning_created_at(&self, session_id: &str, past_time: SystemTime) {
        if let Ok(mut cache) = self.session_reasonings.lock() {
            if let Some(entry) = cache.get_mut(session_id) {
                entry.created_at = past_time;
            }
        }
    }

    #[cfg(test)]
    pub fn get_session_signature_internal_capacity(&self, session_id: &str) -> usize {
        if let Ok(cache) = self.session_signatures.lock() {
            if let Some(entry) = cache.get(session_id) {
                return entry.data.values().map(|e| e.signature.capacity()).sum();
            }
        }
        0
    }

    #[cfg(test)]
    pub fn get_session_signature_last_accessed(&self, session_id: &str) -> Option<SystemTime> {
        let cache = self.session_signatures.lock().ok()?;
        cache.get(session_id).map(|e| e.last_accessed)
    }

    #[cfg(test)]
    pub fn get_session_signature_created_at(&self, session_id: &str) -> Option<SystemTime> {
        let cache = self.session_signatures.lock().ok()?;
        cache.get(session_id).map(|e| e.created_at)
    }

    #[cfg(test)]
    pub fn get_session_reasoning_created_at(&self, session_id: &str) -> Option<SystemTime> {
        let cache = self.session_reasonings.lock().ok()?;
        cache.get(session_id).map(|e| e.created_at)
    }

    #[cfg(test)]
    pub fn get_session_reasoning_history_len(&self, session_id: &str) -> Option<usize> {
        let cache = self.session_reasonings.lock().ok()?;
        cache.get(session_id).map(|e| e.data.len())
    }

    #[cfg(test)]
    pub fn get_session_reasoning_internal_capacity(&self, session_id: &str) -> usize {
        if let Ok(cache) = self.session_reasonings.lock() {
            if let Some(entry) = cache.get(session_id) {
                return entry.data.iter().map(|s| s.capacity()).sum();
            }
        }
        0
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn memory_budget_idle_cleanup_releases_all_signature_layers() {
        let cache = super::SignatureCache::new();
        cache.cache_tool_signature("tool", "s".repeat(60));
        cache.cache_thinking_family("s".repeat(60), "family".into());
        cache.cache_session_signature("session", "s".repeat(60), 1);
        cache.cache_session_reasoning("session", "reasoning".into(), 0);
        let expired =
            std::time::SystemTime::now() - super::SIGNATURE_TTL - std::time::Duration::from_secs(1);
        for e in cache.tool_signatures.lock().unwrap().values_mut() {
            e.created_at = expired;
        }
        for e in cache.thinking_families.lock().unwrap().values_mut() {
            e.created_at = expired;
        }
        for e in cache.session_signatures.lock().unwrap().values_mut() {
            e.created_at = expired;
        }
        for e in cache.session_reasonings.lock().unwrap().values_mut() {
            e.created_at = expired;
        }
        cache.evict_expired();
        assert!(cache.tool_signatures.lock().unwrap().is_empty());
        assert!(cache.thinking_families.lock().unwrap().is_empty());
        assert!(cache.session_signatures.lock().unwrap().is_empty());
        assert!(cache.session_reasonings.lock().unwrap().is_empty());
    }

    #[test]
    fn memory_budget_session_cache_is_globally_bounded() {
        let cache = super::SignatureCache::new();
        for i in 0..40 {
            cache.cache_session_signature(&format!("budget-{i}"), "s".repeat(1024 * 1024), 1);
        }
        let entries = cache.session_signatures.lock().unwrap();
        let bytes: usize = entries
            .values()
            .flat_map(|e| e.data.values())
            .map(|e| e.signature.capacity())
            .sum();
        assert!(bytes <= 24 * 1024 * 1024, "retained {bytes} bytes");
        assert!(entries.contains_key("budget-39"));
    }

    #[test]
    fn memory_budget_rejects_oversized_tool_signature() {
        let cache = super::SignatureCache::new();
        cache.cache_tool_signature("large", "s".repeat(3 * 1024 * 1024));
        assert!(cache.get_tool_signature("large").is_none());
    }

    use super::*;

    #[test]
    fn test_tool_signature_cache() {
        let cache = SignatureCache::new();
        let sig = "x".repeat(60); // Valid length

        cache.cache_tool_signature("tool_1", sig.clone());
        assert_eq!(cache.get_tool_signature("tool_1"), Some(sig));
        assert_eq!(cache.get_tool_signature("tool_2"), None);
    }

    #[test]
    fn test_min_length() {
        let cache = SignatureCache::new();
        cache.cache_tool_signature("tool_short", "short".to_string());
        assert_eq!(cache.get_tool_signature("tool_short"), None);
    }

    #[test]
    fn test_thinking_family() {
        let cache = SignatureCache::new();
        let sig = "y".repeat(60);

        cache.cache_thinking_family(sig.clone(), "claude".to_string());
        assert_eq!(cache.get_signature_family(&sig), Some("claude".to_string()));
    }

    #[test]
    fn test_session_signature() {
        let cache = SignatureCache::new();
        let sig1 = "a".repeat(60);
        let sig2 = "b".repeat(80); // Longer, should replace
        let sig3 = "c".repeat(40); // Too short, should be ignored

        // Initially empty
        assert!(cache.get_session_signature("sid-test123").is_none());

        // Store first signature
        cache.cache_session_signature("sid-test123", sig1.clone(), 5);
        assert_eq!(
            cache.get_session_signature("sid-test123"),
            Some(sig1.clone())
        );

        // Longer signature should replace (same msg count)
        cache.cache_session_signature("sid-test123", sig2.clone(), 5);
        assert_eq!(
            cache.get_session_signature("sid-test123"),
            Some(sig2.clone())
        );

        // Shorter valid signature should NOT replace (same msg count)
        cache.cache_session_signature("sid-test123", sig1.clone(), 5);
        assert_eq!(
            cache.get_session_signature("sid-test123"),
            Some(sig2.clone())
        );

        // Rewind: Shorter signature MUST replace if message count is lower
        cache.cache_session_signature("sid-test123", sig1.clone(), 3);
        assert_eq!(
            cache.get_session_signature("sid-test123"),
            Some(sig1.clone())
        );

        // Too short signature should be ignored entirely (even if rewind)
        cache.cache_session_signature("sid-test123", sig3, 1);
        assert_eq!(cache.get_session_signature("sid-test123"), Some(sig1));

        // Different session should be isolated
        assert!(cache.get_session_signature("sid-other").is_none());
    }

    #[test]
    fn test_clear_all_caches() {
        let cache = SignatureCache::new();
        let sig = "x".repeat(60);

        cache.cache_tool_signature("tool_1", sig.clone());
        cache.cache_thinking_family(sig.clone(), "model".to_string());
        cache.cache_session_signature("sid-1", sig.clone(), 1);

        assert!(cache.get_tool_signature("tool_1").is_some());
        assert!(cache.get_signature_family(&sig).is_some());
        assert!(cache.get_session_signature("sid-1").is_some());

        cache.clear();

        assert!(cache.get_tool_signature("tool_1").is_none());
        assert!(cache.get_signature_family(&sig).is_none());
        assert!(cache.get_session_signature("sid-1").is_none());
    }

    #[test]
    fn test_tool_cache_unexpired_lru_eviction() {
        let cache = SignatureCache::new();
        let sig = "x".repeat(60);

        // Insert first item and sleep briefly to guarantee strictly older timestamp
        cache.cache_tool_signature("tool_oldest", sig.clone());
        std::thread::sleep(std::time::Duration::from_millis(5));

        for i in 0..TOOL_CACHE_LIMIT {
            cache.cache_tool_signature(&format!("tool_{}", i), sig.clone());
        }

        // Total inserted is TOOL_CACHE_LIMIT + 1, all unexpired
        // Cache size must strictly not exceed TOOL_CACHE_LIMIT
        assert_eq!(cache.tool_cache_len(), TOOL_CACHE_LIMIT);

        // Oldest item must have been evicted
        assert_eq!(cache.get_tool_signature("tool_oldest"), None);

        // Most recently inserted item must still exist
        assert_eq!(
            cache.get_tool_signature(&format!("tool_{}", TOOL_CACHE_LIMIT - 1)),
            Some(sig)
        );
    }

    #[test]
    fn test_family_cache_unexpired_lru_eviction() {
        let cache = SignatureCache::new();
        let oldest_sig = format!("{}_oldest", "y".repeat(50));
        cache.set_thinking_family(oldest_sig.clone(), "family_oldest".to_string());
        std::thread::sleep(std::time::Duration::from_millis(5));

        for i in 0..FAMILY_CACHE_LIMIT {
            let sig = format!("{}_{:04}", "y".repeat(50), i);
            cache.set_thinking_family(sig, format!("family_{}", i));
        }

        assert_eq!(cache.family_cache_len(), FAMILY_CACHE_LIMIT);
        assert_eq!(cache.get_signature_family(&oldest_sig), None);
    }

    #[test]
    fn test_session_cache_unexpired_lru_eviction() {
        let cache = SignatureCache::new();
        let sig = "z".repeat(60);

        cache.cache_session_signature("session_oldest", sig.clone(), 1);
        std::thread::sleep(std::time::Duration::from_millis(5));

        for i in 0..SESSION_CACHE_LIMIT {
            cache.cache_session_signature(&format!("session_{}", i), sig.clone(), 1);
        }

        assert_eq!(cache.session_cache_len(), SESSION_CACHE_LIMIT);
        assert_eq!(cache.get_session_signature("session_oldest"), None);
        assert!(cache
            .get_session_signature(&format!("session_{}", SESSION_CACHE_LIMIT - 1))
            .is_some());
    }

    #[test]
    fn test_append_reasoning_turn_index_protection() {
        let cache = SignatureCache::new();

        // Normal turn_index <= 500 should succeed
        cache.append_reasoning("sess_1", "valid reasoning".to_string(), 500);
        assert_eq!(
            cache.get_session_reasoning("sess_1", 500),
            Some("valid reasoning".to_string())
        );

        // turn_index > 500 should be ignored (no huge resize or panic)
        cache.append_reasoning("sess_malicious", "attack".to_string(), 501);
        assert_eq!(cache.get_session_reasoning("sess_malicious", 501), None);
        assert_eq!(cache.reasoning_cache_len(), 1);
    }

    #[test]
    fn test_reasoning_cache_unexpired_lru_eviction() {
        let cache = SignatureCache::new();

        cache.append_reasoning("session_oldest", "reasoning_0".to_string(), 0);
        std::thread::sleep(std::time::Duration::from_millis(5));

        for i in 0..SESSION_CACHE_LIMIT {
            cache.append_reasoning(&format!("session_{}", i), format!("reasoning_{}", i), 0);
        }

        assert_eq!(cache.reasoning_cache_len(), SESSION_CACHE_LIMIT);
        assert_eq!(cache.get_session_reasoning("session_oldest", 0), None);
        assert!(cache
            .get_session_reasoning(&format!("session_{}", SESSION_CACHE_LIMIT - 1), 0)
            .is_some());
    }

    #[test]
    fn test_tool_signature_true_lru_access_promotes_entry() {
        let cache = SignatureCache::new();
        let sig = "x".repeat(50);

        // 插入 tool_a，随后插入 tool_b
        cache.cache_tool_signature("tool_a", sig.clone());
        std::thread::sleep(std::time::Duration::from_millis(10));
        cache.cache_tool_signature("tool_b", sig.clone());
        std::thread::sleep(std::time::Duration::from_millis(10));

        // 读取命中 tool_a，刷新其 last_accessed
        assert_eq!(cache.get_tool_signature("tool_a"), Some(sig.clone()));
        std::thread::sleep(std::time::Duration::from_millis(10));

        // 插入填满并触发淘汰（插入 TOOL_CACHE_LIMIT - 1 个新项，总数为 TOOL_CACHE_LIMIT + 1，超限 1 项）
        for i in 0..(TOOL_CACHE_LIMIT - 1) {
            cache.cache_tool_signature(&format!("tool_fill_{}", i), sig.clone());
        }

        assert_eq!(cache.tool_cache_len(), TOOL_CACHE_LIMIT);
        // tool_a 后来被读取过，比未被读取的 tool_b 更晚访问，因此 tool_b 必然比 tool_a 先淘汰
        assert_eq!(cache.get_tool_signature("tool_b"), None);
        assert_eq!(cache.get_tool_signature("tool_a"), Some(sig));
    }

    #[test]
    fn test_session_signature_single_session_turns_and_bytes_budget() {
        let cache = SignatureCache::new();

        // 1. 超大签名（> 1 MiB）应被安全拒绝
        let huge_sig = "a".repeat(SESSION_SIGNATURE_HISTORY_BYTES + 10);
        cache.cache_session_signature("sess_budget", huge_sig, 1);
        assert_eq!(cache.get_session_signature("sess_budget"), None);

        // 2. 正常签名多轮插入，总轮次超过 SESSION_SIGNATURE_HISTORY_LIMIT 时应限制内部条目数
        for turn in 1..=(SESSION_SIGNATURE_HISTORY_LIMIT + 10) {
            let valid_sig = format!("sig_turn_{:04}_{}", turn, "x".repeat(50));
            cache.cache_session_signature("sess_budget", valid_sig, turn);
        }

        // 验证内部映射大小不超过 SESSION_SIGNATURE_HISTORY_LIMIT
        let outer = cache.session_signatures.lock().unwrap();
        let session_entry = outer.get("sess_budget").expect("session should exist");
        assert!(session_entry.data.len() <= SESSION_SIGNATURE_HISTORY_LIMIT);
        // 最旧的第 1 轮应该已被移除
        assert!(!session_entry.data.contains_key(&1));
        // 最新的最后一轮应该存在
        assert!(session_entry
            .data
            .contains_key(&(SESSION_SIGNATURE_HISTORY_LIMIT + 10)));
    }

    #[test]
    fn test_reasoning_rejects_oversized_without_truncation() {
        let cache = SignatureCache::new();
        let huge_reasoning = "r".repeat(MAX_REASONING_BYTES + 100);
        cache.append_reasoning("sess_trunc", huge_reasoning, 0);

        // 超预算 reasoning 不缓存（不截断），读取返回 None
        assert_eq!(cache.get_session_reasoning("sess_trunc", 0), None);
    }

    #[test]
    fn test_expired_session_write_rebuilds_and_becomes_readable() {
        let cache = SignatureCache::new();
        let sig1 = "x".repeat(60);
        let sig2 = "y".repeat(60);

        // 1. 写入第 1 轮签名
        cache.cache_session_signature("sess_exp", sig1, 1);
        assert!(cache.get_session_signature("sess_exp").is_some());

        // 2. 注入时间：设为 3 小时前（超过 2 小时 TTL）
        let past = SystemTime::now() - Duration::from_secs(3 * 3600);
        cache.inject_session_signature_created_at("sess_exp", past);

        // 3. 再次写入第 2 轮新签名
        cache.cache_session_signature("sess_exp", sig2.clone(), 2);

        // 4. 断言：新写入的第 2 轮签名必须立即可读！且旧轮次 1 不得复活
        let read = cache.get_session_signature("sess_exp");
        assert_eq!(read, Some(sig2));
        assert_eq!(cache.get_session_signature_at("sess_exp", 1), None);
    }

    #[test]
    fn test_signature_compact_storage_eliminates_excess_capacity() {
        let cache = SignatureCache::new();

        // 构造一个长度仅 60 字节，但拥有 10MB capacity 的畸形 String
        let mut huge_capacity_sig = String::with_capacity(10 * 1024 * 1024);
        huge_capacity_sig.push_str(&"s".repeat(60));
        assert!(huge_capacity_sig.capacity() >= 10 * 1024 * 1024);

        cache.cache_session_signature("sess_cap", huge_capacity_sig, 1);

        // 断言：内部存储的 capacity 必须是紧凑的，不得保留 10MB capacity
        let internal_cap = cache.get_session_signature_internal_capacity("sess_cap");
        assert!(
            internal_cap < 1024,
            "Internal capacity must be compact, but was {}",
            internal_cap
        );
    }

    #[test]
    fn test_reasoning_internal_capacity_is_bounded() {
        let cache = SignatureCache::new();

        let mut huge_reasoning = String::with_capacity(10 * 1024 * 1024);
        huge_reasoning.push_str(&"r".repeat(1000));
        cache.append_reasoning("sess_reas_cap", huge_reasoning, 0);

        let internal_cap = cache.get_session_reasoning_internal_capacity("sess_reas_cap");
        assert!(
            internal_cap < 4096,
            "Internal reasoning capacity must be compact, but was {}",
            internal_cap
        );
    }

    #[test]
    fn test_session_signature_turn_miss_does_not_refresh_touch() {
        let cache = SignatureCache::new();
        cache.cache_session_signature("sess_miss", "x".repeat(60), 1);
        let t0 = cache
            .get_session_signature_last_accessed("sess_miss")
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert_eq!(cache.get_session_signature_at("sess_miss", 999), None);
        let t1 = cache
            .get_session_signature_last_accessed("sess_miss")
            .unwrap();
        assert_eq!(t0, t1, "Turn miss must NOT refresh last_accessed");
        assert!(cache.get_session_signature_at("sess_miss", 1).is_some());
        let t2 = cache
            .get_session_signature_last_accessed("sess_miss")
            .unwrap();
        assert!(t2 > t1, "Turn hit MUST refresh last_accessed");
    }

    #[test]
    fn test_oversized_reasoning_does_not_create_session_or_allocate() {
        let cache = SignatureCache::new();
        let huge_reasoning = "r".repeat(MAX_REASONING_BYTES + 100);

        for i in 0..2000 {
            cache.append_reasoning(&format!("sess_overflow_{}", i), huge_reasoning.clone(), 500);
        }

        assert_eq!(
            cache.reasoning_cache_len(),
            0,
            "Rejected oversized reasoning must NOT create sessions or bypass capacity limit"
        );
    }

    #[test]
    fn test_valid_write_refreshes_ttl_near_expiry_and_read_does_not() {
        let cache = SignatureCache::new();
        let sig = "x".repeat(60);

        cache.cache_session_signature("sess_ttl", sig.clone(), 1);
        let old_time = SystemTime::now() - Duration::from_secs(7190);
        cache.inject_session_signature_created_at("sess_ttl", old_time);

        // 有效写入刷新 TTL（created_at 更新为当前时间）
        let sig2 = "y".repeat(70);
        cache.cache_session_signature("sess_ttl", sig2.clone(), 2);
        let new_created = cache.get_session_signature_created_at("sess_ttl").unwrap();
        assert!(
            new_created.elapsed().unwrap().as_secs() < 5,
            "Valid write MUST refresh session TTL created_at"
        );
    }

    #[test]
    fn test_oversized_reasoning_does_not_modify_existing_session() {
        let cache = SignatureCache::new();
        cache.append_reasoning("sess_exist", "valid reasoning 0".to_string(), 0);
        let orig_created = cache
            .get_session_reasoning_created_at("sess_exist")
            .unwrap();
        let orig_len = cache
            .get_session_reasoning_history_len("sess_exist")
            .unwrap();

        std::thread::sleep(std::time::Duration::from_millis(10));
        let huge = "r".repeat(MAX_REASONING_BYTES + 100);
        cache.append_reasoning("sess_exist", huge, 1);

        assert_eq!(
            cache.get_session_reasoning("sess_exist", 0),
            Some("valid reasoning 0".to_string())
        );
        assert_eq!(cache.get_session_reasoning("sess_exist", 1), None);
        assert_eq!(
            cache
                .get_session_reasoning_history_len("sess_exist")
                .unwrap(),
            orig_len
        );
        assert_eq!(
            cache
                .get_session_reasoning_created_at("sess_exist")
                .unwrap(),
            orig_created
        );
    }

    #[test]
    fn test_reasoning_valid_write_refreshes_ttl_and_read_does_not() {
        let cache = SignatureCache::new();
        cache.append_reasoning("sess_r_ttl", "reasoning 0".to_string(), 0);
        let old_time = SystemTime::now() - Duration::from_secs(7190);
        cache.inject_session_reasoning_created_at("sess_r_ttl", old_time);

        // 有效写入刷新 TTL
        cache.append_reasoning("sess_r_ttl", "reasoning 1 longer".to_string(), 1);
        let new_created = cache
            .get_session_reasoning_created_at("sess_r_ttl")
            .unwrap();
        assert!(
            new_created.elapsed().unwrap().as_secs() < 5,
            "Reasoning valid write MUST refresh TTL"
        );

        // 读取命中不延长 TTL
        cache.inject_session_reasoning_created_at("sess_r_ttl", old_time);
        assert!(cache.get_session_reasoning("sess_r_ttl", 1).is_some());
        let read_created = cache
            .get_session_reasoning_created_at("sess_r_ttl")
            .unwrap();
        assert_eq!(read_created, old_time, "Read must NOT extend TTL");
    }

    #[test]
    fn test_rejected_write_does_not_refresh_ttl() {
        let cache = SignatureCache::new();
        let sig = "x".repeat(100);
        cache.cache_session_signature("sess_rej", sig.clone(), 1);
        let old_time = SystemTime::now() - Duration::from_secs(5000);
        cache.inject_session_signature_created_at("sess_rej", old_time);

        // 同轮短签名被拒绝，不得刷新 TTL
        let shorter = "y".repeat(60);
        cache.cache_session_signature("sess_rej", shorter, 1);
        let after_time = cache.get_session_signature_created_at("sess_rej").unwrap();
        assert_eq!(
            after_time, old_time,
            "Rejected same-turn shorter write must NOT refresh TTL"
        );

        // 单条超 1 MiB 签名被拒绝，不得刷新 TTL
        let oversized = "z".repeat(SESSION_SIGNATURE_HISTORY_BYTES + 10);
        cache.cache_session_signature("sess_rej", oversized, 2);
        let after_oversized = cache.get_session_signature_created_at("sess_rej").unwrap();
        assert_eq!(
            after_oversized, old_time,
            "Rejected oversized signature must NOT refresh TTL"
        );
    }

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

    #[test]
    fn test_session_signature_10000_turns_dual_budget() {
        let cache = SignatureCache::new();
        // 写入 10,000 轮签名
        for turn in 1..=10000 {
            let sig = format!("sig_turn_{:05}_{}", turn, "s".repeat(80));
            cache.cache_session_signature("sess_10k", sig, turn);
        }

        // 验证硬上限 256 轮
        let count = cache
            .session_signatures
            .lock()
            .unwrap()
            .get("sess_10k")
            .unwrap()
            .data
            .len();
        assert_eq!(count, SESSION_SIGNATURE_HISTORY_LIMIT);
        assert!(count <= 256);

        // 验证 1 MiB 字节预算
        let cap = cache.get_session_signature_internal_capacity("sess_10k");
        assert!(cap <= SESSION_SIGNATURE_HISTORY_BYTES);
        assert!(cap < 100 * 1024); // 256 * ~100 bytes = ~25KB, strictly < 1MiB
    }

    #[test]
    fn test_family_cache_true_lru_access_promotes_entry() {
        let cache = SignatureCache::new();
        let sig_a = format!("sig_a_{}", "x".repeat(60));
        let sig_b = format!("sig_b_{}", "x".repeat(60));
        cache.set_thinking_family(sig_a.clone(), "family_a".to_string());
        std::thread::sleep(std::time::Duration::from_millis(10));
        cache.set_thinking_family(sig_b.clone(), "family_b".to_string());
        std::thread::sleep(std::time::Duration::from_millis(10));

        // 访问 sig_a 提升其为活跃项
        assert_eq!(
            cache.get_signature_family(&sig_a),
            Some("family_a".to_string())
        );
        std::thread::sleep(std::time::Duration::from_millis(10));

        // 填满超额 1 项
        for i in 0..(FAMILY_CACHE_LIMIT - 1) {
            cache.set_thinking_family(
                format!("sig_fill_{}_{}", i, "x".repeat(60)),
                format!("fam_{}", i),
            );
        }

        assert_eq!(cache.family_cache_len(), FAMILY_CACHE_LIMIT);
        // sig_b 未被访问，早于 sig_a 淘汰
        assert_eq!(cache.get_signature_family(&sig_b), None);
        assert_eq!(
            cache.get_signature_family(&sig_a),
            Some("family_a".to_string())
        );
    }

    #[test]
    fn test_session_signature_true_lru_access_promotes_entry() {
        let cache = SignatureCache::new();
        let sig = "x".repeat(60);
        cache.cache_session_signature("sess_lru_a", sig.clone(), 1);
        std::thread::sleep(std::time::Duration::from_millis(10));
        cache.cache_session_signature("sess_lru_b", sig.clone(), 1);
        std::thread::sleep(std::time::Duration::from_millis(10));

        // 访问 sess_lru_a 提升活跃度
        assert!(cache.get_session_signature("sess_lru_a").is_some());
        std::thread::sleep(std::time::Duration::from_millis(10));

        // 填满超额 1 项
        for i in 0..(SESSION_CACHE_LIMIT - 1) {
            cache.cache_session_signature(&format!("sess_fill_{}", i), sig.clone(), 1);
        }

        assert_eq!(cache.session_cache_len(), SESSION_CACHE_LIMIT);
        assert_eq!(cache.get_session_signature("sess_lru_b"), None);
        assert!(cache.get_session_signature("sess_lru_a").is_some());
    }

    #[test]
    fn test_session_reasoning_true_lru_access_promotes_entry() {
        let cache = SignatureCache::new();
        cache.append_reasoning("reas_lru_a", "reasoning a".to_string(), 0);
        std::thread::sleep(std::time::Duration::from_millis(10));
        cache.append_reasoning("reas_lru_b", "reasoning b".to_string(), 0);
        std::thread::sleep(std::time::Duration::from_millis(10));

        // 访问 reas_lru_a 提升活跃度
        assert_eq!(
            cache.get_session_reasoning("reas_lru_a", 0),
            Some("reasoning a".to_string())
        );
        std::thread::sleep(std::time::Duration::from_millis(10));

        // 填满超额 1 项
        for i in 0..(SESSION_CACHE_LIMIT - 1) {
            cache.append_reasoning(
                &format!("reas_fill_{}", i),
                format!("reasoning fill {}", i),
                0,
            );
        }

        assert_eq!(cache.reasoning_cache_len(), SESSION_CACHE_LIMIT);
        assert_eq!(cache.get_session_reasoning("reas_lru_b", 0), None);
        assert_eq!(
            cache.get_session_reasoning("reas_lru_a", 0),
            Some("reasoning a".to_string())
        );
    }

    #[test]
    fn test_a9_rewind_and_same_turn_replacement() {
        let cache = SignatureCache::new();

        // 1. 写入 1..5 轮
        for turn in 1..=5 {
            cache.cache_session_signature(
                "sess_rewind",
                format!("{}_{}", "x".repeat(50), turn),
                turn,
            );
        }
        assert!(cache.get_session_signature_at("sess_rewind", 5).is_some());

        // 2. Rewind 到第 3 轮（重新生成第 3 轮）
        let sig3_new = format!("{}_rewind_v2", "y".repeat(60));
        cache.cache_session_signature("sess_rewind", sig3_new.clone(), 3);

        // 验证：第 4 轮和第 5 轮应当被彻底删除（未来历史被丢弃）
        assert_eq!(cache.get_session_signature_at("sess_rewind", 4), None);
        assert_eq!(cache.get_session_signature_at("sess_rewind", 5), None);
        assert_eq!(
            cache.get_session_signature_at("sess_rewind", 3),
            Some(sig3_new.clone())
        );

        // 3. 同轮更短签名替换测试：尝试用更短签名覆盖第 3 轮，应当被拒绝，保留较长签名
        let sig3_shorter = format!("{}_short", "z".repeat(50));
        cache.cache_session_signature("sess_rewind", sig3_shorter, 3);
        assert_eq!(
            cache.get_session_signature_at("sess_rewind", 3),
            Some(sig3_new.clone())
        );

        // 4. 同轮更长签名替换测试：用更长签名覆盖第 3 轮，应当成功替换
        let sig3_longer = format!("{}_much_longer_version", "w".repeat(80));
        cache.cache_session_signature("sess_rewind", sig3_longer.clone(), 3);
        assert_eq!(
            cache.get_session_signature_at("sess_rewind", 3),
            Some(sig3_longer)
        );
    }
}
