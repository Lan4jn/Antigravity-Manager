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
            cache.insert(tool_use_id.to_string(), CacheEntry::new(signature));

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
            cache.insert(signature, CacheEntry::new(family));

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
        if signature.len() > SESSION_SIGNATURE_HISTORY_BYTES {
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

            // P1-2: 写入过期会话时重建空历史，刷新 TTL
            if entry.is_expired() {
                let now = SystemTime::now();
                entry.created_at = now;
                entry.last_accessed = now;
                entry.data.clear();
            }

            // Update touch for LRU
            entry.touch();

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
                    let bytes_total: usize = entry.data.values().map(|e| e.signature.capacity()).sum();
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
                    entry.touch();
                    // Find the signature with the maximum message_count (the latest one)
                    if let Some(sig_entry) = entry.data.values().max_by_key(|e| e.message_count) {
                        tracing::debug!(
                            "[SignatureCache] Session {} (latest, msg_count={}) -> HIT (len={})",
                            session_id,
                            sig_entry.message_count,
                            sig_entry.signature.len()
                        );
                        return Some(sig_entry.signature.clone());
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
                    entry.touch();
                    if let Some(sig_entry) = entry.data.get(&message_count) {
                        tracing::debug!(
                            "[SignatureCache] Session {} (msg_count={}) -> HIT (len={})",
                            session_id,
                            message_count,
                            sig_entry.signature.len()
                        );
                        return Some(sig_entry.signature.clone());
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

            // Update touch for LRU
            entry.touch();

            if turn_index >= entry.data.len() {
                entry.data.resize(turn_index + 1, String::new());
            }

            // P1-4: 超预算 reasoning 拒绝缓存（不截断，避免协议/历史恢复问题）
            if reasoning.len() > MAX_REASONING_BYTES {
                tracing::warn!(
                    "[SignatureCache] Reasoning for session {} turn {} exceeds max bytes ({} > {}), rejecting",
                    session_id,
                    turn_index,
                    reasoning.len(),
                    MAX_REASONING_BYTES
                );
                return;
            }

            // Only update if the new reasoning is longer to prevent overwriting with partial content
            let old_len = entry.data[turn_index].len();
            if reasoning.len() > old_len {
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
                    entry.touch();
                    let text = &entry.data[turn_index];
                    if !text.trim().is_empty() {
                        tracing::debug!(
                            "[SignatureCache] Session {} (turn={}) -> Hit reasoning text cache (len: {})",
                            session_id,
                            turn_index,
                            text.len()
                        );
                        return Some(text.clone());
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
        assert!(session_entry.data.contains_key(&(SESSION_SIGNATURE_HISTORY_LIMIT + 10)));
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
}
