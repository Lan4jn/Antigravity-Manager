use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

const VISION_SESSION_TTL: Duration = Duration::from_secs(30 * 60); // 30 分钟未活跃则过期
const MAX_VISION_SESSIONS: usize = 100; // 最多保留 100 个并发会话

#[derive(Debug, Clone, Default)]
pub struct ZaiVisionMcpState {
    sessions: Arc<Mutex<HashMap<String, ZaiVisionSession>>>,
}

#[derive(Debug, Clone)]
struct ZaiVisionSession {
    last_accessed: Instant,
}

impl ZaiVisionMcpState {
    pub fn new() -> Self {
        Self::default()
    }

    /// 清理已过期会话；若依然超出上限，淘汰最旧的会话
    pub async fn cleanup_expired_sessions(&self) -> usize {
        let mut sessions = self.sessions.lock().await;
        Self::cleanup_sessions_internal(&mut sessions, VISION_SESSION_TTL, MAX_VISION_SESSIONS)
    }

    fn cleanup_sessions_internal(
        sessions: &mut HashMap<String, ZaiVisionSession>,
        ttl: Duration,
        max_limit: usize,
    ) -> usize {
        let now = Instant::now();
        let initial_len = sessions.len();

        // 1. 清除超时条目
        sessions.retain(|_, s| now.duration_since(s.last_accessed) < ttl);

        // 2. 超限强行淘汰最旧条目
        if sessions.len() > max_limit {
            let excess = sessions.len() - max_limit;
            let mut items: Vec<(String, Instant)> = sessions
                .iter()
                .map(|(k, v)| (k.clone(), v.last_accessed))
                .collect();
            items.sort_unstable_by_key(|(_, ts)| *ts);
            for (k, _) in items.into_iter().take(excess) {
                sessions.remove(&k);
            }
        }

        initial_len.saturating_sub(sessions.len())
    }

    pub async fn create_session(&self) -> String {
        let mut sessions = self.sessions.lock().await;

        let session_id = uuid::Uuid::new_v4().to_string();
        sessions.insert(
            session_id.clone(),
            ZaiVisionSession {
                last_accessed: Instant::now(),
            },
        );

        // 插入后进行容量与超时淘汰，确保总条数严格不超过 MAX_VISION_SESSIONS
        Self::cleanup_sessions_internal(&mut sessions, VISION_SESSION_TTL, MAX_VISION_SESSIONS);

        session_id
    }

    pub async fn has_session(&self, session_id: &str) -> bool {
        let mut sessions = self.sessions.lock().await;
        if let Some(session) = sessions.get_mut(session_id) {
            if session.last_accessed.elapsed() < VISION_SESSION_TTL {
                session.last_accessed = Instant::now();
                return true;
            } else {
                sessions.remove(session_id);
                return false;
            }
        }
        false
    }

    pub async fn remove_session(&self, session_id: &str) {
        let mut sessions = self.sessions.lock().await;
        sessions.remove(session_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_vision_session_lifecycle_and_expiry() {
        let state = ZaiVisionMcpState::new();
        let sid = state.create_session().await;
        assert!(state.has_session(&sid).await);

        // 模拟过期会话
        {
            let mut sessions = state.sessions.lock().await;
            if let Some(sess) = sessions.get_mut(&sid) {
                sess.last_accessed = Instant::now()
                    .checked_sub(Duration::from_secs(31 * 60))
                    .unwrap();
            }
        }

        // 过期后 has_session 自动清除并返回 false
        assert!(!state.has_session(&sid).await);
        assert!(!state.has_session(&sid).await);
    }

    #[tokio::test]
    async fn test_vision_session_capacity_limit() {
        let state = ZaiVisionMcpState::new();
        let mut sids = Vec::new();

        // 插入超过容量上限的会话
        for _ in 0..(MAX_VISION_SESSIONS + 10) {
            sids.push(state.create_session().await);
        }

        let len = state.sessions.lock().await.len();
        assert_eq!(len, MAX_VISION_SESSIONS);

        // 最早的 10 个应当已被淘汰
        for sid in &sids[..10] {
            let contains = state.sessions.lock().await.contains_key(sid);
            assert!(!contains, "Oldest session should have been evicted");
        }

        // 最新的会话应当存在
        for sid in &sids[10..] {
            let contains = state.sessions.lock().await.contains_key(sid);
            assert!(contains, "Recent session should exist");
        }
    }

    #[tokio::test]
    async fn test_remove_session() {
        let state = ZaiVisionMcpState::new();
        let sid = state.create_session().await;
        assert!(state.has_session(&sid).await);

        state.remove_session(&sid).await;
        assert!(!state.has_session(&sid).await);
    }
}
