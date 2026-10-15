//! Deduplication of the batches of records (header `x-ministream-batch-id`).
//!
//! A batch id is considered as processed once it has been added, it is forgotten after
//! the time to live: a batch received again after that is not considered as a duplicate.
//! This is a trade-off between memory and safety.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

pub struct RequestDeduplicator {
    ttl: Duration,
    processed: Mutex<HashMap<String, Instant>>,
}

impl RequestDeduplicator {
    pub fn new(ttl: Duration) -> RequestDeduplicator {
        RequestDeduplicator {
            ttl,
            processed: Mutex::new(HashMap::new()),
        }
    }

    /// Register a request id, returns false when it was already processed.
    pub fn try_add(&self, request_id: &str) -> bool {
        let now = Instant::now();
        let mut processed = self.processed.lock();
        match processed.get(request_id) {
            Some(at) if now.duration_since(*at) <= self.ttl => false,
            _ => {
                processed.insert(request_id.to_string(), now);
                true
            }
        }
    }

    pub fn remove(&self, request_id: &str) {
        self.processed.lock().remove(request_id);
    }

    /// Remove the expired entries.
    pub fn cleanup(&self) {
        let ttl = self.ttl;
        self.processed.lock().retain(|_, at| at.elapsed() <= ttl);
    }

    pub fn clear(&self) {
        self.processed.lock().clear();
    }

    pub fn len(&self) -> usize {
        self.processed.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deduplicate() {
        let dedup = RequestDeduplicator::new(Duration::from_secs(60));
        assert!(dedup.try_add("s:1"));
        assert!(!dedup.try_add("s:1"));
        assert!(dedup.try_add("s:2"));
        dedup.remove("s:1");
        assert!(dedup.try_add("s:1"));
        dedup.cleanup();
        assert_eq!(dedup.len(), 2);

        let dedup = RequestDeduplicator::new(Duration::ZERO);
        assert!(dedup.try_add("x"));
        std::thread::sleep(Duration::from_millis(2));
        assert!(dedup.try_add("x"));
        std::thread::sleep(Duration::from_millis(2));
        dedup.cleanup();
        assert!(dedup.is_empty());
    }
}
