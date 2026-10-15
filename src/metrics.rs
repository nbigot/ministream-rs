//! Application metrics (exposed in the Prometheus text format by the web server).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

#[derive(Default)]
pub struct Metrics {
    records_ingested: AtomicU64,
    records_sent: AtomicU64,
    http_requests: Mutex<BTreeMap<(String, u16), u64>>,
}

impl Metrics {
    pub fn add_records_ingested(&self, count: u64) {
        self.records_ingested.fetch_add(count, Ordering::Relaxed);
    }

    pub fn add_records_sent(&self, count: u64) {
        self.records_sent.fetch_add(count, Ordering::Relaxed);
    }

    pub fn records_ingested(&self) -> u64 {
        self.records_ingested.load(Ordering::Relaxed)
    }

    pub fn records_sent(&self) -> u64 {
        self.records_sent.load(Ordering::Relaxed)
    }

    pub fn add_http_request(&self, method: &str, status: u16) {
        *self
            .http_requests
            .lock()
            .entry((method.to_string(), status))
            .or_default() += 1;
    }

    pub fn http_requests(&self) -> Vec<((String, u16), u64)> {
        self.http_requests
            .lock()
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }
}
