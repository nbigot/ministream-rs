//! Records iterator of a stream.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use serde::Serialize;
use tokio::sync::{watch, Notify};

use super::responses::GetStreamRecordsResponse;
use crate::apierror::ApiError;
use crate::constants;
use crate::jq::JqFilter;
use crate::log_error;
use crate::logger::Logger;
use crate::metrics::Metrics;
use crate::storage::{NextRecord, StreamIteratorHandler};
use crate::types::{MessageId, StreamIteratorRequest, StreamIteratorUuid, StreamUuid};

/// Statistics of an iterator.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamIteratorStats {
    pub records_read: u64,
    pub records_errors: u64,
    pub records_skipped: u64,
    pub records_sent: u64,
    pub last_time_read: Option<DateTime<Utc>>,
}

struct IteratorState {
    last_record_id_read: MessageId,
    stats: StreamIteratorStats,
    last_activity: Instant,
}

pub struct StreamIterator {
    stream_uuid: StreamUuid,
    uuid: StreamIteratorUuid,
    name: String,
    jq_filter: Option<JqFilter>,
    max_wait: Duration,
    handler: tokio::sync::Mutex<Box<dyn StreamIteratorHandler>>,
    state: Mutex<IteratorState>,
    closed: AtomicBool,
    close_notify: Notify,
    /// Notified each time records are written in the stream (wakes up long polling requests).
    written: Arc<watch::Sender<u64>>,
    logger: Logger,
    metrics: Arc<Metrics>,
}

impl StreamIterator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream_uuid: StreamUuid,
        uuid: StreamIteratorUuid,
        request: &StreamIteratorRequest,
        jq_filter: Option<JqFilter>,
        handler: Box<dyn StreamIteratorHandler>,
        written: Arc<watch::Sender<u64>>,
        logger: Logger,
        metrics: Arc<Metrics>,
    ) -> StreamIterator {
        StreamIterator {
            stream_uuid,
            uuid,
            name: request.name.clone(),
            jq_filter,
            max_wait: Duration::from_secs(request.max_wait_time_seconds),
            handler: tokio::sync::Mutex::new(handler),
            state: Mutex::new(IteratorState {
                last_record_id_read: 0,
                stats: StreamIteratorStats::default(),
                last_activity: Instant::now(),
            }),
            closed: AtomicBool::new(false),
            close_notify: Notify::new(),
            written,
            logger,
            metrics,
        }
    }

    pub fn uuid(&self) -> StreamIteratorUuid {
        self.uuid
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn last_record_id_read(&self) -> MessageId {
        self.state.lock().last_record_id_read
    }

    pub fn stats(&self) -> StreamIteratorStats {
        self.state.lock().stats.clone()
    }

    /// Duration since the iterator was last used.
    pub fn idle_for(&self) -> Duration {
        self.state.lock().last_activity.elapsed()
    }

    pub fn is_busy(&self) -> bool {
        self.handler.try_lock().is_err()
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Close the iterator (a pending long polling request is interrupted).
    pub async fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.close_notify.notify_waiters();
        if let Ok(mut handler) = self.handler.try_lock() {
            let _ = handler.close().await;
        }
        // otherwise the handler is closed when the pending request releases the iterator
    }

    fn not_found(&self) -> ApiError {
        ApiError::bad_request(
            constants::ERROR_STREAM_ITERATOR_NOT_FOUND,
            "iterator not found",
        )
        .details(format!("iterator not found: {}", self.uuid))
        .stream(self.stream_uuid)
    }

    /// Read the next records.
    ///
    /// When no record is available and the iterator has been created with `maxWaitTimeSeconds`,
    /// the request waits for new records (long polling).
    pub async fn get_records(
        &self,
        max_records: u64,
    ) -> Result<GetStreamRecordsResponse, ApiError> {
        let start = Instant::now();
        let mut handler = self.handler.try_lock().map_err(|_| {
            // another request is reading this iterator: tell the client to retry later
            ApiError::new(
                StatusCode::from_u16(425).unwrap_or(StatusCode::CONFLICT),
                constants::ERROR_STREAM_ITERATOR_IS_BUSY,
                "cannot get records",
            )
            .details("iterator is busy, retry later")
            .stream(self.stream_uuid)
        })?;
        if self.is_closed() {
            return Err(self.not_found());
        }
        self.state.lock().last_activity = Instant::now();

        let mut written = self.written.subscribe();
        let deadline = start + self.max_wait;
        let mut response = GetStreamRecordsResponse {
            status: String::new(),
            duration: 0,
            count: 0,
            count_errors: 0,
            count_skipped: 0,
            remain: false,
            last_record_id_read: 0,
            stream_uuid: self.stream_uuid,
            stream_iterator_uuid: self.uuid,
            records: Vec::new(),
        };
        let mut last_id = None;
        let mut records_read = 0u64;

        loop {
            let next = handler.get_next_record().await.map_err(|e| {
                log_error!(self.logger, "cannot read records", "topic" => "stream", "method" => "GetRecords", "stream.uuid" => self.stream_uuid, "it.uuid" => self.uuid, "error" => e.to_string());
                ApiError::internal(constants::ERROR_CANT_GET_MESSAGES_FROM_STREAM, "cannot get records")
                    .details(&e)
                    .stream(self.stream_uuid)
            })?;
            match next {
                NextRecord::End => {
                    let now = Instant::now();
                    if response.count == 0 && now < deadline && !self.is_closed() {
                        // long polling: wait for new records, the iterator closure, or the timeout
                        let wait = (deadline - now).min(Duration::from_secs(1));
                        let notified = self.close_notify.notified();
                        let stop = tokio::select! {
                            changed = written.changed() => changed.is_err(),
                            _ = notified => true,
                            _ = tokio::time::sleep(wait) => false,
                        };
                        if !stop {
                            continue;
                        }
                    }
                    break;
                }
                NextRecord::Invalid(id, _) => {
                    if id.is_some() {
                        last_id = id;
                    }
                    response.count_errors += 1;
                }
                NextRecord::Record(id, envelope) => {
                    last_id = Some(id);
                    records_read += 1;
                    match &self.jq_filter {
                        None => {
                            response.records.push(envelope);
                            response.count += 1;
                        }
                        Some(filter) => match filter.first(&envelope) {
                            Ok(Some(output)) => {
                                response.records.push(output);
                                response.count += 1;
                            }
                            // the record does not match the filter
                            Ok(None) => response.count_skipped += 1,
                            Err(e) => {
                                log_error!(self.logger, "jq error", "topic" => "stream", "method" => "GetRecords", "stream.uuid" => self.stream_uuid, "jq" => filter.source(), "error" => e);
                                response.count_errors += 1;
                            }
                        },
                    }
                    if response.records.len() as u64 >= max_records {
                        // the maximum count of records for a single response is reached
                        response.remain = true;
                        break;
                    }
                }
            }
        }
        drop(handler);

        {
            let mut state = self.state.lock();
            if let Some(id) = last_id {
                state.last_record_id_read = id;
            }
            let stats = &mut state.stats;
            stats.records_read += records_read;
            stats.records_errors += response.count_errors as u64;
            stats.records_skipped += response.count_skipped as u64;
            stats.records_sent += response.count as u64;
            stats.last_time_read = Some(Utc::now());
            state.last_activity = Instant::now();
            response.last_record_id_read = state.last_record_id_read;
        }
        self.metrics.add_records_sent(response.count as u64);
        response.status = "success".into();
        response.duration = start.elapsed().as_millis() as i64;
        Ok(response)
    }
}
