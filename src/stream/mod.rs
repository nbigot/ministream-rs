//! Streams: ingestion of records and records iterators.

pub mod iterator;
pub mod request;
pub mod responses;

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use chrono::Utc;
use parking_lot::Mutex;
use serde_json::Value;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::apierror::ApiError;
use crate::buffering::StreamIngestBuffer;
use crate::constants;
use crate::error::{Error, Result};
use crate::jq::JqFilter;
use crate::logger::Logger;
use crate::metrics::Metrics;
use crate::types::{
    json_size, DeferedStreamRecord, MessageId, SharedStreamInfo, StreamInfo, StreamIteratorUuid,
    StreamProperties, StreamUuid,
};
use crate::{log_debug, log_error, log_info};

pub use iterator::StreamIterator;
pub use responses::GetStreamRecordsResponse;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamState {
    None,
    Starting,
    Running,
    Stopping,
}

impl StreamState {
    pub fn as_str(&self) -> &'static str {
        match self {
            StreamState::None => "none",
            StreamState::Starting => "starting",
            StreamState::Running => "running",
            StreamState::Stopping => "stopping",
        }
    }
}

pub struct Stream {
    uuid: StreamUuid,
    info: SharedStreamInfo,
    logger: Logger,
    log_verbosity: i64,
    state: Mutex<StreamState>,
    /// Sender of the ingestion channel (taken when the stream stops).
    ingest: tokio::sync::Mutex<Option<mpsc::Sender<DeferedStreamRecord>>>,
    iterators: Mutex<HashMap<StreamIteratorUuid, Arc<StreamIterator>>>,
    task: Mutex<Option<JoinHandle<()>>>,
    written: Arc<watch::Sender<u64>>,
    metrics: Arc<Metrics>,
}

impl Stream {
    pub fn new(
        info: SharedStreamInfo,
        logger: Logger,
        log_verbosity: i64,
        metrics: Arc<Metrics>,
    ) -> Arc<Stream> {
        let uuid = info.read().uuid;
        Arc::new(Stream {
            uuid,
            info,
            logger,
            log_verbosity,
            state: Mutex::new(StreamState::None),
            ingest: tokio::sync::Mutex::new(None),
            iterators: Mutex::new(HashMap::new()),
            task: Mutex::new(None),
            written: Arc::new(watch::channel(0).0),
            metrics,
        })
    }

    pub fn uuid(&self) -> StreamUuid {
        self.uuid
    }

    pub fn shared_info(&self) -> &SharedStreamInfo {
        &self.info
    }

    /// Snapshot of the stream information.
    pub fn info(&self) -> StreamInfo {
        self.info.read().clone()
    }

    pub fn state(&self) -> StreamState {
        *self.state.lock()
    }

    fn set_state(&self, state: StreamState) {
        if self.log_verbosity > 0 {
            log_debug!(self.logger, "Set stream state", "topic" => "stream", "method" => "setState", "stream.uuid" => self.uuid, "stream.state" => state.as_str());
        }
        *self.state.lock() = state;
    }

    fn not_running() -> Error {
        Error::msg("stream state is not running")
    }

    /// Start the stream: records sent to the stream are buffered then written by the ingest buffer.
    pub fn start(
        self: &Arc<Self>,
        ingest_buffer: StreamIngestBuffer,
        channel_capacity: usize,
        iterator_idle_timeout: Option<Duration>,
    ) -> Result<()> {
        {
            let mut state = self.state.lock();
            if *state != StreamState::None {
                return Err(Error::msg("stream state is already started"));
            }
            *state = StreamState::Starting;
        }
        let (sender, receiver) = mpsc::channel(channel_capacity.max(1));
        *self
            .ingest
            .try_lock()
            .map_err(|_| Error::msg("stream is busy"))? = Some(sender);
        let task = tokio::spawn(ingest_loop(
            receiver,
            ingest_buffer,
            self.written.clone(),
            Arc::downgrade(self),
            iterator_idle_timeout,
            self.logger.clone(),
            self.uuid,
        ));
        *self.task.lock() = Some(task);
        self.set_state(StreamState::Running);
        Ok(())
    }

    /// Stop the stream: flush the buffered records, close the writer and the iterators.
    pub async fn close(&self) -> Result<()> {
        {
            let mut state = self.state.lock();
            if *state != StreamState::Running {
                return Err(Self::not_running());
            }
            *state = StreamState::Stopping;
        }
        log_debug!(self.logger, "Stopping stream...", "topic" => "stream", "method" => "Close", "stream.uuid" => self.uuid);
        // closing the channel stops the ingest loop once every pending record is written
        self.ingest.lock().await.take();
        let task = self.task.lock().take();
        if let Some(task) = task {
            if let Err(e) = task.await {
                log_error!(self.logger, "Stream ingest task failed", "topic" => "stream", "method" => "Close", "stream.uuid" => self.uuid, "error" => e.to_string());
            }
        }
        self.close_iterators().await;
        self.set_state(StreamState::None);
        Ok(())
    }

    /// Put records into the stream, returns their message ids.
    pub async fn put_messages(&self, records: Vec<Value>) -> Result<Vec<MessageId>> {
        if self.state() != StreamState::Running {
            return Err(Self::not_running());
        }
        if records.is_empty() {
            return Err(Error::msg("no records to ingest"));
        }
        let guard = self.ingest.lock().await;
        let sender = guard.as_ref().ok_or_else(Self::not_running)?;

        let now = Utc::now();
        let mut ids = Vec::with_capacity(records.len());
        let mut deferred = Vec::with_capacity(records.len());
        {
            let mut info = self.info.write();
            let ingested = &mut info.ingested_messages;
            if ingested.cpt_messages == 0 {
                // first message ever of the stream
                ingested.first_msg_id = ingested.last_msg_id + 1;
                ingested.first_msg_timestamp = now;
            }
            for msg in records {
                ingested.last_msg_id += 1;
                ingested.cpt_messages += 1;
                ingested.size_in_bytes += json_size(&msg);
                ingested.last_msg_timestamp = now;
                ids.push(ingested.last_msg_id);
                deferred.push(DeferedStreamRecord {
                    id: ingested.last_msg_id,
                    creation_date: now,
                    msg,
                });
            }
        }
        for record in deferred {
            sender
                .send(record)
                .await
                .map_err(|_| Error::msg("stream ingestion is stopped"))?;
        }
        self.metrics.add_records_ingested(ids.len() as u64);
        Ok(ids)
    }

    pub async fn put_message(&self, record: Value) -> Result<MessageId> {
        Ok(self.put_messages(vec![record]).await?[0])
    }

    /// Subscribe to the notifications sent each time records are written.
    pub fn written_notifier(&self) -> Arc<watch::Sender<u64>> {
        self.written.clone()
    }

    pub fn add_iterator(&self, iterator: Arc<StreamIterator>) -> Result<()> {
        if self.state() != StreamState::Running {
            return Err(Self::not_running());
        }
        let uuid = iterator.uuid();
        self.iterators.lock().insert(uuid, iterator);
        log_info!(self.logger, "Add stream iterator", "topic" => "stream", "method" => "AddIterator", "stream.uuid" => self.uuid, "it.uuid" => uuid);
        Ok(())
    }

    pub fn get_iterator(&self, iterator_uuid: &StreamIteratorUuid) -> Option<Arc<StreamIterator>> {
        self.iterators.lock().get(iterator_uuid).cloned()
    }

    pub fn iterators_count(&self) -> usize {
        self.iterators.lock().len()
    }

    pub async fn close_iterator(&self, iterator_uuid: &StreamIteratorUuid) -> Result<()> {
        let iterator = self
            .iterators
            .lock()
            .remove(iterator_uuid)
            // maybe the iterator has timed out or has been already deleted
            .ok_or_else(|| Error::not_found("iterator not found"))?;
        log_info!(self.logger, "Close stream iterator", "topic" => "stream", "method" => "CloseIterator", "stream.uuid" => self.uuid, "it.uuid" => iterator_uuid);
        iterator.close().await;
        Ok(())
    }

    pub async fn close_iterators(&self) {
        log_debug!(self.logger, "Close stream iterators", "topic" => "stream", "method" => "CloseIterators", "stream.uuid" => self.uuid);
        let iterators: Vec<_> = self.iterators.lock().drain().map(|(_, it)| it).collect();
        for iterator in iterators {
            iterator.close().await;
        }
    }

    /// Close the iterators which have not been used for the given duration.
    pub async fn close_idle_iterators(&self, idle_timeout: Duration) {
        let idle: Vec<_> = {
            let mut iterators = self.iterators.lock();
            let uuids: Vec<_> = iterators
                .values()
                .filter(|it| !it.is_busy() && it.idle_for() >= idle_timeout)
                .map(|it| it.uuid())
                .collect();
            uuids
                .iter()
                .filter_map(|uuid| iterators.remove(uuid))
                .collect()
        };
        for iterator in idle {
            log_info!(self.logger, "Close idle stream iterator", "topic" => "stream", "method" => "CloseIdleIterators", "stream.uuid" => self.uuid, "it.uuid" => iterator.uuid());
            iterator.close().await;
        }
    }

    pub async fn get_records(
        &self,
        iterator_uuid: &StreamIteratorUuid,
        max_records: u64,
    ) -> Result<GetStreamRecordsResponse, ApiError> {
        if self.state() != StreamState::Running {
            return Err(ApiError::internal(
                constants::ERROR_CANT_GET_MESSAGES_FROM_STREAM,
                "cannot get records",
            )
            .details("stream state is not running")
            .stream(self.uuid));
        }
        let iterator = self.get_iterator(iterator_uuid).ok_or_else(|| {
            // maybe the iterator has timed out and has been deleted
            ApiError::bad_request(
                constants::ERROR_STREAM_ITERATOR_NOT_FOUND,
                "iterator not found",
            )
            .details(format!("iterator not found: {iterator_uuid}"))
            .stream(self.uuid)
        })?;
        iterator.get_records(max_records).await
    }

    pub fn properties(&self) -> StreamProperties {
        self.info.read().properties.clone()
    }

    pub fn set_properties(&self, properties: StreamProperties) {
        self.info.write().set_properties(properties);
    }

    pub fn update_properties(&self, properties: StreamProperties) {
        self.info.write().update_properties(properties);
    }

    /// Check if the stream properties match a jq filter (the filter must return `true`).
    pub fn match_filter_properties(&self, filter: &JqFilter) -> bool {
        let properties = Value::Object(self.properties());
        match filter.matches(&properties) {
            Ok(matching) => matching,
            Err(e) => {
                log_error!(self.logger, "jq error", "topic" => "stream", "method" => "MatchFilterProperties", "stream.uuid" => self.uuid, "jq" => filter.source(), "error" => e);
                false
            }
        }
    }
}

async fn flush(
    buffer: &mut StreamIngestBuffer,
    written: &watch::Sender<u64>,
    logger: &Logger,
    uuid: StreamUuid,
) {
    match buffer.flush().await {
        Ok(0) => {}
        Ok(_) => written.send_modify(|v| *v = v.wrapping_add(1)),
        Err(e) => {
            log_error!(logger, "Can't save stream ingest buffer", "topic" => "stream", "method" => "Run", "stream.uuid" => uuid, "error" => e.to_string());
            // records may have been partially written
            written.send_modify(|v| *v = v.wrapping_add(1));
        }
    }
}

/// Receive the records ingested into a stream and write them in bulk.
async fn ingest_loop(
    mut receiver: mpsc::Receiver<DeferedStreamRecord>,
    mut buffer: StreamIngestBuffer,
    written: Arc<watch::Sender<u64>>,
    stream: Weak<Stream>,
    iterator_idle_timeout: Option<Duration>,
    logger: Logger,
    uuid: StreamUuid,
) {
    log_debug!(logger, "Starting stream", "topic" => "stream", "method" => "Run", "stream.uuid" => uuid);
    let frequency = buffer.bulk_flush_frequency();
    let retry_delay = frequency.max(Duration::from_secs(1));
    let far_future = || Instant::now() + Duration::from_secs(86_400 * 365);
    let mut deadline: Option<Instant> = None;
    let mut cleanup = iterator_idle_timeout.map(|timeout| {
        let period = (timeout / 2).clamp(Duration::from_secs(1), Duration::from_secs(60));
        tokio::time::interval_at(Instant::now() + period, period)
    });

    loop {
        tokio::select! {
            message = receiver.recv() => {
                let Some(record) = message else {
                    // the channel is closed and empty: the stream is stopping
                    break;
                };
                buffer.push(record);
                // take every record already waiting in the channel
                while !buffer.is_full() {
                    match receiver.try_recv() {
                        Ok(record) => buffer.push(record),
                        Err(_) => break,
                    }
                }
                if frequency.is_zero() || buffer.is_full() {
                    flush(&mut buffer, &written, &logger, uuid).await;
                    deadline = (!buffer.is_empty()).then(|| Instant::now() + retry_delay);
                } else if deadline.is_none() {
                    deadline = Some(Instant::now() + frequency);
                }
            }
            _ = tokio::time::sleep_until(deadline.unwrap_or_else(far_future)), if deadline.is_some() => {
                flush(&mut buffer, &written, &logger, uuid).await;
                deadline = (!buffer.is_empty()).then(|| Instant::now() + retry_delay);
            }
            _ = async {
                match cleanup.as_mut() {
                    Some(interval) => { interval.tick().await; }
                    None => std::future::pending::<()>().await,
                }
            } => {
                if let (Some(stream), Some(timeout)) = (stream.upgrade(), iterator_idle_timeout) {
                    stream.close_idle_iterators(timeout).await;
                }
            }
        }
    }

    // save the remaining records
    let mut attempts = 0;
    while !buffer.is_empty() && attempts < 3 {
        flush(&mut buffer, &written, &logger, uuid).await;
        attempts += 1;
    }
    if let Err(e) = buffer.close().await {
        log_error!(logger, "Can't close stream ingest buffer", "topic" => "stream", "method" => "Run", "stream.uuid" => uuid, "error" => e.to_string());
    }
    log_debug!(logger, "Stream stopped", "topic" => "stream", "method" => "Run", "stream.uuid" => uuid);
}
