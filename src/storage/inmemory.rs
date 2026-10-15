//! In memory storage provider: nothing is persisted, all streams are lost when the server stops.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;
use serde_json::Value;

use super::catalog::Catalog;
use super::{NextRecord, StorageProvider, StreamIteratorHandler, StreamWriter, WrittenBatch};
use crate::config::Config;
use crate::error::{Error, Result};
use crate::humanize::parse_bytes;
use crate::logger::Logger;
use crate::types::{
    json_size, DeferedStreamRecord, IteratorType, MessageId, SharedStreamInfo,
    StreamIteratorRequest, StreamIteratorUuid, StreamUuid,
};
use crate::{log_debug, log_info};

#[derive(Default)]
struct InMemoryData {
    records: Vec<DeferedStreamRecord>,
    size_in_bytes: u64,
}

/// Records of a stream.
pub struct InMemoryStream {
    data: RwLock<InMemoryData>,
    /// Maximum count of records (0 means unlimited).
    max_records: u64,
    /// Maximum size of the records (0 means unlimited).
    max_size_in_bytes: u64,
}

impl InMemoryStream {
    fn new(max_records: u64, max_size_in_bytes: u64) -> InMemoryStream {
        InMemoryStream {
            data: RwLock::new(InMemoryData::default()),
            max_records,
            max_size_in_bytes,
        }
    }

    pub fn len(&self) -> usize {
        self.data.read().records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn get(&self, index: usize) -> Option<(MessageId, Value)> {
        self.data
            .read()
            .records
            .get(index)
            .map(|r| (r.id, r.to_envelope()))
    }

    /// Initial position of an iterator.
    fn seek_position(&self, request: &StreamIteratorRequest) -> Result<usize> {
        let data = self.data.read();
        let records = &data.records;
        let position = match request.iterator_type {
            IteratorType::FirstMessage => 0,
            IteratorType::LastMessage => records.len().saturating_sub(1),
            IteratorType::AfterLastMessage => records.len(),
            IteratorType::AtMessageId => records
                .binary_search_by_key(&request.message_id, |r| r.id)
                .map_err(|_| Error::not_found("message id not found"))?,
            IteratorType::AfterMessageId => {
                let last_id = records.last().map(|r| r.id).unwrap_or(0);
                if request.message_id > last_id {
                    return Err(Error::not_found("message id not found"));
                }
                records.partition_point(|r| r.id <= request.message_id)
            }
            IteratorType::AtTimestamp => {
                let timestamp = request
                    .timestamp
                    .ok_or_else(|| Error::msg("missing timestamp"))?;
                records.partition_point(|r| r.creation_date < timestamp)
            }
        };
        Ok(position)
    }
}

/// In memory storage provider.
pub struct InMemoryStorage {
    logger: Logger,
    log_verbosity: i64,
    catalog: Catalog,
    streams: RwLock<HashMap<StreamUuid, Arc<InMemoryStream>>>,
    max_records_by_stream: u64,
    max_size_in_bytes: u64,
}

impl InMemoryStorage {
    pub fn new(config: &Config, logger: Logger) -> Result<InMemoryStorage> {
        let max_size = config.storage.in_memory.max_size.trim();
        let max_size_in_bytes = if max_size.is_empty() {
            0
        } else {
            parse_bytes(max_size).map_err(|e| {
                Error::msg(format!(
                    "cannot parse value for configuration storage.inmemory.maxSize: {e}"
                ))
            })?
        };
        Ok(InMemoryStorage {
            logger,
            log_verbosity: config.storage.log_verbosity,
            catalog: Catalog::default(),
            streams: RwLock::new(HashMap::new()),
            max_records_by_stream: config.storage.in_memory.max_records_by_stream,
            max_size_in_bytes,
        })
    }

    fn get_stream(&self, stream_uuid: &StreamUuid) -> Result<Arc<InMemoryStream>> {
        self.streams
            .read()
            .get(stream_uuid)
            .cloned()
            .ok_or_else(|| Error::not_found(format!("stream not found: {stream_uuid}")))
    }
}

#[async_trait]
impl StorageProvider for InMemoryStorage {
    fn name(&self) -> &'static str {
        "InMemory"
    }

    async fn init(&self) -> Result<()> {
        self.streams.write().clear();
        self.catalog.clear();
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        self.streams.write().clear();
        self.catalog.clear();
        Ok(())
    }

    fn stream_exists(&self, stream_uuid: &StreamUuid) -> bool {
        self.catalog.contains(stream_uuid)
    }

    async fn save_stream_catalog(&self) -> Result<()> {
        // the catalog is not persistent: nothing to save
        Ok(())
    }

    async fn load_streams(&self) -> Result<Vec<SharedStreamInfo>> {
        // the catalog is not persistent (only in memory): all streams are lost when the server stops
        log_info!(self.logger, "No stream found", "topic" => "stream", "method" => "LoadStreams");
        Ok(Vec::new())
    }

    async fn on_create_stream(&self, info: &SharedStreamInfo) -> Result<()> {
        let uuid = info.read().uuid;
        let stream = InMemoryStream::new(self.max_records_by_stream, self.max_size_in_bytes);
        self.streams.write().insert(uuid, Arc::new(stream));
        self.catalog.insert(info.clone());
        Ok(())
    }

    async fn on_update_stream_properties(&self, _info: &SharedStreamInfo) -> Result<()> {
        Ok(())
    }

    fn get_stream_info(&self, stream_uuid: &StreamUuid) -> Result<SharedStreamInfo> {
        self.catalog.get(stream_uuid)
    }

    async fn build_index(&self, _stream_uuid: &StreamUuid) -> Result<Value> {
        // there is no index for in memory storage
        Ok(Value::String(String::new()))
    }

    async fn new_stream_iterator_handler(
        &self,
        stream_uuid: &StreamUuid,
        _iterator_uuid: &StreamIteratorUuid,
    ) -> Result<Box<dyn StreamIteratorHandler>> {
        Ok(Box::new(StreamIteratorHandlerInMemory {
            stream: self.get_stream(stream_uuid)?,
            position: 0,
        }))
    }

    async fn new_stream_writer(&self, info: &SharedStreamInfo) -> Result<Box<dyn StreamWriter>> {
        let uuid = info.read().uuid;
        Ok(Box::new(StreamWriterInMemory {
            stream: self.get_stream(&uuid)?,
            info: info.clone(),
            logger: self.logger.clone(),
            log_verbosity: self.log_verbosity,
            stream_uuid: uuid,
        }))
    }

    async fn delete_stream(&self, stream_uuid: &StreamUuid) -> Result<()> {
        self.streams.write().remove(stream_uuid);
        self.catalog.remove(stream_uuid);
        Ok(())
    }
}

struct StreamWriterInMemory {
    stream: Arc<InMemoryStream>,
    info: SharedStreamInfo,
    logger: Logger,
    log_verbosity: i64,
    stream_uuid: StreamUuid,
}

#[async_trait]
impl StreamWriter for StreamWriterInMemory {
    async fn init(&mut self) -> Result<()> {
        if self.log_verbosity > 1 {
            log_debug!(self.logger, "init stream writer", "topic" => "stream", "method" => "Init", "stream.uuid" => self.stream_uuid);
        }
        Ok(())
    }

    async fn open(&mut self) -> Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    async fn write(&mut self, records: &mut Vec<DeferedStreamRecord>) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        if self.log_verbosity > 0 {
            log_debug!(self.logger, "write records into memory", "topic" => "stream", "method" => "Write", "stream.uuid" => self.stream_uuid, "records.cpt" => records.len());
        }

        let pending = std::mem::take(records);
        let mut batch = WrittenBatch::default();
        let mut error = None;
        {
            let mut data = self.stream.data.write();
            let mut iter = pending.into_iter();
            while let Some(record) = iter.next() {
                let size = json_size(&record.msg);
                let limit = if self.stream.max_records > 0
                    && data.records.len() as u64 >= self.stream.max_records
                {
                    Some(format!(
                        "cannot add more record into the stream (limit is {} records)",
                        self.stream.max_records
                    ))
                } else if self.stream.max_size_in_bytes > 0
                    && data.size_in_bytes + size > self.stream.max_size_in_bytes
                {
                    Some(format!(
                        "cannot add more record into the stream (limit is {} bytes)",
                        self.stream.max_size_in_bytes
                    ))
                } else {
                    None
                };
                if let Some(message) = limit {
                    error = Some(Error::msg(message));
                    records.push(record);
                    records.extend(iter);
                    break;
                }
                if self.log_verbosity > 1 {
                    log_debug!(self.logger, "write record into memory", "topic" => "stream", "method" => "Write", "stream.uuid" => self.stream_uuid, "msg" => record);
                }
                batch.add(&record, size);
                data.size_in_bytes += size;
                data.records.push(record);
            }
        }
        batch.apply(&self.info);

        match error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

struct StreamIteratorHandlerInMemory {
    stream: Arc<InMemoryStream>,
    /// Index of the next record to read.
    position: usize,
}

#[async_trait]
impl StreamIteratorHandler for StreamIteratorHandlerInMemory {
    async fn open(&mut self) -> Result<()> {
        self.position = 0;
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    async fn seek(&mut self, request: &StreamIteratorRequest) -> Result<()> {
        self.position = self.stream.seek_position(request)?;
        Ok(())
    }

    async fn get_next_record(&mut self) -> Result<NextRecord> {
        match self.stream.get(self.position) {
            Some((id, record)) => {
                self.position += 1;
                Ok(NextRecord::Record(id, record))
            }
            None => Ok(NextRecord::End),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::StreamInfo;
    use chrono::{Duration, Utc};
    use serde_json::json;
    use uuid::Uuid;

    fn request(iterator_type: IteratorType) -> StreamIteratorRequest {
        StreamIteratorRequest {
            iterator_type,
            message_id: 0,
            timestamp: None,
            jq_filter: String::new(),
            max_wait_time_seconds: 0,
            name: String::new(),
        }
    }

    async fn setup(config: Config) -> (InMemoryStorage, SharedStreamInfo, Box<dyn StreamWriter>) {
        let storage = InMemoryStorage::new(&config, Logger::nop()).unwrap();
        storage.init().await.unwrap();
        let info = StreamInfo::new(Uuid::new_v4()).into_shared();
        storage.on_create_stream(&info).await.unwrap();
        let mut writer = storage.new_stream_writer(&info).await.unwrap();
        writer.init().await.unwrap();
        writer.open().await.unwrap();
        (storage, info, writer)
    }

    fn records(ids: std::ops::RangeInclusive<u64>) -> Vec<DeferedStreamRecord> {
        let start = Utc::now();
        ids.map(|id| DeferedStreamRecord {
            id,
            creation_date: start + Duration::seconds(id as i64),
            msg: json!({ "n": id }),
        })
        .collect()
    }

    async fn read_all(handler: &mut Box<dyn StreamIteratorHandler>) -> Vec<MessageId> {
        let mut ids = Vec::new();
        while let NextRecord::Record(id, _) = handler.get_next_record().await.unwrap() {
            ids.push(id);
        }
        ids
    }

    #[tokio::test]
    async fn write_and_iterate() {
        let (storage, info, mut writer) = setup(Config::default()).await;
        let uuid = info.read().uuid;
        let mut batch = records(1..=5);
        let fourth_timestamp = batch[3].creation_date;
        writer.write(&mut batch).await.unwrap();
        assert!(batch.is_empty());
        assert_eq!(info.read().readable_messages.cpt_messages, 5);
        assert_eq!(info.read().readable_messages.first_msg_id, 1);
        assert_eq!(info.read().readable_messages.last_msg_id, 5);

        let cases = vec![
            (request(IteratorType::FirstMessage), vec![1, 2, 3, 4, 5]),
            (request(IteratorType::LastMessage), vec![5]),
            (request(IteratorType::AfterLastMessage), vec![]),
            (
                StreamIteratorRequest {
                    message_id: 3,
                    ..request(IteratorType::AtMessageId)
                },
                vec![3, 4, 5],
            ),
            (
                StreamIteratorRequest {
                    message_id: 3,
                    ..request(IteratorType::AfterMessageId)
                },
                vec![4, 5],
            ),
            (
                StreamIteratorRequest {
                    message_id: 0,
                    ..request(IteratorType::AfterMessageId)
                },
                vec![1, 2, 3, 4, 5],
            ),
            (
                StreamIteratorRequest {
                    timestamp: Some(fourth_timestamp),
                    ..request(IteratorType::AtTimestamp)
                },
                vec![4, 5],
            ),
        ];
        for (req, expected) in cases {
            let mut handler = storage
                .new_stream_iterator_handler(&uuid, &Uuid::new_v4())
                .await
                .unwrap();
            handler.open().await.unwrap();
            handler.seek(&req).await.unwrap();
            assert_eq!(read_all(&mut handler).await, expected, "{req:?}");
        }

        let mut handler = storage
            .new_stream_iterator_handler(&uuid, &Uuid::new_v4())
            .await
            .unwrap();
        let req = StreamIteratorRequest {
            message_id: 42,
            ..request(IteratorType::AtMessageId)
        };
        assert!(handler.seek(&req).await.is_err());

        // an iterator positioned after the last message sees the new records
        let mut handler = storage
            .new_stream_iterator_handler(&uuid, &Uuid::new_v4())
            .await
            .unwrap();
        handler
            .seek(&request(IteratorType::AfterLastMessage))
            .await
            .unwrap();
        writer.write(&mut records(6..=7)).await.unwrap();
        assert_eq!(read_all(&mut handler).await, vec![6, 7]);
    }

    #[tokio::test]
    async fn max_records_limit() {
        let mut config = Config::default();
        config.storage.in_memory.max_records_by_stream = 3;
        let (_storage, info, mut writer) = setup(config).await;
        let mut batch = records(1..=5);
        assert!(writer.write(&mut batch).await.is_err());
        assert_eq!(batch.iter().map(|r| r.id).collect::<Vec<_>>(), vec![4, 5]);
        assert_eq!(info.read().readable_messages.cpt_messages, 3);
    }

    #[test]
    fn invalid_max_size() {
        let mut config = Config::default();
        config.storage.in_memory.max_size = "lots".into();
        assert!(InMemoryStorage::new(&config, Logger::nop()).is_err());
    }
}
