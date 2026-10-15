//! Storage providers.
//!
//! A storage provider persists the catalog of streams, the stream information and the
//! records of the streams. Available providers:
//! - `InMemory`: nothing is persisted, everything is lost when the server stops,
//! - `JSONFile`: records are stored in JSON lines files with a binary index,
//! - `MySQL`: records are stored in MySQL tables (cargo feature `mysql`).

pub mod catalog;
pub mod inmemory;
pub mod jsonfile;
#[cfg(feature = "mysql")]
pub mod mysql;

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

use crate::config::Config;
use crate::error::{Error, Result};
use crate::logger::Logger;
use crate::types::{
    DeferedStreamRecord, MessageId, SharedStreamInfo, StreamIteratorRequest, StreamIteratorUuid,
    StreamUuid,
};

/// Names of the storage provider types.
pub const STORAGE_PROVIDER_TYPES: &[&str] = &["InMemory", "JSONFile", "MySQL"];

/// Persist the records ingested in a stream.
#[async_trait]
pub trait StreamWriter: Send {
    /// Prepare the storage of the stream (create files, tables, ...).
    async fn init(&mut self) -> Result<()>;
    /// Open the writer (called once after `init`).
    async fn open(&mut self) -> Result<()>;
    /// Close the writer (called once when the stream stops).
    async fn close(&mut self) -> Result<()>;
    /// Persist the records and update the readable messages of the stream information.
    ///
    /// The records successfully persisted are removed from the front of the vector, so that
    /// on error the remaining records can be retried without duplicates.
    async fn write(&mut self, records: &mut Vec<DeferedStreamRecord>) -> Result<()>;
}

/// Result of reading the next record of a stream.
#[derive(Debug)]
pub enum NextRecord {
    /// A record (its JSON envelope `{"i": id, "d": date, "m": message}`).
    Record(MessageId, Value),
    /// A record exists but cannot be decoded, the iterator may continue with the next one.
    Invalid(Option<MessageId>, Error),
    /// No more record available for now.
    End,
}

/// Statistics about a batch of records written by a stream writer.
#[derive(Debug, Default)]
pub(crate) struct WrittenBatch {
    count: u64,
    size_in_bytes: u64,
    first: Option<(MessageId, DateTime<Utc>)>,
    last: Option<(MessageId, DateTime<Utc>)>,
}

impl WrittenBatch {
    pub(crate) fn add(&mut self, record: &DeferedStreamRecord, size_in_bytes: u64) {
        if self.first.is_none() {
            self.first = Some((record.id, record.creation_date));
        }
        self.last = Some((record.id, record.creation_date));
        self.count += 1;
        self.size_in_bytes += size_in_bytes;
    }

    /// Update the readable messages of the shared stream information.
    pub(crate) fn apply(&self, info: &SharedStreamInfo) {
        self.apply_to(&mut info.write());
    }

    /// Update the readable messages of the stream information.
    pub(crate) fn apply_to(&self, info: &mut crate::types::StreamInfo) {
        let (Some(first), Some(last)) = (self.first, self.last) else {
            return;
        };
        let readable = &mut info.readable_messages;
        if readable.cpt_messages == 0 {
            // first message ever of the stream
            readable.first_msg_id = first.0;
            readable.first_msg_timestamp = first.1;
        }
        readable.cpt_messages += self.count;
        readable.size_in_bytes += self.size_in_bytes;
        readable.last_msg_id = last.0;
        readable.last_msg_timestamp = last.1;
        info.last_update = Utc::now();
    }
}

/// Read the records of a stream from a position.
#[async_trait]
pub trait StreamIteratorHandler: Send {
    async fn open(&mut self) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
    /// Set the initial position of the iterator.
    async fn seek(&mut self, request: &StreamIteratorRequest) -> Result<()>;
    /// Read the next record (an error means the iterator cannot continue).
    async fn get_next_record(&mut self) -> Result<NextRecord>;
}

#[async_trait]
pub trait StorageProvider: Send + Sync {
    /// Name of the storage provider type.
    fn name(&self) -> &'static str;
    async fn init(&self) -> Result<()>;
    async fn stop(&self) -> Result<()>;
    fn stream_exists(&self, stream_uuid: &StreamUuid) -> bool;
    /// Generate a new stream uuid which is not used yet.
    fn generate_new_stream_uuid(&self) -> StreamUuid {
        loop {
            let candidate = Uuid::new_v4();
            if !self.stream_exists(&candidate) {
                return candidate;
            }
        }
    }
    async fn save_stream_catalog(&self) -> Result<()>;
    /// Load the catalog of streams.
    async fn load_streams(&self) -> Result<Vec<SharedStreamInfo>>;
    async fn on_create_stream(&self, info: &SharedStreamInfo) -> Result<()>;
    /// Persist the properties of a stream after they have been changed.
    async fn on_update_stream_properties(&self, info: &SharedStreamInfo) -> Result<()>;
    fn get_stream_info(&self, stream_uuid: &StreamUuid) -> Result<SharedStreamInfo>;
    /// Build or rebuild the index of a stream and return statistics about the index.
    async fn build_index(&self, stream_uuid: &StreamUuid) -> Result<Value>;
    async fn new_stream_iterator_handler(
        &self,
        stream_uuid: &StreamUuid,
        iterator_uuid: &StreamIteratorUuid,
    ) -> Result<Box<dyn StreamIteratorHandler>>;
    async fn new_stream_writer(&self, info: &SharedStreamInfo) -> Result<Box<dyn StreamWriter>>;
    async fn delete_stream(&self, stream_uuid: &StreamUuid) -> Result<()>;
}

/// Create the storage provider configured by `storage.type`.
pub fn new_storage_provider(config: &Config, logger: Logger) -> Result<Arc<dyn StorageProvider>> {
    let storage_type = config.storage.storage_type.as_str();
    match storage_type.to_ascii_lowercase().as_str() {
        "inmemory" => Ok(Arc::new(inmemory::InMemoryStorage::new(config, logger)?)),
        "jsonfile" => Ok(Arc::new(jsonfile::JsonFileStorage::new(config, logger)?)),
        #[cfg(feature = "mysql")]
        "mysql" => Ok(Arc::new(mysql::MySqlStorage::new(config, logger)?)),
        #[cfg(not(feature = "mysql"))]
        "mysql" => Err(Error::msg(
            "error creating storage provider: MySQL support is not compiled in (cargo feature \"mysql\")",
        )),
        _ => Err(Error::msg(format!(
            "error creating storage provider. No such storage provider type exists: '{}' (allowed values: {})",
            storage_type,
            STORAGE_PROVIDER_TYPES.join(", ")
        ))),
    }
}
