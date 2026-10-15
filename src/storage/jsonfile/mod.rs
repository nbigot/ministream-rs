//! JSON file storage provider.
//!
//! Layout of the data directory:
//!
//! ```text
//! <dataDirectory>/streams.json                       catalog of streams {"streams": [uuid, ...]}
//! <dataDirectory>/streams/<uuid>/stream.json         stream information
//! <dataDirectory>/streams/<uuid>/data.jsonl          records, one JSON envelope per line
//! <dataDirectory>/streams/<uuid>/index.bin           binary index of the records
//! ```

pub mod index;
mod iterator;
mod writer;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::catalog::Catalog;
use super::{StorageProvider, StreamIteratorHandler, StreamWriter};
use crate::config::Config;
use crate::error::{Error, Result};
use crate::logger::Logger;
use crate::types::{SharedStreamInfo, StreamInfo, StreamIteratorUuid, StreamUuid};
use crate::{log_error, log_info};

/// Paths of the files of a stream, and the locks protecting them.
pub struct StreamFiles {
    pub stream_uuid: StreamUuid,
    pub directory: PathBuf,
    pub data: PathBuf,
    pub index: PathBuf,
    pub meta: PathBuf,
    /// Serializes the writes of the data and index files with the index rebuilds and reads.
    pub io_lock: Mutex<()>,
    /// Serializes the writes of the stream information file.
    meta_lock: Mutex<()>,
}

impl StreamFiles {
    fn new(streams_directory: &Path, stream_uuid: StreamUuid) -> StreamFiles {
        let directory = streams_directory.join(stream_uuid.to_string());
        StreamFiles {
            stream_uuid,
            data: directory.join("data.jsonl"),
            index: directory.join("index.bin"),
            meta: directory.join("stream.json"),
            directory,
            io_lock: Mutex::new(()),
            meta_lock: Mutex::new(()),
        }
    }

    /// Save the stream information file (atomically).
    pub fn save_meta(&self, info: &SharedStreamInfo) -> Result<()> {
        let _guard = self.meta_lock.lock();
        let snapshot = info.read().clone();
        let json = serde_json::to_vec(&snapshot)?;
        write_file_atomically(&self.meta, &json)
    }
}

/// Write a file through a temporary file, so that readers never see a partially written file.
fn write_file_atomically(path: &Path, content: &[u8]) -> Result<()> {
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(".tmp");
    let temporary = PathBuf::from(temporary);
    std::fs::write(&temporary, content)?;
    std::fs::rename(&temporary, path)?;
    Ok(())
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct CatalogDocument {
    #[serde(default, deserialize_with = "uuids_or_empty")]
    streams: Vec<StreamUuid>,
}

fn uuids_or_empty<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<StreamUuid>, D::Error> {
    Ok(Option::<Vec<StreamUuid>>::deserialize(d)?.unwrap_or_default())
}

/// JSON file storage provider.
pub struct JsonFileStorage {
    logger: Logger,
    log_verbosity: i64,
    data_directory: PathBuf,
    catalog: Catalog,
    files: Mutex<HashMap<StreamUuid, Arc<StreamFiles>>>,
    catalog_lock: Mutex<()>,
}

impl JsonFileStorage {
    pub fn new(config: &Config, logger: Logger) -> Result<JsonFileStorage> {
        let mut data_directory = config.storage.json_file.data_directory.trim().to_string();
        if data_directory.is_empty() {
            data_directory = config.data_directory.trim().to_string();
        }
        if data_directory.is_empty() {
            return Err(Error::msg(
                "configuration storage.jsonfile.dataDirectory is empty",
            ));
        }
        Ok(JsonFileStorage {
            logger,
            log_verbosity: config.storage.log_verbosity,
            data_directory: PathBuf::from(data_directory),
            catalog: Catalog::default(),
            files: Mutex::new(HashMap::new()),
            catalog_lock: Mutex::new(()),
        })
    }

    pub fn data_directory(&self) -> &Path {
        &self.data_directory
    }

    fn streams_directory(&self) -> PathBuf {
        self.data_directory.join("streams")
    }

    fn catalog_path(&self) -> PathBuf {
        self.data_directory.join("streams.json")
    }

    fn stream_files(&self, stream_uuid: &StreamUuid) -> Arc<StreamFiles> {
        self.files
            .lock()
            .entry(*stream_uuid)
            .or_insert_with(|| Arc::new(StreamFiles::new(&self.streams_directory(), *stream_uuid)))
            .clone()
    }

    fn load_stream_info(&self, stream_uuid: &StreamUuid) -> Result<StreamInfo> {
        log_info!(self.logger, "Loading stream", "topic" => "stream", "method" => "LoadStreamFromUUID", "stream.uuid" => stream_uuid);
        let path = self.stream_files(stream_uuid).meta.clone();
        let content = std::fs::read(&path)
            .map_err(|e| Error::msg(format!("can't open stream file {}: {e}", path.display())))?;
        let info: StreamInfo = serde_json::from_slice(&content).map_err(|e| {
            Error::msg(format!(
                "can't decode json stream file {}: {e}",
                path.display()
            ))
        })?;
        if info.uuid != *stream_uuid {
            return Err(Error::msg(format!(
                "stream file {} has an unexpected uuid {}",
                path.display(),
                info.uuid
            )));
        }
        Ok(info)
    }

    fn write_catalog(&self) -> Result<()> {
        let _guard = self.catalog_lock.lock();
        let document = CatalogDocument {
            streams: self.catalog.uuids(),
        };
        write_file_atomically(&self.catalog_path(), &serde_json::to_vec(&document)?)
    }
}

#[async_trait]
impl StorageProvider for JsonFileStorage {
    fn name(&self) -> &'static str {
        "JSONFile"
    }

    async fn init(&self) -> Result<()> {
        std::fs::create_dir_all(&self.data_directory).map_err(|e| {
            Error::msg(format!(
                "cannot create data directory {}: {e}",
                self.data_directory.display()
            ))
        })?;
        std::fs::create_dir_all(self.streams_directory())?;
        self.catalog.clear();
        self.files.lock().clear();
        if !self.catalog_path().exists() {
            // create a file having an empty catalog of streams
            self.write_catalog()?;
        }
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        Ok(())
    }

    fn stream_exists(&self, stream_uuid: &StreamUuid) -> bool {
        self.catalog.contains(stream_uuid)
    }

    async fn save_stream_catalog(&self) -> Result<()> {
        log_info!(self.logger, "Save streams", "topic" => "stream", "method" => "SaveStreamCatalog", "filename" => self.catalog_path().display().to_string());
        self.write_catalog().inspect_err(|e| {
            log_error!(self.logger, "Can't save streams", "topic" => "stream", "method" => "SaveStreamCatalog", "error" => e.to_string());
        })
    }

    async fn load_streams(&self) -> Result<Vec<SharedStreamInfo>> {
        let path = self.catalog_path();
        log_info!(self.logger, "Loading streams", "topic" => "stream", "method" => "LoadStreamCatalog", "filename" => path.display().to_string());
        let content = std::fs::read(&path).map_err(|e| {
            Error::msg(format!("can't open stream catalog {}: {e}", path.display()))
        })?;
        let document: CatalogDocument = serde_json::from_slice(&content).map_err(|e| {
            Error::msg(format!(
                "can't decode stream catalog {}: {e}",
                path.display()
            ))
        })?;

        if document.streams.is_empty() {
            log_info!(self.logger, "No stream found", "topic" => "stream", "method" => "LoadStreams");
        } else {
            log_info!(self.logger, "Found streams", "topic" => "stream", "method" => "LoadStreams", "streams" => document.streams.len());
        }

        let mut infos = Vec::with_capacity(document.streams.len());
        for stream_uuid in &document.streams {
            let info = self.load_stream_info(stream_uuid).inspect_err(|e| {
                log_error!(self.logger, "Can't load stream", "topic" => "stream", "method" => "LoadStreamCatalog", "stream.uuid" => stream_uuid, "error" => e.to_string());
            })?;
            let info = info.into_shared();
            self.catalog.insert(info.clone());
            infos.push(info);
        }
        Ok(infos)
    }

    async fn on_create_stream(&self, info: &SharedStreamInfo) -> Result<()> {
        let uuid = info.read().uuid;
        let files = self.stream_files(&uuid);
        std::fs::create_dir_all(&files.directory)?;
        self.catalog.insert(info.clone());
        Ok(())
    }

    async fn on_update_stream_properties(&self, info: &SharedStreamInfo) -> Result<()> {
        let uuid = info.read().uuid;
        self.stream_files(&uuid).save_meta(info)
    }

    fn get_stream_info(&self, stream_uuid: &StreamUuid) -> Result<SharedStreamInfo> {
        self.catalog.get(stream_uuid)
    }

    async fn build_index(&self, stream_uuid: &StreamUuid) -> Result<Value> {
        if !self.stream_exists(stream_uuid) {
            return Err(Error::not_found(format!("stream not found {stream_uuid}")));
        }
        let files = self.stream_files(stream_uuid);
        let stats = {
            let _guard = files.io_lock.lock();
            index::build_index(&files.data, &files.index, &self.logger)?
        };
        Ok(serde_json::to_value(stats)?)
    }

    async fn new_stream_iterator_handler(
        &self,
        stream_uuid: &StreamUuid,
        iterator_uuid: &StreamIteratorUuid,
    ) -> Result<Box<dyn StreamIteratorHandler>> {
        if !self.stream_exists(stream_uuid) {
            return Err(Error::not_found(format!("stream not found {stream_uuid}")));
        }
        Ok(Box::new(iterator::StreamIteratorHandlerFile::new(
            self.stream_files(stream_uuid),
            *iterator_uuid,
            self.logger.clone(),
        )))
    }

    async fn new_stream_writer(&self, info: &SharedStreamInfo) -> Result<Box<dyn StreamWriter>> {
        let uuid = info.read().uuid;
        Ok(Box::new(writer::StreamWriterFile::new(
            self.stream_files(&uuid),
            info.clone(),
            self.logger.clone(),
            self.log_verbosity,
        )))
    }

    async fn delete_stream(&self, stream_uuid: &StreamUuid) -> Result<()> {
        let files = self.stream_files(stream_uuid);
        // on some platforms (Windows) files may still be released by other threads: retry a few times
        let mut attempt = 0;
        loop {
            match std::fs::remove_dir_all(&files.directory) {
                Ok(()) => break,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
                Err(e) if attempt >= 10 => return Err(e.into()),
                Err(_) => {
                    attempt += 1;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
        self.catalog.remove(stream_uuid);
        self.files.lock().remove(stream_uuid);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::NextRecord;
    use crate::types::{DeferedStreamRecord, IteratorType, MessageId, StreamIteratorRequest};
    use chrono::Utc;
    use serde_json::json;
    use uuid::Uuid;

    fn config(dir: &Path) -> Config {
        let mut config = Config::default();
        config.storage.json_file.data_directory = dir.to_string_lossy().to_string();
        config
    }

    fn request(iterator_type: IteratorType, message_id: MessageId) -> StreamIteratorRequest {
        StreamIteratorRequest {
            iterator_type,
            message_id,
            timestamp: None,
            jq_filter: String::new(),
            max_wait_time_seconds: 0,
            name: String::new(),
        }
    }

    fn records(ids: std::ops::RangeInclusive<u64>) -> Vec<DeferedStreamRecord> {
        ids.map(|id| DeferedStreamRecord {
            id,
            creation_date: Utc::now(),
            msg: json!({ "n": id, "text": "hello\nworld" }),
        })
        .collect()
    }

    async fn read_ids(
        storage: &JsonFileStorage,
        uuid: &StreamUuid,
        req: StreamIteratorRequest,
    ) -> Result<Vec<MessageId>> {
        let mut handler = storage
            .new_stream_iterator_handler(uuid, &Uuid::new_v4())
            .await?;
        handler.open().await?;
        handler.seek(&req).await?;
        let mut ids = Vec::new();
        loop {
            match handler.get_next_record().await? {
                NextRecord::Record(id, envelope) => {
                    assert_eq!(envelope["m"]["n"], id);
                    ids.push(id)
                }
                NextRecord::Invalid(_, e) => panic!("invalid record: {e}"),
                NextRecord::End => break,
            }
        }
        handler.close().await?;
        Ok(ids)
    }

    #[tokio::test]
    async fn persist_reload_and_iterate() {
        let dir = tempfile::tempdir().unwrap();
        let storage = JsonFileStorage::new(&config(dir.path()), Logger::nop()).unwrap();
        storage.init().await.unwrap();
        assert!(storage.load_streams().await.unwrap().is_empty());

        let mut info = StreamInfo::new(Uuid::new_v4());
        info.properties.insert("name".into(), json!("test"));
        let info = info.into_shared();
        let uuid = info.read().uuid;
        storage.on_create_stream(&info).await.unwrap();
        storage.save_stream_catalog().await.unwrap();

        let mut writer = storage.new_stream_writer(&info).await.unwrap();
        writer.init().await.unwrap();
        writer.open().await.unwrap();
        let mut batch = records(1..=3);
        writer.write(&mut batch).await.unwrap();
        assert!(batch.is_empty());
        writer.write(&mut records(4..=6)).await.unwrap();
        writer.close().await.unwrap();
        assert_eq!(info.read().readable_messages.cpt_messages, 6);

        // the index written by the writer is consistent with the data file
        let files = storage.stream_files(&uuid);
        assert!(index::is_index_consistent(&files.data, &files.index).unwrap());

        assert_eq!(
            read_ids(&storage, &uuid, request(IteratorType::FirstMessage, 0))
                .await
                .unwrap(),
            vec![1, 2, 3, 4, 5, 6]
        );
        assert_eq!(
            read_ids(&storage, &uuid, request(IteratorType::LastMessage, 0))
                .await
                .unwrap(),
            vec![6]
        );
        assert_eq!(
            read_ids(&storage, &uuid, request(IteratorType::AfterLastMessage, 0))
                .await
                .unwrap(),
            Vec::<u64>::new()
        );
        assert_eq!(
            read_ids(&storage, &uuid, request(IteratorType::AtMessageId, 4))
                .await
                .unwrap(),
            vec![4, 5, 6]
        );
        assert_eq!(
            read_ids(&storage, &uuid, request(IteratorType::AfterMessageId, 4))
                .await
                .unwrap(),
            vec![5, 6]
        );
        assert!(
            read_ids(&storage, &uuid, request(IteratorType::AtMessageId, 40))
                .await
                .is_err()
        );
        assert!(
            read_ids(&storage, &uuid, request(IteratorType::AfterMessageId, 40))
                .await
                .is_err()
        );

        // reload everything from the disk
        let storage = JsonFileStorage::new(&config(dir.path()), Logger::nop()).unwrap();
        storage.init().await.unwrap();
        let infos = storage.load_streams().await.unwrap();
        assert_eq!(infos.len(), 1);
        let reloaded = infos[0].read().clone();
        assert_eq!(reloaded.uuid, uuid);
        assert_eq!(reloaded.properties["name"], "test");
        assert_eq!(reloaded.readable_messages.cpt_messages, 6);

        // a broken index is detected and rebuilt when the writer starts
        std::fs::write(&files.index, b"broken").unwrap();
        let mut writer = storage.new_stream_writer(&infos[0]).await.unwrap();
        writer.init().await.unwrap();
        writer.open().await.unwrap();
        writer.write(&mut records(7..=7)).await.unwrap();
        assert_eq!(
            read_ids(&storage, &uuid, request(IteratorType::AtMessageId, 2))
                .await
                .unwrap(),
            vec![2, 3, 4, 5, 6, 7]
        );
        writer.close().await.unwrap();

        let stats = storage.build_index(&uuid).await.unwrap();
        assert_eq!(stats["CptMessages"], 7);
        assert_eq!(stats["LastMsgId"], 7);

        storage.delete_stream(&uuid).await.unwrap();
        storage.save_stream_catalog().await.unwrap();
        assert!(!files.directory.exists());
        let storage = JsonFileStorage::new(&config(dir.path()), Logger::nop()).unwrap();
        storage.init().await.unwrap();
        assert!(storage.load_streams().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn iterator_does_not_read_incomplete_lines() {
        let dir = tempfile::tempdir().unwrap();
        let storage = JsonFileStorage::new(&config(dir.path()), Logger::nop()).unwrap();
        storage.init().await.unwrap();
        let info = StreamInfo::new(Uuid::new_v4()).into_shared();
        let uuid = info.read().uuid;
        storage.on_create_stream(&info).await.unwrap();
        let mut writer = storage.new_stream_writer(&info).await.unwrap();
        writer.init().await.unwrap();
        writer.open().await.unwrap();
        writer.write(&mut records(1..=1)).await.unwrap();

        let mut handler = storage
            .new_stream_iterator_handler(&uuid, &Uuid::new_v4())
            .await
            .unwrap();
        handler.open().await.unwrap();
        handler
            .seek(&request(IteratorType::FirstMessage, 0))
            .await
            .unwrap();
        assert!(matches!(
            handler.get_next_record().await.unwrap(),
            NextRecord::Record(1, _)
        ));

        // simulate a record being written
        let files = storage.stream_files(&uuid);
        let mut data = std::fs::OpenOptions::new()
            .append(true)
            .open(&files.data)
            .unwrap();
        std::io::Write::write_all(&mut data, b"{\"i\":2,\"d\":\"2023-01-01T00:00:00Z\",").unwrap();
        assert!(matches!(
            handler.get_next_record().await.unwrap(),
            NextRecord::End
        ));
        std::io::Write::write_all(&mut data, b"\"m\":{\"n\":2}}\n").unwrap();
        assert!(matches!(
            handler.get_next_record().await.unwrap(),
            NextRecord::Record(2, _)
        ));
        assert!(matches!(
            handler.get_next_record().await.unwrap(),
            NextRecord::End
        ));
    }

    #[test]
    fn data_directory_is_required() {
        assert!(JsonFileStorage::new(&Config::default(), Logger::nop()).is_err());
    }
}
