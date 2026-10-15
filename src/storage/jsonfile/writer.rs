//! Stream writer of the JSON file storage provider.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::Arc;

use async_trait::async_trait;

use super::index::{self, timestamp_nanos, IndexRow};
use super::StreamFiles;
use crate::error::{Error, Result};
use crate::logger::Logger;
use crate::storage::{StreamWriter, WrittenBatch};
use crate::types::{DeferedStreamRecord, SharedStreamInfo};
use crate::{log_debug, log_error, log_warn};

pub struct StreamWriterFile {
    files: Arc<StreamFiles>,
    info: SharedStreamInfo,
    logger: Logger,
    log_verbosity: i64,
    data: Option<File>,
    index: Option<File>,
    /// Current size of the data file.
    data_offset: u64,
}

impl StreamWriterFile {
    pub fn new(
        files: Arc<StreamFiles>,
        info: SharedStreamInfo,
        logger: Logger,
        log_verbosity: i64,
    ) -> StreamWriterFile {
        StreamWriterFile {
            files,
            info,
            logger,
            log_verbosity,
            data: None,
            index: None,
            data_offset: 0,
        }
    }
}

fn open_append(path: &std::path::Path) -> std::io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

#[async_trait]
impl StreamWriter for StreamWriterFile {
    async fn init(&mut self) -> Result<()> {
        let files = &self.files;
        std::fs::create_dir_all(&files.directory)?;
        // ensure the data and index files exist
        open_append(&files.data)?;
        open_append(&files.index)?;

        {
            let _guard = files.io_lock.lock();
            let removed = index::truncate_incomplete_line(&files.data)?;
            if removed > 0 {
                log_warn!(self.logger, "Incomplete record removed from the data file", "topic" => "stream", "method" => "Init", "stream.uuid" => files.stream_uuid, "bytes" => removed);
            }
            if !index::is_index_consistent(&files.data, &files.index)? {
                log_warn!(self.logger, "Stream index is not consistent with the data file, rebuilding it", "topic" => "stream", "method" => "Init", "stream.uuid" => files.stream_uuid);
                index::build_index(&files.data, &files.index, &self.logger)?;
            }
        }

        // if the stream information file does not exist then create it for the first time
        if !files.meta.exists() {
            files.save_meta(&self.info)?;
        }
        Ok(())
    }

    async fn open(&mut self) -> Result<()> {
        if self.data.is_some() {
            return Err(Error::msg(
                "cannot open stream writer file because it's already opened",
            ));
        }
        let data = open_append(&self.files.data).inspect_err(|e| {
            log_error!(self.logger, "can't open data file", "topic" => "stream", "method" => "Open", "stream.uuid" => self.files.stream_uuid, "filename" => self.files.data.display().to_string(), "error" => e.to_string());
        })?;
        let index = open_append(&self.files.index).inspect_err(|e| {
            log_error!(self.logger, "can't open index file", "topic" => "stream", "method" => "Open", "stream.uuid" => self.files.stream_uuid, "filename" => self.files.index.display().to_string(), "error" => e.to_string());
        })?;
        self.data_offset = data.metadata()?.len();
        self.data = Some(data);
        self.index = Some(index);
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        let (Some(data), Some(index)) = (self.data.take(), self.index.take()) else {
            return Err(Error::msg(
                "cannot close stream writer file because it's not opened",
            ));
        };
        data.sync_all()?;
        index.sync_all()?;
        log_debug!(self.logger, "Stream writer file closed", "topic" => "streamwriter", "method" => "close", "stream.uuid" => self.files.stream_uuid);
        Ok(())
    }

    async fn write(&mut self, records: &mut Vec<DeferedStreamRecord>) -> Result<()> {
        let (Some(data), Some(index)) = (self.data.as_mut(), self.index.as_mut()) else {
            return Err(Error::msg(
                "cannot write to stream writer file because it's not opened",
            ));
        };
        if records.is_empty() {
            return Ok(());
        }
        if self.log_verbosity > 0 {
            log_debug!(self.logger, "write records into file", "topic" => "stream", "method" => "Write", "stream.uuid" => self.files.stream_uuid, "records.cpt" => records.len());
        }

        // serialize the whole batch, then append it with a single write
        let mut data_buffer = Vec::with_capacity(records.len() * 128);
        let mut index_buffer = Vec::with_capacity(records.len() * index::ROW_SIZE as usize);
        let mut batch = WrittenBatch::default();
        for record in records.iter() {
            if self.log_verbosity > 1 {
                log_debug!(self.logger, "write record into file", "topic" => "stream", "method" => "Write", "stream.uuid" => self.files.stream_uuid, "msg" => record);
            }
            let start = data_buffer.len() as u64;
            serde_json::to_writer(&mut data_buffer, record)?;
            data_buffer.push(b'\n');
            let length = data_buffer.len() as u64 - start;
            let row = IndexRow {
                id: record.id,
                length,
                offset: self.data_offset + start,
                timestamp_nanos: timestamp_nanos(&record.creation_date),
            };
            index_buffer.extend_from_slice(&row.to_bytes());
            batch.add(record, length);
        }

        {
            let _guard = self.files.io_lock.lock();
            let index_len = index.metadata()?.len();
            let result = data
                .write_all(&data_buffer)
                .and_then(|_| index.write_all(&index_buffer));
            if let Err(e) = result {
                // best effort rollback of a partially written batch
                // (the next initialization repairs the files anyway)
                let _ = data.set_len(self.data_offset);
                let _ = index.set_len(index_len);
                log_error!(self.logger, "can't write records", "topic" => "stream", "method" => "Write", "stream.uuid" => self.files.stream_uuid, "error" => e.to_string());
                return Err(e.into());
            }
        }
        self.data_offset += data_buffer.len() as u64;
        records.clear();
        batch.apply(&self.info);

        if let Err(e) = self.files.save_meta(&self.info) {
            // the records are persisted, only the cached information could not be saved
            log_error!(self.logger, "can't save stream information", "topic" => "stream", "method" => "Write", "stream.uuid" => self.files.stream_uuid, "error" => e.to_string());
        }
        Ok(())
    }
}
