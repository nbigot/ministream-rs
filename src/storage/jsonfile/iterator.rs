//! Records iterator of the JSON file storage provider.

use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use super::index::{timestamp_nanos, IndexReader};
use super::StreamFiles;
use crate::error::{Error, Result};
use crate::log_error;
use crate::logger::Logger;
use crate::storage::{NextRecord, StreamIteratorHandler};
use crate::types::{IteratorType, StreamIteratorRequest, StreamIteratorUuid};

pub struct StreamIteratorHandlerFile {
    files: Arc<StreamFiles>,
    iterator_uuid: StreamIteratorUuid,
    logger: Logger,
    reader: Option<BufReader<File>>,
    /// Offset in the data file of the next record to read.
    offset: u64,
    line: Vec<u8>,
}

impl StreamIteratorHandlerFile {
    pub fn new(files: Arc<StreamFiles>, iterator_uuid: StreamIteratorUuid, logger: Logger) -> Self {
        StreamIteratorHandlerFile {
            files,
            iterator_uuid,
            logger,
            reader: None,
            offset: 0,
            line: Vec::new(),
        }
    }

    /// Compute the offset in the data file of the first record to read.
    fn seek_offset(&self, request: &StreamIteratorRequest) -> Result<u64> {
        let _guard = self.files.io_lock.lock();
        let mut index = IndexReader::open(&self.files.index)?;
        let end = index.last()?.map(|row| row.end()).unwrap_or(0);
        let offset = match request.iterator_type {
            IteratorType::FirstMessage => 0,
            IteratorType::LastMessage => index.last()?.map(|row| row.offset).unwrap_or(0),
            IteratorType::AfterLastMessage => end,
            IteratorType::AtMessageId => index
                .find_message_id(request.message_id)?
                .map(|row| row.offset)
                .ok_or_else(|| Error::not_found("message id not found"))?,
            IteratorType::AfterMessageId => {
                let last_id = index.last()?.map(|row| row.id).unwrap_or(0);
                if request.message_id > last_id {
                    return Err(Error::not_found("message id not found"));
                }
                index
                    .first_after_message_id(request.message_id)?
                    .map(|row| row.offset)
                    .unwrap_or(end)
            }
            IteratorType::AtTimestamp => {
                let timestamp = request
                    .timestamp
                    .ok_or_else(|| Error::msg("missing timestamp"))?;
                index
                    .first_at_or_after_timestamp(timestamp_nanos(&timestamp))?
                    .map(|row| row.offset)
                    .unwrap_or(end)
            }
        };
        Ok(offset)
    }
}

#[async_trait]
impl StreamIteratorHandler for StreamIteratorHandlerFile {
    async fn open(&mut self) -> Result<()> {
        let file = File::open(&self.files.data)?;
        self.reader = Some(BufReader::with_capacity(256 * 1024, file));
        self.offset = 0;
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.reader = None;
        Ok(())
    }

    async fn seek(&mut self, request: &StreamIteratorRequest) -> Result<()> {
        let offset = self.seek_offset(request)?;
        let reader = self
            .reader
            .as_mut()
            .ok_or_else(|| Error::msg("iterator is not opened"))?;
        reader.seek(SeekFrom::Start(offset))?;
        self.offset = offset;
        Ok(())
    }

    async fn get_next_record(&mut self) -> Result<NextRecord> {
        let reader = self
            .reader
            .as_mut()
            .ok_or_else(|| Error::msg("iterator is not opened"))?;
        self.line.clear();
        let n = reader.read_until(b'\n', &mut self.line)? as u64;
        if n == 0 {
            // end of the file reached
            return Ok(NextRecord::End);
        }
        if self.line.last() != Some(&b'\n') {
            // the record is being written: read it again later
            reader.seek(SeekFrom::Start(self.offset))?;
            return Ok(NextRecord::End);
        }
        self.offset += n;

        match serde_json::from_slice::<Value>(&self.line) {
            Ok(envelope) => match envelope.get("i").and_then(Value::as_u64) {
                Some(id) => Ok(NextRecord::Record(id, envelope)),
                None => Ok(NextRecord::Invalid(None, Error::msg("record without id"))),
            },
            Err(e) => {
                log_error!(self.logger, "json format error", "topic" => "streamiterator", "method" => "GetNextRecord", "stream.uuid" => self.files.stream_uuid, "it.uuid" => self.iterator_uuid, "line" => String::from_utf8_lossy(&self.line), "error" => e.to_string());
                Ok(NextRecord::Invalid(None, e.into()))
            }
        }
    }
}
