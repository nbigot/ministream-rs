//! Ingestion buffer: records are buffered and written in bulk by the stream writer.

use std::time::Duration;

use crate::error::Result;
use crate::log_error;
use crate::logger::Logger;
use crate::storage::StreamWriter;
use crate::types::{DeferedStreamRecord, StreamUuid};

/// After this count of consecutive failed flushes, the buffered records are dropped
/// (so that the buffer cannot grow forever when the storage keeps failing).
const MAX_FLUSH_ATTEMPTS: u32 = 5;

pub struct StreamIngestBuffer {
    stream_uuid: StreamUuid,
    bulk_flush_frequency: Duration,
    bulk_max_size: usize,
    buffer: Vec<DeferedStreamRecord>,
    writer: Box<dyn StreamWriter>,
    consecutive_failures: u32,
    logger: Logger,
}

impl StreamIngestBuffer {
    pub fn new(
        stream_uuid: StreamUuid,
        bulk_flush_frequency: Duration,
        bulk_max_size: usize,
        writer: Box<dyn StreamWriter>,
        logger: Logger,
    ) -> StreamIngestBuffer {
        let bulk_max_size = bulk_max_size.max(1);
        StreamIngestBuffer {
            stream_uuid,
            bulk_flush_frequency,
            bulk_max_size,
            buffer: Vec::with_capacity(bulk_max_size.min(10_000)),
            writer,
            consecutive_failures: 0,
            logger,
        }
    }

    pub fn bulk_flush_frequency(&self) -> Duration {
        self.bulk_flush_frequency
    }

    pub fn push(&mut self, record: DeferedStreamRecord) {
        self.buffer.push(record);
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.buffer.len() >= self.bulk_max_size
    }

    /// Write the buffered records, returns the count of records written.
    pub async fn flush(&mut self) -> Result<usize> {
        if self.buffer.is_empty() {
            return Ok(0);
        }
        let before = self.buffer.len();
        let result = self.writer.write(&mut self.buffer).await;
        let written = before - self.buffer.len();
        match result {
            Ok(()) => {
                self.consecutive_failures = 0;
                Ok(written)
            }
            Err(e) => {
                self.consecutive_failures += 1;
                if self.consecutive_failures >= MAX_FLUSH_ATTEMPTS && !self.buffer.is_empty() {
                    log_error!(self.logger, "Records dropped after too many failed writes", "topic" => "stream", "method" => "flush", "stream.uuid" => self.stream_uuid, "records.cpt" => self.buffer.len(), "error" => e.to_string());
                    self.buffer.clear();
                    self.consecutive_failures = 0;
                }
                Err(e)
            }
        }
    }

    pub async fn close(&mut self) -> Result<()> {
        self.writer.close().await
    }
}
