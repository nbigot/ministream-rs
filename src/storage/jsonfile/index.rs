//! Binary index of a JSON lines stream data file.
//!
//! The index file is a sequence of 32 bytes rows (little endian), one per record:
//! `message id (u64) | length in bytes (i64) | offset in the data file (i64) | timestamp in ns (i64)`.
//! This is the format produced by the index rebuild of the Go implementation.

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::logger::Logger;
use crate::types::{zero_time, MessageId};
use crate::{log_info, log_warn};

pub const ROW_SIZE: u64 = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexRow {
    pub id: MessageId,
    pub length: u64,
    pub offset: u64,
    pub timestamp_nanos: i64,
}

impl IndexRow {
    pub fn to_bytes(&self) -> [u8; ROW_SIZE as usize] {
        let mut bytes = [0u8; ROW_SIZE as usize];
        bytes[0..8].copy_from_slice(&self.id.to_le_bytes());
        bytes[8..16].copy_from_slice(&(self.length as i64).to_le_bytes());
        bytes[16..24].copy_from_slice(&(self.offset as i64).to_le_bytes());
        bytes[24..32].copy_from_slice(&self.timestamp_nanos.to_le_bytes());
        bytes
    }

    pub fn from_bytes(bytes: &[u8; ROW_SIZE as usize]) -> IndexRow {
        let field = |i: usize| -> [u8; 8] { bytes[i..i + 8].try_into().expect("8 bytes") };
        IndexRow {
            id: u64::from_le_bytes(field(0)),
            length: i64::from_le_bytes(field(8)).max(0) as u64,
            offset: i64::from_le_bytes(field(16)).max(0) as u64,
            timestamp_nanos: i64::from_le_bytes(field(24)),
        }
    }

    /// Offset of the end of the record in the data file.
    pub fn end(&self) -> u64 {
        self.offset + self.length
    }
}

pub fn timestamp_nanos(t: &DateTime<Utc>) -> i64 {
    t.timestamp_nanos_opt().unwrap_or(i64::MIN)
}

/// Random access reader of an index file.
pub struct IndexReader {
    file: File,
    rows: u64,
}

impl IndexReader {
    pub fn open(path: &Path) -> Result<IndexReader> {
        let file = File::open(path)?;
        let rows = file.metadata()?.len() / ROW_SIZE;
        Ok(IndexReader { file, rows })
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    pub fn row(&mut self, n: u64) -> Result<IndexRow> {
        let mut bytes = [0u8; ROW_SIZE as usize];
        self.file.seek(SeekFrom::Start(n * ROW_SIZE))?;
        self.file.read_exact(&mut bytes)?;
        Ok(IndexRow::from_bytes(&bytes))
    }

    pub fn last(&mut self) -> Result<Option<IndexRow>> {
        match self.rows {
            0 => Ok(None),
            n => self.row(n - 1).map(Some),
        }
    }

    /// Index of the first row for which the predicate is false
    /// (the rows must be partitioned according to the predicate).
    fn partition_point(&mut self, mut predicate: impl FnMut(&IndexRow) -> bool) -> Result<u64> {
        let (mut low, mut high) = (0u64, self.rows);
        while low < high {
            let middle = low + (high - low) / 2;
            if predicate(&self.row(middle)?) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        Ok(low)
    }

    fn row_or_none(&mut self, n: u64) -> Result<Option<IndexRow>> {
        if n < self.rows {
            self.row(n).map(Some)
        } else {
            Ok(None)
        }
    }

    /// Row of the record having the given id.
    pub fn find_message_id(&mut self, id: MessageId) -> Result<Option<IndexRow>> {
        let n = self.partition_point(|row| row.id < id)?;
        Ok(self.row_or_none(n)?.filter(|row| row.id == id))
    }

    /// Row of the first record having an id greater than the given id.
    pub fn first_after_message_id(&mut self, id: MessageId) -> Result<Option<IndexRow>> {
        let n = self.partition_point(|row| row.id <= id)?;
        self.row_or_none(n)
    }

    /// Row of the first record created at or after the given timestamp.
    pub fn first_at_or_after_timestamp(&mut self, nanos: i64) -> Result<Option<IndexRow>> {
        let n = self.partition_point(|row| row.timestamp_nanos < nanos)?;
        self.row_or_none(n)
    }
}

/// Statistics returned by an index rebuild.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct IndexStats {
    pub cpt_messages: u64,
    pub file_size: u64,
    pub first_msg_id: MessageId,
    pub last_msg_id: MessageId,
    pub first_msg_timestamp: DateTime<Utc>,
    pub last_msg_timestamp: DateTime<Utc>,
    pub invalid_lines: u64,
}

#[derive(Deserialize)]
struct RecordHeader {
    #[serde(rename = "i")]
    id: MessageId,
    #[serde(rename = "d")]
    creation_date: DateTime<Utc>,
}

/// Build (or rebuild) the index file from the data file.
///
/// Lines that cannot be decoded are skipped (and counted), an incomplete last line
/// (a record being written) is ignored.
pub fn build_index(data_path: &Path, index_path: &Path, logger: &Logger) -> Result<IndexStats> {
    log_info!(logger, "Build index started", "topic" => "index", "method" => "BuildIndex", "index.filename" => index_path.display().to_string());

    let data = File::open(data_path)?;
    let mut reader = BufReader::with_capacity(1024 * 1024, data);
    let mut index = BufWriter::new(
        std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(index_path)?,
    );

    let mut stats = IndexStats {
        cpt_messages: 0,
        file_size: 0,
        first_msg_id: 0,
        last_msg_id: 0,
        first_msg_timestamp: zero_time(),
        last_msg_timestamp: zero_time(),
        invalid_lines: 0,
    };
    let mut offset = 0u64;
    let mut line = Vec::with_capacity(4096);
    loop {
        line.clear();
        let n = reader.read_until(b'\n', &mut line)? as u64;
        if n == 0 {
            break;
        }
        if line.last() != Some(&b'\n') {
            log_warn!(logger, "Incomplete last line ignored", "topic" => "index", "method" => "BuildIndex", "offset" => offset);
            break;
        }
        match serde_json::from_slice::<RecordHeader>(&line) {
            Ok(header) => {
                let row = IndexRow {
                    id: header.id,
                    length: n,
                    offset,
                    timestamp_nanos: timestamp_nanos(&header.creation_date),
                };
                index.write_all(&row.to_bytes())?;
                if stats.cpt_messages == 0 {
                    stats.first_msg_id = header.id;
                    stats.first_msg_timestamp = header.creation_date;
                }
                stats.last_msg_id = header.id;
                stats.last_msg_timestamp = header.creation_date;
                stats.cpt_messages += 1;
            }
            Err(e) => {
                stats.invalid_lines += 1;
                log_warn!(logger, "Invalid record skipped", "topic" => "index", "method" => "BuildIndex", "offset" => offset, "error" => e.to_string());
            }
        }
        offset += n;
    }
    stats.file_size = offset;
    index.flush()?;
    index.get_ref().sync_all()?;

    log_info!(logger, "Build index ended", "topic" => "index", "method" => "BuildIndex",
        "index.filename" => index_path.display().to_string(),
        "index.byteSize" => stats.file_size,
        "index.rowsCount" => stats.cpt_messages,
        "index.firstMsgId" => stats.first_msg_id,
        "index.lastMsgId" => stats.last_msg_id,
        "index.invalidLines" => stats.invalid_lines);
    Ok(stats)
}

/// Check that the index file matches the data file (same records count can't be checked
/// cheaply, but the last row must end exactly at the end of the data file).
pub fn is_index_consistent(data_path: &Path, index_path: &Path) -> Result<bool> {
    let data_len = std::fs::metadata(data_path)?.len();
    let index_len = std::fs::metadata(index_path)?.len();
    if index_len % ROW_SIZE != 0 {
        return Ok(false);
    }
    let mut reader = IndexReader::open(index_path)?;
    match reader.last()? {
        None => Ok(data_len == 0),
        Some(row) => Ok(row.end() == data_len),
    }
}

/// Remove an incomplete last line from the data file (crash while writing).
/// Returns the count of bytes removed.
pub fn truncate_incomplete_line(data_path: &Path) -> Result<u64> {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(data_path)?;
    let len = file.metadata()?.len();
    if len == 0 {
        return Ok(0);
    }
    let mut last = [0u8; 1];
    file.seek(SeekFrom::Start(len - 1))?;
    file.read_exact(&mut last)?;
    if last[0] == b'\n' {
        return Ok(0);
    }
    // search backward the end of the last complete line
    let mut end = 0u64;
    let mut position = len;
    let mut buffer = vec![0u8; 64 * 1024];
    while position > 0 {
        let chunk = (buffer.len() as u64).min(position);
        position -= chunk;
        file.seek(SeekFrom::Start(position))?;
        file.read_exact(&mut buffer[..chunk as usize])?;
        if let Some(i) = buffer[..chunk as usize].iter().rposition(|b| *b == b'\n') {
            end = position + i as u64 + 1;
            break;
        }
    }
    file.set_len(end)?;
    file.sync_all()?;
    Ok(len - end)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_rows(path: &Path, rows: &[IndexRow]) {
        let bytes: Vec<u8> = rows.iter().flat_map(|r| r.to_bytes()).collect();
        std::fs::write(path, bytes).unwrap();
    }

    #[test]
    fn row_bytes_roundtrip() {
        let row = IndexRow {
            id: 7,
            length: 42,
            offset: 1000,
            timestamp_nanos: -5,
        };
        assert_eq!(IndexRow::from_bytes(&row.to_bytes()), row);
    }

    #[test]
    fn searches() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.bin");
        let rows: Vec<IndexRow> = (1..=10)
            .map(|i| IndexRow {
                id: i * 2,
                length: 10,
                offset: (i - 1) * 10,
                timestamp_nanos: i as i64 * 100,
            })
            .collect();
        write_rows(&path, &rows);
        let mut reader = IndexReader::open(&path).unwrap();
        assert_eq!(reader.rows(), 10);
        assert_eq!(reader.find_message_id(6).unwrap().unwrap().offset, 20);
        assert!(reader.find_message_id(7).unwrap().is_none());
        assert!(reader.find_message_id(100).unwrap().is_none());
        assert_eq!(reader.first_after_message_id(6).unwrap().unwrap().id, 8);
        assert_eq!(reader.first_after_message_id(7).unwrap().unwrap().id, 8);
        assert!(reader.first_after_message_id(20).unwrap().is_none());
        assert_eq!(
            reader.first_at_or_after_timestamp(250).unwrap().unwrap().id,
            6
        );
        assert_eq!(
            reader.first_at_or_after_timestamp(300).unwrap().unwrap().id,
            6
        );
        assert_eq!(
            reader.first_at_or_after_timestamp(0).unwrap().unwrap().id,
            2
        );
        assert!(reader.first_at_or_after_timestamp(5000).unwrap().is_none());
        assert_eq!(reader.last().unwrap().unwrap().id, 20);
    }

    #[test]
    fn build_and_check() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data.jsonl");
        let index = dir.path().join("index.bin");
        let lines = concat!(
            "{\"i\":1,\"d\":\"2023-01-01T00:00:00Z\",\"m\":{\"a\":1}}\n",
            "not json\n",
            "{\"i\":2,\"d\":\"2023-01-01T00:00:01+01:00\",\"m\":[1,2]}\n",
            "{\"i\":3,\"d\":\"2023-01-01T00:00:02Z\",\"m\":"
        );
        std::fs::write(&data, lines).unwrap();
        std::fs::write(&index, b"garbage").unwrap();
        assert!(!is_index_consistent(&data, &index).unwrap());

        let removed = truncate_incomplete_line(&data).unwrap();
        assert_eq!(
            removed,
            "{\"i\":3,\"d\":\"2023-01-01T00:00:02Z\",\"m\":".len() as u64
        );

        let stats = build_index(&data, &index, &Logger::nop()).unwrap();
        assert_eq!(stats.cpt_messages, 2);
        assert_eq!(stats.invalid_lines, 1);
        assert_eq!(stats.first_msg_id, 1);
        assert_eq!(stats.last_msg_id, 2);
        assert_eq!(stats.file_size, std::fs::metadata(&data).unwrap().len());
        assert!(is_index_consistent(&data, &index).unwrap());

        let mut reader = IndexReader::open(&index).unwrap();
        let second = reader.find_message_id(2).unwrap().unwrap();
        let content = std::fs::read(&data).unwrap();
        let line = &content[second.offset as usize..second.end() as usize];
        assert!(line.starts_with(b"{\"i\":2"));
        assert!(line.ends_with(b"\n"));
    }

    #[test]
    fn truncate_without_any_newline() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data.jsonl");
        std::fs::write(&data, "partial").unwrap();
        assert_eq!(truncate_incomplete_line(&data).unwrap(), 7);
        assert_eq!(std::fs::metadata(&data).unwrap().len(), 0);
        assert_eq!(truncate_incomplete_line(&data).unwrap(), 0);
    }
}
