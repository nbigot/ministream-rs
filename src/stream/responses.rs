//! Responses of the stream API.

use serde::Serialize;
use serde_json::Value;

use crate::types::{MessageId, StreamIteratorUuid, StreamUuid};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetStreamRecordsResponse {
    pub status: String,
    /// Duration of the request in milliseconds.
    pub duration: i64,
    pub count: i64,
    pub count_errors: i64,
    pub count_skipped: i64,
    /// True when the maximum count of records was reached (more records may be available).
    pub remain: bool,
    pub last_record_id_read: MessageId,
    #[serde(rename = "streamUUID")]
    pub stream_uuid: StreamUuid,
    #[serde(rename = "streamIteratorUUID")]
    pub stream_iterator_uuid: StreamIteratorUuid,
    pub records: Vec<Value>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateRecordsIteratorResponse {
    pub status: String,
    pub message: String,
    #[serde(rename = "streamUUID")]
    pub stream_uuid: StreamUuid,
    #[serde(rename = "streamIteratorUUID")]
    pub stream_iterator_uuid: StreamIteratorUuid,
}

pub type CloseRecordsIteratorResponse = CreateRecordsIteratorResponse;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetRecordsIteratorStatsResponse {
    pub status: String,
    pub message: String,
    #[serde(rename = "streamUUID")]
    pub stream_uuid: StreamUuid,
    #[serde(rename = "streamIteratorUUID")]
    pub stream_iterator_uuid: StreamIteratorUuid,
    pub last_record_id_read: MessageId,
    pub name: String,
    pub stats: super::iterator::StreamIteratorStats,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PutStreamRecordsResponse {
    pub status: String,
    #[serde(rename = "streamUUID")]
    pub stream_uuid: StreamUuid,
    pub duration: i64,
    pub count: i64,
    pub message_ids: Vec<MessageId>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LoginResponse {
    pub status: String,
    pub message: String,
    pub jwt: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RebuildStreamIndexResponse {
    pub status: String,
    pub message: String,
    #[serde(rename = "streamUUID")]
    pub stream_uuid: StreamUuid,
    pub duration: i64,
    pub index_stats: Value,
}
