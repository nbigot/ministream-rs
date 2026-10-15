//! Core data types shared by the streams, the storage providers and the web API.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub type MessageId = u64;
pub type Size64 = u64;
pub type StreamUuid = Uuid;
pub type StreamIteratorUuid = Uuid;
pub type StreamProperties = serde_json::Map<String, Value>;

/// Stream information shared between the stream, its writer and the storage catalog.
pub type SharedStreamInfo = Arc<RwLock<StreamInfo>>;

/// The "zero" time (0001-01-01T00:00:00Z), used when a timestamp is not set.
pub fn zero_time() -> DateTime<Utc> {
    DateTime::from_timestamp(-62_135_596_800, 0).expect("valid zero time")
}

pub fn is_zero_time(t: &DateTime<Utc>) -> bool {
    *t == zero_time()
}

/// Statistics about a set of messages of a stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamMessagesInfo {
    #[serde(default)]
    pub cpt_messages: Size64,
    #[serde(default)]
    pub size_in_bytes: Size64,
    #[serde(default)]
    pub first_msg_id: MessageId,
    #[serde(default)]
    pub last_msg_id: MessageId,
    #[serde(default = "zero_time")]
    pub first_msg_timestamp: DateTime<Utc>,
    // note: the JSON key "fastMsgTimestamp" (sic) is kept for compatibility
    #[serde(
        rename = "fastMsgTimestamp",
        alias = "lastMsgTimestamp",
        default = "zero_time"
    )]
    pub last_msg_timestamp: DateTime<Utc>,
}

impl Default for StreamMessagesInfo {
    fn default() -> Self {
        StreamMessagesInfo {
            cpt_messages: 0,
            size_in_bytes: 0,
            first_msg_id: 0,
            last_msg_id: 0,
            first_msg_timestamp: zero_time(),
            last_msg_timestamp: zero_time(),
        }
    }
}

/// Description of a stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamInfo {
    pub uuid: StreamUuid,
    #[serde(default = "zero_time")]
    pub creation_date: DateTime<Utc>,
    #[serde(default = "zero_time")]
    pub last_update: DateTime<Utc>,
    #[serde(default, deserialize_with = "properties_or_empty")]
    pub properties: StreamProperties,
    /// Messages that have been ingested in the stream.
    #[serde(default)]
    pub ingested_messages: StreamMessagesInfo,
    /// Messages that are readable by a consumer (i.e. persisted by the storage provider).
    #[serde(default)]
    pub readable_messages: StreamMessagesInfo,
}

fn properties_or_empty<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<StreamProperties, D::Error> {
    Ok(Option::<StreamProperties>::deserialize(deserializer)?.unwrap_or_default())
}

impl StreamInfo {
    pub fn new(uuid: StreamUuid) -> StreamInfo {
        let now = Utc::now();
        StreamInfo {
            uuid,
            creation_date: now,
            last_update: now,
            properties: StreamProperties::new(),
            ingested_messages: StreamMessagesInfo::default(),
            readable_messages: StreamMessagesInfo::default(),
        }
    }

    pub fn into_shared(self) -> SharedStreamInfo {
        Arc::new(RwLock::new(self))
    }

    /// Add or update properties.
    pub fn update_properties(&mut self, properties: StreamProperties) {
        for (key, value) in properties {
            self.properties.insert(key, value);
        }
        self.last_update = Utc::now();
    }

    /// Replace all properties.
    pub fn set_properties(&mut self, properties: StreamProperties) {
        self.properties = properties;
        self.last_update = Utc::now();
    }
}

/// A record waiting to be persisted by a stream writer.
///
/// This is also the envelope used to store a record (JSON keys `i`, `d` and `m`)
/// and returned to the consumers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeferedStreamRecord {
    #[serde(rename = "i")]
    pub id: MessageId,
    #[serde(rename = "d")]
    pub creation_date: DateTime<Utc>,
    #[serde(rename = "m")]
    pub msg: Value,
}

impl DeferedStreamRecord {
    /// Convert the record into its JSON envelope `{"i": .., "d": .., "m": ..}`.
    pub fn to_envelope(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

/// Kind of position at which a records iterator starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IteratorType {
    FirstMessage,
    LastMessage,
    AfterLastMessage,
    AtMessageId,
    AfterMessageId,
    AtTimestamp,
}

impl IteratorType {
    pub fn as_str(&self) -> &'static str {
        match self {
            IteratorType::FirstMessage => "FIRST_MESSAGE",
            IteratorType::LastMessage => "LAST_MESSAGE",
            IteratorType::AfterLastMessage => "AFTER_LAST_MESSAGE",
            IteratorType::AtMessageId => "AT_MESSAGE_ID",
            IteratorType::AfterMessageId => "AFTER_MESSAGE_ID",
            IteratorType::AtTimestamp => "AT_TIMESTAMP",
        }
    }
}

/// Request to create a records iterator.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamIteratorRequest {
    pub iterator_type: IteratorType,
    #[serde(default)]
    pub message_id: MessageId,
    #[serde(default)]
    pub timestamp: Option<DateTime<Utc>>,
    #[serde(default)]
    pub jq_filter: String,
    #[serde(default)]
    pub max_wait_time_seconds: u64,
    #[serde(default)]
    pub name: String,
}

/// Number of bytes of the compact JSON serialization of a value (without allocating it).
pub fn json_size(value: &Value) -> u64 {
    struct Counter(u64);
    impl std::io::Write for Counter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 += buf.len() as u64;
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    let _ = serde_json::to_writer(&mut counter, value);
    counter.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn stream_info_json_roundtrip_with_go_format() {
        // document produced by the Go implementation (local time offsets, typo in fastMsgTimestamp)
        let doc = r#"{"uuid":"4ce589e2-b483-467b-8b59-758b339801db","creationDate":"2023-01-15T10:20:30.123456789+01:00","lastUpdate":"2023-01-15T10:20:30.123456789+01:00","properties":null,"ingestedMessages":{"cptMessages":2,"sizeInBytes":20,"firstMsgId":1,"lastMsgId":2,"firstMsgTimestamp":"2023-01-15T10:20:31+01:00","fastMsgTimestamp":"2023-01-15T10:20:32+01:00"},"readableMessages":{"cptMessages":0,"sizeInBytes":0,"firstMsgId":0,"lastMsgId":0,"firstMsgTimestamp":"0001-01-01T00:00:00Z","fastMsgTimestamp":"0001-01-01T00:00:00Z"}}"#;
        let info: StreamInfo = serde_json::from_str(doc).unwrap();
        assert!(info.properties.is_empty());
        assert_eq!(info.ingested_messages.last_msg_id, 2);
        assert!(is_zero_time(&info.readable_messages.first_msg_timestamp));
        let out = serde_json::to_value(&info).unwrap();
        assert_eq!(
            out["readableMessages"]["fastMsgTimestamp"],
            "0001-01-01T00:00:00Z"
        );
        assert_eq!(
            out["ingestedMessages"]["fastMsgTimestamp"],
            "2023-01-15T09:20:32Z"
        );
    }

    #[test]
    fn record_envelope() {
        let record = DeferedStreamRecord {
            id: 3,
            creation_date: DateTime::from_timestamp(1_700_000_000, 5).unwrap(),
            msg: json!({"b": 1, "a": [true]}),
        };
        assert_eq!(
            serde_json::to_string(&record).unwrap(),
            r#"{"i":3,"d":"2023-11-14T22:13:20.000000005Z","m":{"a":[true],"b":1}}"#
        );
        assert_eq!(json_size(&record.msg), 18);
    }

    #[test]
    fn iterator_request_deserialization() {
        let req: StreamIteratorRequest = serde_json::from_str(
            r#"{"iteratorType":"AT_TIMESTAMP","timestamp":"2023-01-02T03:04:05Z"}"#,
        )
        .unwrap();
        assert_eq!(req.iterator_type, IteratorType::AtTimestamp);
        assert!(req.timestamp.is_some());
        assert_eq!(req.max_wait_time_seconds, 0);
    }
}
