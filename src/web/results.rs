//! JSON results of the web API.

use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use crate::types::{MessageId, Size64, StreamInfo, StreamProperties, StreamUuid};

#[derive(Debug, Clone, Serialize)]
pub struct JsonResultSuccess {
    pub code: u16,
    pub message: String,
}

impl JsonResultSuccess {
    pub fn ok() -> JsonResultSuccess {
        JsonResultSuccess {
            code: 200,
            message: "success".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct JsonResult<T: Serialize> {
    pub code: u16,
    pub message: String,
    pub data: T,
}

#[derive(Debug, Clone, Serialize)]
pub struct GetAccountResult {
    pub id: Uuid,
    pub name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct JsonResultListStreamsProperties {
    pub code: u16,
    pub result: JsonResultListStreamsPropertiesResult,
}

#[derive(Debug, Clone, Serialize)]
pub struct JsonResultListStreamsPropertiesResult {
    pub total: usize,
    pub rows: Vec<JsonResultListStreamsPropertiesRow>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonResultListStreamsPropertiesRow {
    pub uuid: StreamUuid,
    pub cpt_messages: Size64,
    pub size_in_bytes: Size64,
    pub creation_date: DateTime<Utc>,
    pub last_update: DateTime<Utc>,
    pub properties: StreamProperties,
    pub first_msg_id: MessageId,
    pub last_msg_id: MessageId,
}

impl From<StreamInfo> for JsonResultListStreamsPropertiesRow {
    fn from(info: StreamInfo) -> Self {
        JsonResultListStreamsPropertiesRow {
            uuid: info.uuid,
            cpt_messages: info.readable_messages.cpt_messages,
            size_in_bytes: info.readable_messages.size_in_bytes,
            creation_date: info.creation_date,
            last_update: info.last_update,
            properties: info.properties,
            first_msg_id: info.readable_messages.first_msg_id,
            last_msg_id: info.readable_messages.last_msg_id,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct JsonResultListUsers {
    pub code: u16,
    pub users: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JsonResultPbkdf2 {
    pub code: u16,
    pub message: String,
    pub hash: String,
    pub digest: String,
    pub iterations: i64,
    pub salt: String,
}
