//! Validation of the "create records iterator" requests.
//!
//! Valid requests examples:
//!
//! ```json
//! {"iteratorType": "FIRST_MESSAGE"}
//! {"iteratorType": "LAST_MESSAGE"}
//! {"iteratorType": "AFTER_LAST_MESSAGE"}
//! {"iteratorType": "AT_MESSAGE_ID", "messageId": 1234}
//! {"iteratorType": "AFTER_MESSAGE_ID", "messageId": 1234}
//! {"iteratorType": "AT_TIMESTAMP", "timestamp": "2006-01-02T15:04:06Z"}
//! {"iteratorType": "FIRST_MESSAGE", "name": "myApp", "jqFilter": ".m", "maxWaitTimeSeconds": 20}
//! ```

use chrono::{DateTime, Utc};
use serde_json::{Map, Value};

use crate::types::{IteratorType, StreamIteratorRequest};

const COMMON_PROPERTIES: &[&str] = &["iteratorType", "jqFilter", "name", "maxWaitTimeSeconds"];
const MAX_WAIT_TIME_SECONDS: u64 = 60;

/// Validate a request body and build the request.
pub fn parse_stream_iterator_request(body: &[u8]) -> Result<StreamIteratorRequest, String> {
    let value: Value = serde_json::from_slice(body).map_err(|e| format!("invalid json: {e}"))?;
    let object = value
        .as_object()
        .ok_or_else(|| "the request must be a JSON object".to_string())?;

    let iterator_type = match object.get("iteratorType") {
        None => return Err("iteratorType is required".into()),
        Some(Value::String(s)) => s.as_str(),
        Some(_) => return Err("iteratorType must be a string".into()),
    };
    let (iterator_type, extra): (IteratorType, Option<&str>) = match iterator_type {
        "FIRST_MESSAGE" => (IteratorType::FirstMessage, None),
        "LAST_MESSAGE" => (IteratorType::LastMessage, None),
        "AFTER_LAST_MESSAGE" => (IteratorType::AfterLastMessage, None),
        "AT_MESSAGE_ID" => (IteratorType::AtMessageId, Some("messageId")),
        "AFTER_MESSAGE_ID" => (IteratorType::AfterMessageId, Some("messageId")),
        "AT_TIMESTAMP" => (IteratorType::AtTimestamp, Some("timestamp")),
        other => {
            return Err(format!(
                "iteratorType: invalid value \"{other}\" (allowed values: FIRST_MESSAGE, LAST_MESSAGE, AFTER_LAST_MESSAGE, AT_MESSAGE_ID, AFTER_MESSAGE_ID, AT_TIMESTAMP)"
            ))
        }
    };

    let mut errors = Vec::new();
    for key in object.keys() {
        if !COMMON_PROPERTIES.contains(&key.as_str()) && Some(key.as_str()) != extra {
            errors.push(format!("additional property \"{key}\" is not allowed"));
        }
    }
    if let Some(required) = extra {
        if !object.contains_key(required) {
            errors.push(format!(
                "\"{required}\" is required for iteratorType {}",
                iterator_type.as_str()
            ));
        }
    }

    let jq_filter = string_property(object, "jqFilter", 1, 512, &mut errors);
    let name = string_property(object, "name", 1, 256, &mut errors);
    let max_wait_time_seconds = match object.get("maxWaitTimeSeconds") {
        None => 0,
        Some(v) => match integer(v) {
            Some(n) if n <= MAX_WAIT_TIME_SECONDS => n,
            _ => {
                errors.push(format!(
                    "maxWaitTimeSeconds must be an integer between 0 and {MAX_WAIT_TIME_SECONDS}"
                ));
                0
            }
        },
    };
    let message_id = match object.get("messageId") {
        Some(v) if extra == Some("messageId") => integer(v).unwrap_or_else(|| {
            errors.push("messageId must be a positive integer".into());
            0
        }),
        _ => 0,
    };
    let timestamp = match object.get("timestamp") {
        Some(v) if extra == Some("timestamp") => match v.as_str().map(DateTime::parse_from_rfc3339)
        {
            Some(Ok(t)) => Some(t.with_timezone(&Utc)),
            _ => {
                errors.push(
                    "timestamp must be a RFC 3339 date time (ex: 2006-01-02T15:04:05Z)".into(),
                );
                None
            }
        },
        _ => None,
    };

    if !errors.is_empty() {
        return Err(errors.join(", "));
    }
    Ok(StreamIteratorRequest {
        iterator_type,
        message_id,
        timestamp,
        jq_filter,
        max_wait_time_seconds,
        name,
    })
}

fn integer(value: &Value) -> Option<u64> {
    match value {
        Value::Number(n) => n.as_u64().or_else(|| {
            n.as_f64()
                .filter(|f| *f >= 0.0 && f.fract() == 0.0 && *f < u64::MAX as f64)
                .map(|f| f as u64)
        }),
        _ => None,
    }
}

fn string_property(
    object: &Map<String, Value>,
    key: &str,
    min_length: usize,
    max_length: usize,
    errors: &mut Vec<String>,
) -> String {
    match object.get(key) {
        None => String::new(),
        Some(Value::String(s)) if (min_length..=max_length).contains(&s.chars().count()) => {
            s.clone()
        }
        Some(_) => {
            errors.push(format!(
                "{key} must be a string of {min_length} to {max_length} characters"
            ));
            String::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(body: &str) -> Result<StreamIteratorRequest, String> {
        parse_stream_iterator_request(body.as_bytes())
    }

    #[test]
    fn valid_requests() {
        assert_eq!(
            parse(r#"{"iteratorType":"FIRST_MESSAGE"}"#)
                .unwrap()
                .iterator_type,
            IteratorType::FirstMessage
        );
        assert_eq!(
            parse(r#"{"iteratorType":"LAST_MESSAGE"}"#)
                .unwrap()
                .iterator_type,
            IteratorType::LastMessage
        );
        assert_eq!(
            parse(r#"{"iteratorType":"AT_MESSAGE_ID","messageId":1234}"#)
                .unwrap()
                .message_id,
            1234
        );
        assert_eq!(
            parse(r#"{"iteratorType":"AFTER_MESSAGE_ID","messageId":0}"#)
                .unwrap()
                .message_id,
            0
        );
        assert!(
            parse(r#"{"iteratorType":"AT_TIMESTAMP","timestamp":"2006-01-02T15:04:06Z"}"#)
                .unwrap()
                .timestamp
                .is_some()
        );
        let req = parse(r#"{"iteratorType":"FIRST_MESSAGE","name":"myApp","jqFilter":".m","maxWaitTimeSeconds":20}"#).unwrap();
        assert_eq!(req.name, "myApp");
        assert_eq!(req.jq_filter, ".m");
        assert_eq!(req.max_wait_time_seconds, 20);
    }

    #[test]
    fn invalid_requests() {
        for body in [
            "",
            "[]",
            "{}",
            r#"{"iteratorType":"SOMEWHERE"}"#,
            r#"{"iteratorType":1}"#,
            r#"{"iteratorType":"FIRST_MESSAGE","messageId":1}"#,
            r#"{"iteratorType":"FIRST_MESSAGE","other":1}"#,
            r#"{"iteratorType":"AT_MESSAGE_ID"}"#,
            r#"{"iteratorType":"AT_MESSAGE_ID","messageId":-1}"#,
            r#"{"iteratorType":"AT_MESSAGE_ID","messageId":"12"}"#,
            r#"{"iteratorType":"AT_TIMESTAMP"}"#,
            r#"{"iteratorType":"AT_TIMESTAMP","timestamp":"yesterday"}"#,
            r#"{"iteratorType":"FIRST_MESSAGE","maxWaitTimeSeconds":61}"#,
            r#"{"iteratorType":"FIRST_MESSAGE","maxWaitTimeSeconds":"20"}"#,
            r#"{"iteratorType":"FIRST_MESSAGE","jqFilter":""}"#,
            r#"{"iteratorType":"FIRST_MESSAGE","name":""}"#,
        ] {
            assert!(parse(body).is_err(), "{body}");
        }
    }
}
