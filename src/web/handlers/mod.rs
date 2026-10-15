//! Handlers of the web API.

pub mod account;
pub mod admin;
pub mod stream;

use std::collections::HashMap;
use std::sync::Arc;

use axum::response::Response;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use super::AppState;
use crate::apierror::{ApiError, ValidationError};
use crate::constants;
use crate::stream::Stream;
use crate::types::{StreamProperties, StreamUuid};

pub type ApiResult = Result<Response, ApiError>;

/// Deserialize a JSON request body.
pub fn parse_json<T: DeserializeOwned>(body: &[u8]) -> Result<T, ApiError> {
    serde_json::from_slice(body).map_err(|e| {
        ApiError::bad_request(
            constants::ERROR_CANT_DESERIALIZE_JSON,
            "invalid json body format",
        )
        .details(e)
    })
}

/// Error returned when a payload is valid JSON but does not pass the validation rules.
pub fn validation_failed(errors: Vec<ValidationError>) -> ApiError {
    ApiError::bad_request(
        constants::ERROR_CANT_DESERIALIZE_JSON,
        "invalid json body format",
    )
    .details("cannot deserialize json data")
    .validations(errors)
}

/// Find the stream of the `streamuuid` path parameter.
pub fn stream_from_param(
    state: &AppState,
    raw: &str,
) -> Result<(StreamUuid, Arc<Stream>), ApiError> {
    let uuid = Uuid::parse_str(raw).map_err(|_| {
        // missing or invalid parameter
        ApiError::bad_request(constants::ERROR_INVALID_STREAM_UUID, "invalid stream uuid")
            .validation(ValidationError::new(
                constants::PARAM_NAME_STREAM_UUID,
                "parameter",
                raw,
            ))
    })?;
    let stream = state.service.get_stream(&uuid).ok_or_else(|| {
        // stream uuid not found among existing streams
        ApiError::bad_request(constants::ERROR_STREAM_UUID_NOT_FOUND, "stream not found")
            .stream(uuid)
            .validation(ValidationError::new(
                constants::PARAM_NAME_STREAM_UUID,
                "parameter",
                uuid.to_string(),
            ))
    })?;
    Ok((uuid, stream))
}

/// Payload of the requests setting stream properties.
#[derive(Debug, Deserialize)]
pub struct PropertiesPayload {
    #[serde(default)]
    pub properties: Option<HashMap<String, String>>,
}

pub const MAX_PROPERTIES: usize = 32;
pub const MAX_PROPERTY_KEY_LENGTH: usize = 64;

/// Validate the properties of a payload (at most 32 properties, keys of 1 to 64 characters,
/// values of 1 to `max_value_length` characters).
pub fn validate_properties(
    payload: PropertiesPayload,
    max_value_length: usize,
) -> Result<StreamProperties, ApiError> {
    let Some(properties) = payload.properties else {
        return Err(validation_failed(vec![ValidationError::new(
            "Properties",
            "required",
            "",
        )]));
    };
    let mut errors = Vec::new();
    if properties.len() > MAX_PROPERTIES {
        errors.push(ValidationError::new(
            "Properties",
            "lte",
            MAX_PROPERTIES.to_string(),
        ));
    }
    let mut keys: Vec<&String> = properties.keys().collect();
    keys.sort();
    for key in keys {
        let key_length = key.chars().count();
        if key_length == 0 {
            errors.push(ValidationError::new("Properties", "gt", "0"));
        } else if key_length > MAX_PROPERTY_KEY_LENGTH {
            errors.push(ValidationError::new(
                format!("Properties[{key}]"),
                "lte",
                MAX_PROPERTY_KEY_LENGTH.to_string(),
            ));
        }
        let value_length = properties[key].chars().count();
        if value_length == 0 {
            errors.push(ValidationError::new(
                format!("Properties[{key}]"),
                "required",
                "",
            ));
        } else if value_length > max_value_length {
            errors.push(ValidationError::new(
                format!("Properties[{key}]"),
                "max",
                max_value_length.to_string(),
            ));
        }
    }
    if !errors.is_empty() {
        return Err(validation_failed(errors));
    }
    Ok(properties
        .into_iter()
        .map(|(k, v)| (k, Value::String(v)))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(json: &str) -> PropertiesPayload {
        parse_json(json.as_bytes()).unwrap()
    }

    #[test]
    fn properties_validation() {
        let props =
            validate_properties(payload(r#"{"properties": {"name": "test"}}"#), 128).unwrap();
        assert_eq!(props["name"], "test");
        assert!(validate_properties(payload(r#"{"properties": {}}"#), 128)
            .unwrap()
            .is_empty());
        assert!(validate_properties(payload(r#"{}"#), 128).is_err());
        assert!(validate_properties(payload(r#"{"properties": null}"#), 128).is_err());
        assert!(validate_properties(payload(r#"{"properties": {"": "x"}}"#), 128).is_err());
        assert!(validate_properties(payload(r#"{"properties": {"a": ""}}"#), 128).is_err());
        let long = "x".repeat(129);
        assert!(validate_properties(
            payload(&format!(r#"{{"properties": {{"a": "{long}"}}}}"#)),
            128
        )
        .is_err());
        assert!(validate_properties(
            payload(&format!(r#"{{"properties": {{"a": "{long}"}}}}"#)),
            256
        )
        .is_ok());
        let many: Vec<String> = (0..33).map(|i| format!(r#""k{i}": "v""#)).collect();
        assert!(validate_properties(
            payload(&format!(r#"{{"properties": {{{}}}}}"#, many.join(","))),
            128
        )
        .is_err());
        assert!(parse_json::<PropertiesPayload>(br#"{"properties": {"a": 1}}"#).is_err());
    }
}
