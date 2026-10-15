//! Errors returned by the web API.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
use uuid::Uuid;

/// Error returned to the HTTP clients as a JSON document.
///
/// ```json
/// {"error": "stream not found", "code": 1001, "streamUUID": "...", "validationErrors": [...]}
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct ApiError {
    /// HTTP response status code.
    #[serde(skip)]
    pub http_code: StatusCode,
    /// Application-level error message.
    #[serde(rename = "error", skip_serializing_if = "String::is_empty")]
    pub message: String,
    /// Application-level error details that best describes the error.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub details: String,
    /// Application-specific error code (see [`crate::constants`]).
    pub code: i32,
    /// Stream uuid (nil uuid when the error is not related to a stream).
    #[serde(rename = "streamUUID")]
    pub stream_uuid: Uuid,
    #[serde(rename = "validationErrors", skip_serializing_if = "Vec::is_empty")]
    pub validation_errors: Vec<ValidationError>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ValidationError {
    #[serde(rename = "failedField", skip_serializing_if = "String::is_empty")]
    pub failed_field: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub tag: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub value: String,
}

impl ValidationError {
    pub fn new(
        failed_field: impl Into<String>,
        tag: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        ValidationError {
            failed_field: failed_field.into(),
            tag: tag.into(),
            value: value.into(),
        }
    }
}

impl ApiError {
    pub fn new(http_code: StatusCode, code: i32, message: impl Into<String>) -> Self {
        ApiError {
            http_code,
            message: message.into(),
            details: String::new(),
            code,
            stream_uuid: Uuid::nil(),
            validation_errors: Vec::new(),
        }
    }

    pub fn bad_request(code: i32, message: impl Into<String>) -> Self {
        ApiError::new(StatusCode::BAD_REQUEST, code, message)
    }

    pub fn internal(code: i32, message: impl Into<String>) -> Self {
        ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, code, message)
    }

    pub fn details(mut self, details: impl ToString) -> Self {
        self.details = details.to_string();
        self
    }

    pub fn stream(mut self, stream_uuid: Uuid) -> Self {
        self.stream_uuid = stream_uuid;
        self
    }

    pub fn validation(mut self, error: ValidationError) -> Self {
        self.validation_errors.push(error);
        self
    }

    pub fn validations(mut self, errors: Vec<ValidationError>) -> Self {
        self.validation_errors.extend(errors);
        self
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.details.is_empty() {
            write!(f, "{}", self.message)
        } else {
            write!(f, "{}: {}", self.message, self.details)
        }
    }
}

impl std::error::Error for ApiError {}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.http_code, Json(self)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_format() {
        let e = ApiError::bad_request(1001, "stream not found").validation(ValidationError::new(
            "streamuuid",
            "parameter",
            "abc",
        ));
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "error": "stream not found",
                "code": 1001,
                "streamUUID": "00000000-0000-0000-0000-000000000000",
                "validationErrors": [{"failedField": "streamuuid", "tag": "parameter", "value": "abc"}]
            })
        );
    }
}
