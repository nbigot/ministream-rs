//! Stream API handlers.

use std::collections::HashMap;
use std::time::Instant;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use serde_json::Value;
use uuid::Uuid;

use super::{parse_json, stream_from_param, validate_properties, ApiResult, PropertiesPayload};
use crate::apierror::{ApiError, ValidationError};
use crate::constants;
use crate::jq::JqFilter;
use crate::log_info;
use crate::stream::request::parse_stream_iterator_request;
use crate::stream::responses::{
    CloseRecordsIteratorResponse, CreateRecordsIteratorResponse, GetRecordsIteratorStatsResponse,
    PutStreamRecordsResponse, RebuildStreamIndexResponse,
};
use crate::types::StreamUuid;
use crate::web::client::ClientInfo;
use crate::web::rbac::Rbac;
use crate::web::results::{
    JsonResultListStreamsProperties, JsonResultListStreamsPropertiesResult, JsonResultSuccess,
};
use crate::web::AppState;

fn invalid_jq(jq: &str, error: String) -> ApiError {
    ApiError::bad_request(constants::ERROR_INVALID_JQ_FILTER, "invalid jq filter")
        .details(error)
        .validation(ValidationError::new("jq", "JQ", jq))
}

/// Parse the optional `jq` query parameter.
fn jq_from_query(query: &HashMap<String, String>) -> Result<Option<JqFilter>, ApiError> {
    let jq = query.get("jq").map(String::as_str).unwrap_or_default();
    JqFilter::parse_optional(jq).map_err(|e| invalid_jq(jq, e))
}

/// List the uuids of the streams (`GET /api/v1/streams/?jq=<filter on properties>`).
pub async fn list_streams(
    State(state): State<AppState>,
    Rbac(rbac): Rbac,
    Query(query): Query<HashMap<String, String>>,
) -> ApiResult {
    let jq = jq_from_query(&query)?;
    let filters: Vec<&JqFilter> = jq
        .iter()
        .chain(rbac.abac.as_ref().map(|a| &a.filter))
        .collect();
    let uuids: Vec<StreamUuid> = if filters.is_empty() {
        state.service.get_streams_uuids()
    } else {
        state
            .service
            .get_streams_filtered(&filters)
            .iter()
            .map(|s| s.uuid())
            .collect()
    };
    Ok(Json(uuids).into_response())
}

/// List the streams and their properties (`GET /api/v1/streams/properties?jq=<filter>`).
pub async fn list_streams_properties(
    State(state): State<AppState>,
    Rbac(rbac): Rbac,
    Query(query): Query<HashMap<String, String>>,
) -> ApiResult {
    let jq = jq_from_query(&query)?;
    let filters: Vec<&JqFilter> = jq
        .iter()
        .chain(rbac.abac.as_ref().map(|a| &a.filter))
        .collect();
    let rows: Vec<_> = state
        .service
        .get_streams_filtered(&filters)
        .iter()
        .map(|s| s.info().into())
        .collect();
    Ok(Json(JsonResultListStreamsProperties {
        code: 200,
        result: JsonResultListStreamsPropertiesResult {
            total: rows.len(),
            rows,
        },
    })
    .into_response())
}

/// Create a stream (`POST /api/v1/stream/`).
pub async fn create_stream(
    State(state): State<AppState>,
    client: ClientInfo,
    body: Bytes,
) -> ApiResult {
    let payload: PropertiesPayload = parse_json(&body)?;
    let properties = validate_properties(payload, 128)?;
    let stream = state.service.create_stream(properties).await.map_err(|e| {
        ApiError::bad_request(constants::ERROR_CANT_CREATE_STREAM, "cannot create stream")
            .details(e)
    })?;
    log_info!(state.logger, "Stream created", "topic" => "stream", "method" => "CreateStream",
        "accountId" => state.account.id, "ipAddress" => client.ip, "ipAddresses" => client.ips_joined(),
        "streamUUID" => stream.uuid());
    Ok((StatusCode::CREATED, Json(stream.info())).into_response())
}

async fn change_properties(
    state: AppState,
    raw_uuid: String,
    body: Bytes,
    replace: bool,
) -> ApiResult {
    let payload: PropertiesPayload = parse_json(&body)?;
    let properties = validate_properties(payload, 256)?;
    let (_, stream) = stream_from_param(&state, &raw_uuid)?;
    let properties = state
        .service
        .change_stream_properties(&stream, properties, replace)
        .await;
    Ok(Json(properties).into_response())
}

/// Set and replace the properties of a stream (`POST /api/v1/stream/{streamuuid}/properties`).
pub async fn set_stream_properties(
    State(state): State<AppState>,
    Path(stream_uuid): Path<String>,
    body: Bytes,
) -> ApiResult {
    change_properties(state, stream_uuid, body, true).await
}

/// Add or update properties of a stream (`PATCH /api/v1/stream/{streamuuid}/properties`).
pub async fn update_stream_properties(
    State(state): State<AppState>,
    Path(stream_uuid): Path<String>,
    body: Bytes,
) -> ApiResult {
    change_properties(state, stream_uuid, body, false).await
}

/// Get the properties of a stream (`GET /api/v1/stream/{streamuuid}/properties`).
pub async fn get_stream_properties(
    State(state): State<AppState>,
    Path(stream_uuid): Path<String>,
) -> ApiResult {
    let (_, stream) = stream_from_param(&state, &stream_uuid)?;
    Ok(Json(stream.properties()).into_response())
}

/// Delete a stream (`DELETE /api/v1/stream/{streamuuid}`).
pub async fn delete_stream(
    State(state): State<AppState>,
    client: ClientInfo,
    Path(stream_uuid): Path<String>,
) -> ApiResult {
    let (uuid, _) = stream_from_param(&state, &stream_uuid)?;
    state.service.delete_stream(&uuid).await.map_err(|e| {
        ApiError::bad_request(constants::ERROR_CANT_DELETE_STREAM, "cannot delete stream")
            .details(e)
    })?;
    log_info!(state.logger, "Stream deleted", "topic" => "stream", "method" => "DeleteStream",
        "accountId" => state.account.id, "ipAddress" => client.ip, "ipAddresses" => client.ips_joined(),
        "streamUUID" => uuid);
    Ok(Json(JsonResultSuccess::ok()).into_response())
}

/// Get the information of a stream (`GET /api/v1/stream/{streamuuid}`).
pub async fn get_stream_information(
    State(state): State<AppState>,
    Path(stream_uuid): Path<String>,
) -> ApiResult {
    let (_, stream) = stream_from_param(&state, &stream_uuid)?;
    Ok(Json(stream.info()).into_response())
}

/// Create a records iterator (`POST /api/v1/stream/{streamuuid}/iterator`).
pub async fn create_records_iterator(
    State(state): State<AppState>,
    Path(stream_uuid): Path<String>,
    body: Bytes,
) -> ApiResult {
    let request = parse_stream_iterator_request(&body).map_err(|e| {
        ApiError::bad_request(
            constants::ERROR_INVALID_CREATE_RECORDS_ITERATOR_REQUEST,
            "invalid request",
        )
        .details(e)
    })?;
    let (uuid, stream) = stream_from_param(&state, &stream_uuid)?;
    let iterator_uuid = state
        .service
        .create_records_iterator(&stream, &request)
        .await?;
    Ok(Json(CreateRecordsIteratorResponse {
        status: "success".into(),
        message: "Stream iterator created".into(),
        stream_uuid: uuid,
        stream_iterator_uuid: iterator_uuid,
    })
    .into_response())
}

fn parse_iterator_uuid(
    raw: &str,
    stream_uuid: StreamUuid,
    code: i32,
    message: &str,
) -> Result<Uuid, ApiError> {
    Uuid::parse_str(raw).map_err(|e| {
        ApiError::bad_request(code, message)
            .details(e)
            .stream(stream_uuid)
            .validation(ValidationError::new(
                constants::PARAM_NAME_STREAM_ITERATOR_UUID,
                "parameter",
                raw,
            ))
    })
}

/// Statistics of a records iterator (`GET /api/v1/stream/{streamuuid}/iterator/{streamiteratoruuid}/stats`).
pub async fn get_records_iterator_stats(
    State(state): State<AppState>,
    Path((stream_uuid, iterator_uuid)): Path<(String, String)>,
) -> ApiResult {
    let (uuid, stream) = stream_from_param(&state, &stream_uuid)?;
    let iterator_uuid = parse_iterator_uuid(
        &iterator_uuid,
        uuid,
        constants::ERROR_INVALID_STREAM_UUID,
        "invalid stream iterator uuid",
    )?;
    let iterator = stream.get_iterator(&iterator_uuid).ok_or_else(|| {
        ApiError::bad_request(
            constants::ERROR_STREAM_ITERATOR_NOT_FOUND,
            "iterator not found",
        )
        .details(format!("iterator not found: {iterator_uuid}"))
        .stream(uuid)
    })?;
    Ok(Json(GetRecordsIteratorStatsResponse {
        status: "success".into(),
        message: String::new(),
        stream_uuid: uuid,
        stream_iterator_uuid: iterator_uuid,
        last_record_id_read: iterator.last_record_id_read(),
        name: iterator.name().to_string(),
        stats: iterator.stats(),
    })
    .into_response())
}

/// Close a records iterator (`DELETE /api/v1/stream/{streamuuid}/iterator/{streamiteratoruuid}`).
pub async fn close_records_iterator(
    State(state): State<AppState>,
    Path((stream_uuid, raw_iterator_uuid)): Path<(String, String)>,
) -> ApiResult {
    let (uuid, stream) = stream_from_param(&state, &stream_uuid)?;
    let iterator_uuid = parse_iterator_uuid(
        &raw_iterator_uuid,
        uuid,
        constants::ERROR_INVALID_STREAM_UUID,
        "invalid stream iterator uuid",
    )?;
    stream.close_iterator(&iterator_uuid).await.map_err(|e| {
        ApiError::bad_request(
            constants::ERROR_CANT_CLOSE_STREAM_ITERATOR,
            "cannot close stream iterator uuid",
        )
        .details(e)
        .stream(uuid)
        .validation(ValidationError::new(
            constants::PARAM_NAME_STREAM_ITERATOR_UUID,
            "parameter",
            raw_iterator_uuid.as_str(),
        ))
    })?;
    Ok(Json(CloseRecordsIteratorResponse {
        status: "success".into(),
        message: "Stream iterator closed".into(),
        stream_uuid: uuid,
        stream_iterator_uuid: iterator_uuid,
    })
    .into_response())
}

/// Get the next records of an iterator
/// (`GET /api/v1/stream/{streamuuid}/iterator/{streamiteratoruuid}/records?maxRecords=<n>`).
pub async fn get_records(
    State(state): State<AppState>,
    Path((stream_uuid, iterator_uuid)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
) -> ApiResult {
    let (uuid, stream) = stream_from_param(&state, &stream_uuid)?;
    let iterator_uuid = parse_iterator_uuid(
        &iterator_uuid,
        uuid,
        constants::ERROR_INVALID_ITERATOR_UUID,
        "invalid iterator uuid",
    )?;

    let limit = state.config.max_records_per_get();
    let max_records = match query.get("maxRecords").map(String::as_str) {
        None | Some("") => limit,
        Some(raw) => {
            let checked = match raw.parse::<u64>() {
                Ok(0) => Err("value must be positive".to_string()),
                Ok(n) if n > limit => Err(format!("value cannot exceed limit {limit}")),
                Ok(n) => Ok(n),
                Err(e) => Err(e.to_string()),
            };
            checked.map_err(|e| {
                ApiError::bad_request(
                    constants::ERROR_INVALID_PARAMETER_VALUE,
                    "invalid integer value",
                )
                .details(e)
                .stream(uuid)
                .validation(ValidationError::new("maxRecords", "parameter", raw))
            })?
        }
    };

    let response = stream.get_records(&iterator_uuid, max_records).await?;
    Ok(Json(response).into_response())
}

fn batch_id(headers: &HeaderMap) -> Option<String> {
    headers
        .get(constants::HEADER_BATCH_ID)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

/// Register the batch id of the request, fails when the batch was already processed.
fn deduplicate(
    state: &AppState,
    headers: &HeaderMap,
    stream_uuid: StreamUuid,
) -> Result<Option<String>, ApiError> {
    let Some(batch_id) = batch_id(headers) else {
        return Ok(None);
    };
    let dedup_id = format!("{stream_uuid}:{batch_id}");
    if !state.dedup.try_add(&dedup_id) {
        return Err(ApiError::bad_request(
            constants::ERROR_DUPLICATED_BATCH_ID,
            "batch id already processed",
        )
        .details(format!("{}: {}", constants::HEADER_BATCH_ID, batch_id))
        .stream(stream_uuid));
    }
    Ok(Some(dedup_id))
}

/// Put a single record (a JSON object) into a stream (`PUT /api/v1/stream/{streamuuid}/record`).
pub async fn put_record(
    State(state): State<AppState>,
    Path(stream_uuid): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult {
    let start = Instant::now();
    let (uuid, stream) = stream_from_param(&state, &stream_uuid)?;
    let dedup_id = deduplicate(&state, &headers, uuid)?;
    let forget_batch = || {
        if let Some(id) = &dedup_id {
            state.dedup.remove(id);
        }
    };

    let record: serde_json::Map<String, Value> = serde_json::from_slice(&body).map_err(|e| {
        forget_batch();
        ApiError::bad_request(
            constants::ERROR_CANT_DESERIALIZE_JSON,
            "invalid json body format",
        )
        .details(e)
        .stream(uuid)
    })?;
    let message_id = stream
        .put_message(Value::Object(record))
        .await
        .map_err(|e| {
            forget_batch();
            ApiError::internal(
                constants::ERROR_CANT_PUT_MESSAGE_INTO_STREAM,
                "cannot put record into stream",
            )
            .details(e)
            .stream(uuid)
        })?;

    Ok((
        StatusCode::ACCEPTED,
        Json(PutStreamRecordsResponse {
            status: "success".into(),
            stream_uuid: uuid,
            duration: start.elapsed().as_millis() as i64,
            count: 1,
            message_ids: vec![message_id],
        }),
    )
        .into_response())
}

fn is_json_lines(headers: &HeaderMap) -> bool {
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    let essence = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    matches!(
        essence.as_str(),
        "application/jsonlines"
            | "application/x-ndjson"
            | "application/jsonl"
            | "application/x-jsonlines"
    )
}

/// Parse a JSON lines body (one JSON value per line, blank lines are ignored).
fn parse_json_lines(body: &[u8]) -> Result<Vec<Value>, String> {
    body.split(|b| *b == b'\n')
        .enumerate()
        .filter(|(_, line)| !line.iter().all(u8::is_ascii_whitespace))
        .map(|(n, line)| serde_json::from_slice(line).map_err(|e| format!("line {}: {e}", n + 1)))
        .collect()
}

/// Put records into a stream (`PUT /api/v1/stream/{streamuuid}/records`).
///
/// The body is either a JSON array of records, or JSON lines when the content type is
/// `application/jsonlines` or `application/x-ndjson`.
pub async fn put_records(
    State(state): State<AppState>,
    Path(stream_uuid): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult {
    let start = Instant::now();
    let (uuid, stream) = stream_from_param(&state, &stream_uuid)?;
    let dedup_id = deduplicate(&state, &headers, uuid)?;
    let forget_batch = || {
        if let Some(id) = &dedup_id {
            state.dedup.remove(id);
        }
    };

    let records: Vec<Value> = if is_json_lines(&headers) {
        parse_json_lines(&body).map_err(|e| {
            forget_batch();
            ApiError::bad_request(
                constants::ERROR_CANT_DESERIALIZE_JSON_RECORDS,
                "invalid jsonlines body format",
            )
            .details(e)
            .stream(uuid)
        })?
    } else {
        serde_json::from_slice(&body).map_err(|e| {
            forget_batch();
            ApiError::bad_request(
                constants::ERROR_CANT_DESERIALIZE_JSON_RECORDS,
                "invalid json body format",
            )
            .details(e)
            .stream(uuid)
        })?
    };
    if records.is_empty() {
        forget_batch();
        return Err(ApiError::bad_request(
            constants::ERROR_CANT_PUT_MESSAGES_INTO_STREAM,
            "cannot put records into stream",
        )
        .details("no records to ingest")
        .stream(uuid));
    }

    let count = records.len() as i64;
    let message_ids = stream.put_messages(records).await.map_err(|e| {
        forget_batch();
        ApiError::internal(
            constants::ERROR_CANT_PUT_MESSAGES_INTO_STREAM,
            "cannot put records into stream",
        )
        .details(e)
        .stream(uuid)
    })?;

    Ok((
        StatusCode::ACCEPTED,
        Json(PutStreamRecordsResponse {
            status: "success".into(),
            stream_uuid: uuid,
            duration: start.elapsed().as_millis() as i64,
            count,
            message_ids,
        }),
    )
        .into_response())
}

/// Build or rebuild the index of a stream (`POST /api/v1/stream/{streamuuid}/index/rebuild`).
pub async fn rebuild_index(
    State(state): State<AppState>,
    client: ClientInfo,
    Path(stream_uuid): Path<String>,
) -> ApiResult {
    let start = Instant::now();
    let (uuid, stream) = stream_from_param(&state, &stream_uuid)?;
    stream.close_iterators().await;
    let index_stats = state.service.build_index(&uuid).await.map_err(|e| {
        ApiError::internal(
            constants::ERROR_CANT_REBUILD_STREAM_INDEX,
            "cannot rebuild stream index",
        )
        .details(e)
        .stream(uuid)
    })?;
    log_info!(state.logger, "Stream index rebuilt", "topic" => "stream", "method" => "RebuildIndex",
        "accountId" => state.account.id, "ipAddress" => client.ip, "ipAddresses" => client.ips_joined(),
        "streamUUID" => uuid);
    Ok(Json(RebuildStreamIndexResponse {
        status: "success".into(),
        message: "stream index rebuilt".into(),
        stream_uuid: uuid,
        duration: start.elapsed().as_millis() as i64,
        index_stats,
    })
    .into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_lines() {
        let records = parse_json_lines(b"{\"a\":1}\n\n[2]\r\n\"three\"\n").unwrap();
        assert_eq!(records.len(), 3);
        assert!(parse_json_lines(b"{\"a\":1}\n{oops}\n")
            .unwrap_err()
            .starts_with("line 2"));
        assert!(parse_json_lines(b"").unwrap().is_empty());
    }

    #[test]
    fn json_lines_content_types() {
        let mut headers = HeaderMap::new();
        assert!(!is_json_lines(&headers));
        headers.insert(
            "content-type",
            "application/x-ndjson; charset=utf-8".parse().unwrap(),
        );
        assert!(is_json_lines(&headers));
        headers.insert("content-type", "Application/JSONLines".parse().unwrap());
        assert!(is_json_lines(&headers));
        headers.insert("content-type", "application/json".parse().unwrap());
        assert!(!is_json_lines(&headers));
    }
}
