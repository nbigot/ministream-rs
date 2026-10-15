//! Administration and utilities API handlers.

use axum::body::Bytes;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;

use super::{parse_json, validation_failed, ApiResult};
use crate::apierror::{ApiError, ValidationError};
use crate::auth::hash_password;
use crate::constants;
use crate::log_info;
use crate::web::results::{JsonResultPbkdf2, JsonResultSuccess};
use crate::web::{AppState, ServerCommand};

/// Shutdown the server (`POST /api/v1/admin/server/shutdown`).
pub async fn shutdown_server(State(state): State<AppState>) -> ApiResult {
    log_info!(state.logger, "Shutdown requested", "topic" => "server", "method" => "ApiServerShutdown");
    let _ = state.control.send(ServerCommand::Shutdown);
    Ok(Json(JsonResultSuccess::ok()).into_response())
}

/// Restart the server, the configuration file is reloaded (`POST /api/v1/admin/server/restart`).
pub async fn restart_server(State(state): State<AppState>) -> ApiResult {
    log_info!(state.logger, "Restart requested", "topic" => "server", "method" => "ApiServerRestart");
    let _ = state.control.send(ServerCommand::Restart);
    Ok(Json(JsonResultSuccess::ok()).into_response())
}

/// Revoke all the JWT emitted until now (`POST /api/v1/admin/jwt/revoke`).
pub async fn jwt_revoke_all(State(state): State<AppState>) -> ApiResult {
    state.jwt.revoke_all();
    log_info!(state.logger, "All JWT revoked", "topic" => "server", "method" => "ActionJWTRevokeAll");
    Ok(Json(JsonResultSuccess::ok()).into_response())
}

/// Ping the server (`GET /api/v1/utils/ping`).
pub async fn ping() -> &'static str {
    "ok"
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Pbkdf2Payload {
    pub digest: String,
    pub iterations: i64,
    pub salt: String,
    pub password: String,
}

/// Generate a PBKDF2 hash from a password (`POST /api/v1/utils/pbkdf2`).
pub async fn utils_pbkdf2(body: Bytes) -> ApiResult {
    let payload: Pbkdf2Payload = parse_json(&body)?;
    let mut errors = Vec::new();
    for (field, value) in [
        ("digest", &payload.digest),
        ("salt", &payload.salt),
        ("password", &payload.password),
    ] {
        if value.is_empty() {
            errors.push(ValidationError::new(field, "required", ""));
        }
    }
    if payload.iterations < 1 {
        errors.push(ValidationError::new("iterations", "min", "1"));
    } else if payload.iterations > 10_000 {
        errors.push(ValidationError::new("iterations", "max", "10000"));
    }
    if !errors.is_empty() {
        return Err(validation_failed(errors));
    }

    let iterations = payload.iterations;
    let (digest, salt, password) = (payload.digest, payload.salt, payload.password);
    let hash = {
        let (digest, salt) = (digest.clone(), salt.clone());
        tokio::task::spawn_blocking(move || hash_password(&digest, iterations, &salt, &password))
            .await
            .map_err(|e| {
                ApiError::internal(
                    constants::ERROR_INVALID_PARAMETER_VALUE,
                    "cannot hash password",
                )
                .details(e)
            })?
    };
    match hash {
        Ok(hash) => Ok(Json(JsonResultPbkdf2 {
            code: 200,
            message: "success".into(),
            hash,
            digest,
            iterations,
            salt,
        })
        .into_response()),
        Err(e) => Err(ApiError::bad_request(
            constants::ERROR_INVALID_PARAMETER_VALUE,
            "invalid hash value",
        )
        .details(e)
        .validation(ValidationError::new("hash", "parameter", ""))),
    }
}
