//! Account and user API handlers.

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;

use super::ApiResult;
use crate::apierror::{ApiError, ValidationError};
use crate::constants;
use crate::stream::responses::LoginResponse;
use crate::web::client::ClientInfo;
use crate::web::results::{GetAccountResult, JsonResult, JsonResultListUsers, JsonResultSuccess};
use crate::web::AppState;
use crate::{log_error, log_info};

fn header<'a>(headers: &'a HeaderMap, name: &str) -> &'a str {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
}

fn jwt_not_enabled() -> ApiError {
    ApiError::bad_request(constants::ERROR_JWT_NOT_ENABLED, "bad Request")
        .details("JWT is not enabled on server")
}

fn rbac_not_enabled() -> ApiError {
    ApiError::bad_request(constants::ERROR_RBAC_NOT_ENABLED, "bad Request")
        .details("RBAC is not enabled on server")
}

/// Account details (`GET /api/v1/account/`), the secret API key is not returned.
pub async fn get_account(State(state): State<AppState>) -> ApiResult {
    Ok(Json(JsonResult {
        code: 200,
        message: "success".into(),
        data: GetAccountResult {
            id: state.account.id,
            name: state.account.name.clone(),
        },
    })
    .into_response())
}

/// Validate the API key of the `API-KEY` header (`GET /api/v1/account/validate`).
pub async fn validate_api_key(
    State(state): State<AppState>,
    client: ClientInfo,
    headers: HeaderMap,
) -> ApiResult {
    if !state.jwt.is_enabled() {
        return Err(jwt_not_enabled());
    }
    if state
        .account
        .check_api_key(header(&headers, constants::HEADER_API_KEY))
    {
        log_info!(state.logger, "ValidateApiKey succeeded", "topic" => "account", "method" => "ValidateApiKey",
            "accountId" => state.account.id, "ipAddress" => client.ip, "ipAddresses" => client.ips_joined());
        return Ok(Json(JsonResultSuccess::ok()).into_response());
    }
    log_info!(state.logger, "ValidateApiKey failed", "topic" => "account", "method" => "ValidateApiKey",
        "accountId" => state.account.id, "ipAddress" => client.ip, "ipAddresses" => client.ips_joined());
    // wrong secret value or non existing header key/value
    Err(ApiError::new(
        StatusCode::FORBIDDEN,
        constants::ERROR_INVALID_PARAMETER_VALUE,
        "invalid hash value",
    )
    .details("wrong secret value or non existing header key/value")
    .validation(ValidationError::new(
        constants::HEADER_API_KEY,
        "header",
        "",
    )))
}

/// Log in with the secret API key of the `API-KEY` header (`GET /api/v1/account/login`).
/// The returned JWT is a super user token.
pub async fn login_account(
    State(state): State<AppState>,
    client: ClientInfo,
    headers: HeaderMap,
) -> ApiResult {
    if !state.jwt.is_enabled() {
        return Err(jwt_not_enabled());
    }
    let account_id = state.account.id;
    if state
        .account
        .check_api_key(header(&headers, constants::HEADER_API_KEY))
    {
        match state.jwt.generate(true, None) {
            Ok((token, claims)) => {
                log_info!(state.logger, "Login succeeded", "topic" => "account", "method" => "LoginAccount",
                    "accountId" => account_id, "ipAddress" => client.ip, "ipAddresses" => client.ips_joined(),
                    "userAgent" => client.user_agent, "jti" => claims.get("jti"));
                return Ok(Json(LoginResponse {
                    status: "success".into(),
                    message: "login account succeeded".into(),
                    jwt: token,
                })
                .into_response());
            }
            Err(e) => {
                log_error!(state.logger, "Cannot generate jwt", "topic" => "account", "method" => "LoginAccount", "error" => e.to_string());
            }
        }
    }
    // wrong secret value or non existing header key/value
    log_info!(state.logger, "Login failed", "topic" => "account", "method" => "LoginAccount",
        "accountId" => account_id, "ipAddress" => client.ip, "ipAddresses" => client.ips_joined(),
        "userAgent" => client.user_agent);
    Err(ApiError::new(
        StatusCode::FORBIDDEN,
        constants::ERROR_INVALID_PARAMETER_VALUE,
        "wrong credentials",
    )
    .validation(ValidationError::new(
        constants::HEADER_API_KEY,
        "header",
        "",
    )))
}

/// List the RBAC users (`GET /api/v1/users/`).
pub async fn list_users(State(state): State<AppState>) -> ApiResult {
    let rbac = state.rbac.rbac().ok_or_else(rbac_not_enabled)?;
    Ok(Json(JsonResultListUsers {
        code: 200,
        users: rbac.get_user_list(),
    })
    .into_response())
}

/// Log in a RBAC user with the `ACCESS-KEY-ID` and `SECRET-ACCESS-KEY` headers (`GET /api/v1/user/login`).
pub async fn login_user(
    State(state): State<AppState>,
    client: ClientInfo,
    headers: HeaderMap,
) -> ApiResult {
    if !state.jwt.is_enabled() {
        return Err(jwt_not_enabled());
    }
    let rbac = state.rbac.rbac().ok_or_else(rbac_not_enabled)?;
    let account_id = state.account.id;
    let access_key_id = header(&headers, constants::HEADER_ACCESS_KEY_ID);
    let secret_access_key = header(&headers, constants::HEADER_SECRET_ACCESS_KEY);
    let wrong_credentials = || {
        log_info!(state.logger, "Login failed", "topic" => "user", "method" => "LoginUser",
            "accountId" => account_id, "accessKeyId" => access_key_id, "ipAddress" => client.ip,
            "ipAddresses" => client.ips_joined(), "userAgent" => client.user_agent);
        ApiError::new(
            StatusCode::FORBIDDEN,
            constants::ERROR_WRONG_CREDENTIALS,
            "wrong credentials",
        )
    };

    if access_key_id.is_empty() || secret_access_key.is_empty() {
        return Err(wrong_credentials());
    }
    let Some(user) = rbac.get_user(access_key_id) else {
        return Err(wrong_credentials());
    };
    match state
        .auth
        .authenticate_user(access_key_id, secret_access_key)
        .await
    {
        Ok(true) => {}
        Ok(false) => return Err(wrong_credentials()),
        Err(e) => {
            log_error!(state.logger, "AuthenticateUser error", "topic" => "user", "method" => "LoginUser",
                "accountId" => account_id, "accessKeyId" => access_key_id, "ipAddress" => client.ip,
                "ipAddresses" => client.ips_joined(), "userAgent" => client.user_agent, "error" => e.to_string());
            return Err(ApiError::internal(
                constants::ERROR_AUTH_INTERNAL_ERROR,
                "authenticate user error",
            ));
        }
    }

    let (token, claims) = state.jwt.generate(false, Some(&user)).map_err(|e| {
        log_error!(state.logger, "Cannot generate jwt", "topic" => "user", "method" => "LoginUser", "error" => e.to_string());
        ApiError::internal(constants::ERROR_AUTH_INTERNAL_ERROR, "authenticate user error")
    })?;
    log_info!(state.logger, "Login succeeded", "topic" => "user", "method" => "LoginUser",
        "accountId" => account_id, "accessKeyId" => access_key_id, "ipAddress" => client.ip,
        "ipAddresses" => client.ips_joined(), "userAgent" => client.user_agent, "jti" => claims.get("jti"));
    Ok(Json(LoginResponse {
        status: "success".into(),
        message: "login user succeeded".into(),
        jwt: token,
    })
    .into_response())
}
