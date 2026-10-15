//! RBAC middleware (and the audit log of the access decisions).

use std::collections::BTreeMap;
use std::convert::Infallible;

use axum::extract::{FromRequestParts, RawPathParams, Request, State};
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde_json::{json, Value};
use uuid::Uuid;

use super::client::ClientInfo;
use super::jwt::AuthContext;
use super::AppState;
use crate::apierror::ApiError;
use crate::constants;
use crate::log_error;
use crate::rbac::Abac;

/// RBAC decision of a request (available to the handlers).
#[derive(Debug, Clone, Default)]
pub struct RbacContext {
    /// Attributes of the decision: user, role, rule, action and abac.
    pub info: BTreeMap<&'static str, String>,
    /// ABAC rule the handler must apply on the resources it returns (ex: list of streams).
    pub abac: Option<Abac>,
}

/// Extractor of the [`RbacContext`] (default context when RBAC is not applied).
pub struct Rbac(pub RbacContext);

impl<S: Send + Sync> FromRequestParts<S> for Rbac {
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(Rbac(
            parts
                .extensions
                .get::<RbacContext>()
                .cloned()
                .unwrap_or_default(),
        ))
    }
}

/// State of the RBAC middleware of a route.
#[derive(Clone)]
pub struct RbacRoute {
    pub state: AppState,
    pub action: &'static str,
    /// When true, the ABAC rules are evaluated on the stream of the route
    /// (`{"streamUUID": "...", "properties": {...}}`), otherwise they are handed to the handler.
    pub abac_on_stream: bool,
}

fn audit_fields(
    auth: &AuthContext,
    client: &ClientInfo,
    info: &BTreeMap<&'static str, String>,
) -> [(&'static str, Value); 5] {
    [
        ("rbac", json!(info)),
        ("accountId", json!(auth.account_id().unwrap_or_default())),
        ("jti", json!(auth.jti().unwrap_or_default())),
        ("ipAddress", json!(client.ip)),
        ("ipAddresses", json!(client.ips_joined())),
    ]
}

fn audit(
    state: &AppState,
    message: &str,
    auth: &AuthContext,
    client: &ClientInfo,
    info: &BTreeMap<&'static str, String>,
) {
    let mut fields: Vec<(&str, Value)> =
        vec![("topic", json!("rbac")), ("method", json!("RBACHandler"))];
    fields.extend(audit_fields(auth, client, info));
    state
        .logger
        .log(crate::logger::Level::Info, message, &fields);
}

/// Attributes of the stream of the route, used to evaluate the ABAC rules.
fn stream_attributes(state: &AppState, params: &RawPathParams) -> Option<Value> {
    let raw = params
        .iter()
        .find(|(key, _)| *key == constants::PARAM_NAME_STREAM_UUID)
        .map(|(_, value)| value)?;
    let uuid = Uuid::parse_str(raw).ok()?;
    let stream = state.service.get_stream(&uuid)?;
    Some(json!({
        "streamUUID": uuid.to_string(),
        "properties": stream.properties(),
    }))
}

/// Check that the user of the JWT is allowed to perform the action of the route.
pub async fn rbac_middleware(
    State(route): State<RbacRoute>,
    params: RawPathParams,
    client: ClientInfo,
    mut request: Request,
    next: Next,
) -> Response {
    let state = &route.state;
    if !state.rbac.is_enabled() {
        return next.run(request).await;
    }
    let auth = request
        .extensions()
        .get::<AuthContext>()
        .cloned()
        .unwrap_or_default();
    if auth.superuser {
        return next.run(request).await;
    }
    let audit_enabled = state.config.audit_log.enable;

    let Some(roles) = auth.roles.clone() else {
        let details = "roles of the user are unknown (is JWT enabled?)";
        if audit_enabled {
            log_error!(state.logger, "RBAC internal error", "topic" => "rbac", "method" => "RBACHandler", "action" => route.action, "ipAddress" => client.ip, "details" => details);
        }
        return ApiError::internal(constants::ERROR_RBAC_INVALID_RULE, "rbac error")
            .details(details)
            .into_response();
    };
    let user = auth.user.clone().unwrap_or_default();

    let mut attributes: Option<Option<Value>> = None;
    for role in &roles {
        for rule in &role.rules {
            if !rule.actions.iter().any(|a| a == route.action) {
                continue;
            }
            let mut info = BTreeMap::from([
                ("user", user.clone()),
                ("role", role.id.clone()),
                ("rule", rule.id.clone()),
                ("action", route.action.to_string()),
            ]);
            let mut context_abac = rule.abac.clone();
            if let (Some(abac), true) = (&rule.abac, route.abac_on_stream) {
                let Some(attributes) = attributes
                    .get_or_insert_with(|| stream_attributes(state, &params))
                    .as_ref()
                else {
                    // unknown stream: this rule cannot grant the action
                    continue;
                };
                match abac.check(attributes) {
                    Ok(true) => {
                        info.insert("abac", abac.jq_def.clone());
                        context_abac = None;
                    }
                    Ok(false) => continue,
                    Err(e) => {
                        if audit_enabled {
                            log_error!(state.logger, "RBAC internal error", "topic" => "rbac", "method" => "RBACHandler", "rbac" => info, "ipAddress" => client.ip, "details" => e.to_string());
                        }
                        return ApiError::internal(
                            constants::ERROR_RBAC_INVALID_RULE,
                            "rbac error",
                        )
                        .details(format!("abac error: {e}"))
                        .into_response();
                    }
                }
            }

            // action authorized
            if audit_enabled && state.config.audit_log.enable_log_access_granted {
                audit(state, "RBAC Access granted", &auth, &client, &info);
            }
            request.extensions_mut().insert(RbacContext {
                info,
                abac: context_abac,
            });
            return next.run(request).await;
        }
    }

    // action not authorized
    if audit_enabled {
        let info = BTreeMap::from([("user", user), ("action", route.action.to_string())]);
        audit(state, "RBAC Access denied", &auth, &client, &info);
    }
    ApiError::new(
        StatusCode::FORBIDDEN,
        constants::ERROR_RBAC_FORBIDDEN,
        "rbac action forbidden",
    )
    .details(route.action)
    .into_response()
}
