//! JSON Web Tokens: generation, validation and the middleware protecting the routes.

use std::convert::Infallible;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use axum::extract::{FromRequestParts, Request, State};
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use chrono::{Duration, Utc};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use rand::RngCore;
use serde_json::{Map, Value};
use uuid::Uuid;

use super::AppState;
use crate::apierror::ApiError;
use crate::config::JwtConfig;
use crate::constants;
use crate::error::{Error, Result};
use crate::rbac::{Role, User};

pub type Claims = Map<String, Value>;

/// Why a token was rejected.
#[derive(Debug, PartialEq, Eq)]
pub enum TokenError {
    /// The token cannot be decoded, its signature is invalid or it is expired.
    InvalidOrExpired,
    /// The token claims do not match this server.
    InvalidClaims,
    /// The token was emitted before the revocation date.
    Revoked,
}

pub struct JwtManager {
    enabled: bool,
    iss: String,
    sub: String,
    aud: String,
    account_id: String,
    expire_in_minutes: i64,
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    validation: Validation,
    /// Tokens issued before this unix timestamp (seconds) are revoked.
    revoked_before: AtomicI64,
}

impl JwtManager {
    /// Create the JWT manager. The `account` claim is the configured `accountId`,
    /// or the account id of the server when it is not configured.
    pub fn new(config: &JwtConfig, default_account_id: &Uuid) -> JwtManager {
        let secret = if config.secret_key.is_empty() {
            // note: a new key generated at startup invalidates all the tokens previously
            // generated, therefore all the applications/users have to authenticate again
            let mut key = vec![0u8; 256];
            rand::rng().fill_bytes(&mut key);
            key
        } else {
            config.secret_key.as_bytes().to_vec()
        };
        let mut validation = Validation::new(Algorithm::HS256);
        validation.leeway = 0;
        validation.validate_aud = false;
        validation.set_required_spec_claims(&["exp"]);
        JwtManager {
            enabled: config.enable,
            iss: config.iss.clone(),
            sub: config.sub.clone(),
            aud: config.aud.clone(),
            account_id: if config.account_id.is_empty() {
                default_account_id.to_string()
            } else {
                config.account_id.clone()
            },
            expire_in_minutes: if config.token_expire_in_minutes > 0 {
                config.token_expire_in_minutes
            } else {
                60
            },
            encoding_key: EncodingKey::from_secret(&secret),
            decoding_key: DecodingKey::from_secret(&secret),
            validation,
            revoked_before: AtomicI64::new(
                config
                    .revoke_emitted_before_date
                    .map(|d| d.timestamp())
                    .unwrap_or(i64::MIN),
            ),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Generate a token for the super user or for a RBAC user.
    pub fn generate(&self, superuser: bool, user: Option<&User>) -> Result<(String, Claims)> {
        let now = Utc::now();
        let mut claims = Claims::new();
        claims.insert("iss".into(), self.iss.clone().into());
        claims.insert("sub".into(), self.sub.clone().into());
        claims.insert("aud".into(), self.aud.clone().into());
        claims.insert(
            "exp".into(),
            (now + Duration::minutes(self.expire_in_minutes))
                .timestamp()
                .into(),
        );
        claims.insert("iat".into(), now.timestamp().into());
        claims.insert("jti".into(), Uuid::new_v4().to_string().into());
        claims.insert(
            constants::JWT_CLAIMS_ACCOUNT_KEY.into(),
            self.account_id.clone().into(),
        );
        if superuser {
            claims.insert(constants::JWT_CLAIMS_SUPERUSER_KEY.into(), true.into());
        }
        if let Some(user) = user {
            claims.insert(
                constants::JWT_CLAIMS_USER_KEY.into(),
                user.id.clone().into(),
            );
            claims.insert(
                constants::JWT_CLAIMS_ROLES_KEY.into(),
                user.role_ids().into(),
            );
        }
        let token =
            jsonwebtoken::encode(&Header::new(Algorithm::HS256), &claims, &self.encoding_key)
                .map_err(|e| Error::msg(format!("cannot sign jwt: {e}")))?;
        Ok((token, claims))
    }

    /// Validate a token and return its claims.
    pub fn validate(&self, token: &str) -> std::result::Result<Claims, TokenError> {
        let claims = jsonwebtoken::decode::<Claims>(token, &self.decoding_key, &self.validation)
            .map_err(|_| TokenError::InvalidOrExpired)?
            .claims;
        let claim_is =
            |key: &str, expected: &str| claims.get(key).and_then(Value::as_str) == Some(expected);
        if !(claim_is("iss", &self.iss)
            && claim_is("sub", &self.sub)
            && claim_is("aud", &self.aud)
            && claim_is(constants::JWT_CLAIMS_ACCOUNT_KEY, &self.account_id))
        {
            return Err(TokenError::InvalidClaims);
        }
        let issued_at = claims
            .get("iat")
            .and_then(Value::as_f64)
            .ok_or(TokenError::InvalidClaims)?;
        if (issued_at as i64) < self.revoked_before.load(Ordering::SeqCst) {
            return Err(TokenError::Revoked);
        }
        Ok(claims)
    }

    /// Revoke all the tokens emitted until now.
    pub fn revoke_all(&self) {
        self.revoked_before
            .store(Utc::now().timestamp(), Ordering::SeqCst);
    }
}

/// Authentication information of a request (set by the JWT middleware).
#[derive(Debug, Clone, Default)]
pub struct AuthContext {
    pub claims: Option<Arc<Claims>>,
    pub superuser: bool,
    pub user: Option<String>,
    /// Roles of the user (only when RBAC is enabled).
    pub roles: Option<Vec<Arc<Role>>>,
}

impl AuthContext {
    fn claim(&self, key: &str) -> Option<String> {
        self.claims
            .as_ref()
            .and_then(|c| c.get(key))
            .and_then(Value::as_str)
            .map(str::to_string)
    }

    pub fn account_id(&self) -> Option<String> {
        self.claim(constants::JWT_CLAIMS_ACCOUNT_KEY)
    }

    pub fn jti(&self) -> Option<String> {
        self.claim("jti")
    }
}

/// Extractor of the [`AuthContext`] (default context when the route is not protected).
pub struct Auth(pub AuthContext);

impl<S: Send + Sync> FromRequestParts<S> for Auth {
    type Rejection = Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> std::result::Result<Self, Self::Rejection> {
        Ok(Auth(
            parts
                .extensions
                .get::<AuthContext>()
                .cloned()
                .unwrap_or_default(),
        ))
    }
}

/// Extract the token of the "Authorization: Bearer <token>" header.
fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let value = headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    let scheme = "Bearer";
    if value.len() > scheme.len() + 1 && value[..scheme.len()].eq_ignore_ascii_case(scheme) {
        let token = value[scheme.len()..].trim();
        (!token.is_empty()).then_some(token)
    } else {
        None
    }
}

/// Middleware protecting routes with a JWT (does nothing when JWT is disabled).
pub async fn jwt_protected(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    if !state.jwt.is_enabled() {
        request.extensions_mut().insert(AuthContext::default());
        return next.run(request).await;
    }

    let Some(token) = bearer_token(request.headers()) else {
        return ApiError::bad_request(
            constants::ERROR_JWT_MISSING_OR_MALFORMED,
            "missing or malformed jwt",
        )
        .into_response();
    };
    let claims = match state.jwt.validate(token) {
        Ok(claims) => claims,
        Err(error) => {
            let message = match error {
                TokenError::InvalidOrExpired => "invalid or expired jwt",
                TokenError::InvalidClaims => "invalid jwt",
                TokenError::Revoked => "jwt was revoked",
            };
            return ApiError::new(
                StatusCode::UNAUTHORIZED,
                constants::ERROR_JWT_INVALID_OR_EXPIRED,
                message,
            )
            .into_response();
        }
    };

    let mut context = AuthContext {
        superuser: claims.get(constants::JWT_CLAIMS_SUPERUSER_KEY) == Some(&Value::Bool(true)),
        user: claims
            .get(constants::JWT_CLAIMS_USER_KEY)
            .and_then(Value::as_str)
            .map(str::to_string),
        ..Default::default()
    };
    if !context.superuser {
        if let Some(rbac) = state.rbac.rbac() {
            let role_names: Vec<String> = claims
                .get(constants::JWT_CLAIMS_ROLES_KEY)
                .and_then(Value::as_array)
                .map(|roles| {
                    roles
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();
            match rbac.get_roles(&role_names) {
                Ok(roles) => context.roles = Some(roles),
                Err(e) => {
                    return ApiError::new(
                        StatusCode::FORBIDDEN,
                        constants::ERROR_JWT_RBAC_UNKNOWN_ROLE,
                        "invalid jwt",
                    )
                    .details(e)
                    .into_response();
                }
            }
        }
    }
    context.claims = Some(Arc::new(claims));
    request.extensions_mut().insert(context);
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manager(config: JwtConfig) -> JwtManager {
        JwtManager::new(&config, &Uuid::nil())
    }

    fn config() -> JwtConfig {
        JwtConfig {
            enable: true,
            secret_key: "secret".into(),
            token_expire_in_minutes: 5,
            iss: "ministream".into(),
            sub: "auth".into(),
            aud: "ministream".into(),
            ..Default::default()
        }
    }

    #[test]
    fn generate_and_validate() {
        let jwt = manager(config());
        let (token, claims) = jwt.generate(true, None).unwrap();
        assert_eq!(claims["superuser"], true);
        assert_eq!(claims["account"], Uuid::nil().to_string());
        let validated = jwt.validate(&token).unwrap();
        assert_eq!(validated["jti"], claims["jti"]);
        assert_eq!(
            jwt.validate("not.a.token"),
            Err(TokenError::InvalidOrExpired)
        );
    }

    #[test]
    fn tokens_of_another_server_are_rejected() {
        let (token, _) = manager(config()).generate(true, None).unwrap();
        let other_secret = manager(JwtConfig {
            secret_key: "other".into(),
            ..config()
        });
        assert_eq!(
            other_secret.validate(&token),
            Err(TokenError::InvalidOrExpired)
        );
        let other_issuer = manager(JwtConfig {
            iss: "other".into(),
            ..config()
        });
        assert_eq!(
            other_issuer.validate(&token),
            Err(TokenError::InvalidClaims)
        );
        let other_account = manager(JwtConfig {
            account_id: "other".into(),
            ..config()
        });
        assert_eq!(
            other_account.validate(&token),
            Err(TokenError::InvalidClaims)
        );
    }

    #[test]
    fn revocation() {
        let mut revoked = config();
        revoked.revoke_emitted_before_date = Some(Utc::now() + Duration::hours(1));
        let jwt = manager(revoked);
        let (token, _) = jwt.generate(true, None).unwrap();
        assert_eq!(jwt.validate(&token), Err(TokenError::Revoked));

        let jwt = manager(config());
        let mut claims = jwt.generate(true, None).unwrap().1;
        claims.insert("iat".into(), (Utc::now().timestamp() - 10).into());
        let old_token =
            jsonwebtoken::encode(&Header::new(Algorithm::HS256), &claims, &jwt.encoding_key)
                .unwrap();
        assert!(jwt.validate(&old_token).is_ok());
        jwt.revoke_all();
        assert_eq!(jwt.validate(&old_token), Err(TokenError::Revoked));
    }

    #[test]
    fn bearer_header() {
        let mut headers = HeaderMap::new();
        assert_eq!(bearer_token(&headers), None);
        headers.insert("authorization", "Bearer abc".parse().unwrap());
        assert_eq!(bearer_token(&headers), Some("abc"));
        headers.insert("authorization", "bearer  xyz ".parse().unwrap());
        assert_eq!(bearer_token(&headers), Some("xyz"));
        headers.insert("authorization", "Basic abc".parse().unwrap());
        assert_eq!(bearer_token(&headers), None);
        headers.insert("authorization", "Bearer".parse().unwrap());
        assert_eq!(bearer_token(&headers), None);
    }
}
