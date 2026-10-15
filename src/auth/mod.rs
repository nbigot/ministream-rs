//! User authentication.
//!
//! Users credentials are PBKDF2 hashes stored in a JSON document (`{"user": "<hash>"}`)
//! loaded either from a file (method `FILE`) or from an HTTP endpoint (method `HTTP`).

pub mod file;
pub mod http;
pub mod pbkdf2;

use std::collections::HashMap;

use uuid::Uuid;

use crate::config::AuthConfig;
use crate::error::{Error, Result};
use crate::logger::Logger;
use crate::{log_info, log_warn};

pub use self::pbkdf2::{hash_password, Pbkdf2Hash};

/// Users credentials indexed by user id.
pub type Credentials = HashMap<String, Pbkdf2Hash>;

/// Parse a credentials JSON document (`{"user": "$pbkdf2-sha512$i=...$salt$hash"}`).
///
/// Keys starting with an underscore are comments and are ignored.
pub fn parse_credentials(document: &str) -> Result<Credentials> {
    let map: serde_json::Map<String, serde_json::Value> = serde_json::from_str(document)?;
    let mut credentials = Credentials::new();
    for (user, value) in map {
        if user.starts_with('_') {
            continue;
        }
        let hash = value
            .as_str()
            .ok_or_else(|| Error::msg(format!("invalid hash for user {user}: not a string")))?;
        let hash = Pbkdf2Hash::parse(hash)
            .map_err(|e| Error::msg(format!("invalid hash for user {user}: {e}")))?;
        credentials.insert(user, hash);
    }
    Ok(credentials)
}

/// Verify a password against the credentials (runs on the blocking thread pool
/// because PBKDF2 is CPU intensive by design).
pub(crate) async fn verify_password(hash: Option<Pbkdf2Hash>, password: &str) -> Result<bool> {
    let Some(hash) = hash else {
        return Ok(false);
    };
    let password = password.to_string();
    tokio::task::spawn_blocking(move || hash.verify(&password))
        .await
        .map_err(|e| Error::msg(format!("password verification failed: {e}")))
}

enum AuthMethod {
    File(file::AuthFile),
    Http(http::AuthHttp),
}

/// Authentication manager.
pub struct AuthManager {
    enabled: bool,
    method: Option<AuthMethod>,
}

impl AuthManager {
    /// An authentication manager that grants every user (authentication disabled).
    pub fn disabled() -> AuthManager {
        AuthManager {
            enabled: false,
            method: None,
        }
    }

    pub fn new(config: &AuthConfig, account_id: &Uuid, logger: &Logger) -> Result<AuthManager> {
        if !config.enable {
            log_info!(logger, "Authentication is disabled in configuration", "topic" => "auth");
            return Ok(AuthManager::disabled());
        }
        let method = match config.method.to_ascii_uppercase().as_str() {
            "FILE" => AuthMethod::File(file::AuthFile::new(&config.methods.file.filename, logger)?),
            "HTTP" => AuthMethod::Http(http::AuthHttp::new(&config.methods.http, account_id)?),
            other => return Err(Error::msg(format!("auth method not found: {other}"))),
        };
        log_info!(logger, "Authentication enabled", "topic" => "auth", "method" => config.method.to_ascii_uppercase());
        Ok(AuthManager {
            enabled: true,
            method: Some(method),
        })
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Authenticate a user.
    ///
    /// Returns `Ok(false)` for unknown users or wrong passwords, and an error only when
    /// the credentials cannot be checked (ex: the HTTP credentials endpoint is down).
    /// Warning: always grants when the authentication is disabled.
    pub async fn authenticate_user(&self, user_id: &str, password: &str) -> Result<bool> {
        match &self.method {
            _ if !self.enabled => Ok(true),
            Some(AuthMethod::File(m)) => m.authenticate_user(user_id, password).await,
            Some(AuthMethod::Http(m)) => m.authenticate_user(user_id, password).await,
            None => Err(Error::msg("authentication method is not initialized")),
        }
    }
}

pub(crate) fn warn_unknown_user(logger: &Logger, user_id: &str) {
    log_warn!(logger, "Unknown user", "topic" => "auth", "user" => user_id);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_document() {
        let hash = hash_password("sha256", 2, "salt", "pwd").unwrap();
        let doc = format!(r#"{{"_WARNING": "example only", "alice": "{hash}"}}"#);
        let creds = parse_credentials(&doc).unwrap();
        assert_eq!(creds.len(), 1);
        assert!(creds["alice"].verify("pwd"));
        assert!(parse_credentials(r#"{"bob": "not a hash"}"#).is_err());
        assert!(parse_credentials(r#"{"bob": 12}"#).is_err());
    }

    #[tokio::test]
    async fn disabled_manager_grants_everybody() {
        let m = AuthManager::disabled();
        assert!(m.authenticate_user("anyone", "anything").await.unwrap());
    }

    #[test]
    fn unknown_method() {
        let config = AuthConfig {
            enable: true,
            method: "VAULT".into(),
            ..Default::default()
        };
        assert!(AuthManager::new(&config, &Uuid::nil(), &Logger::nop()).is_err());
    }
}
