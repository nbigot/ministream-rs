//! Credentials loaded from a JSON file.

use crate::error::{Error, Result};
use crate::log_error;
use crate::logger::Logger;

use super::{parse_credentials, verify_password, warn_unknown_user, Credentials};

pub struct AuthFile {
    credentials: Credentials,
    logger: Logger,
}

impl AuthFile {
    pub fn new(filename: &str, logger: &Logger) -> Result<AuthFile> {
        if filename.is_empty() {
            let err = Error::msg("filename is empty");
            log_error!(logger, "can't initialize credentials", "topic" => "credentials", "method" => "Initialize", "filename" => "", "error" => err.to_string());
            return Err(err);
        }
        let credentials = std::fs::read_to_string(filename)
            .map_err(|e| Error::msg(format!("can't open credential file: {filename}: {e}")))
            .and_then(|document| parse_credentials(&document))
            .inspect_err(|e| {
                log_error!(logger, "can't initialize credentials", "topic" => "credentials", "method" => "Initialize", "filename" => filename, "error" => e.to_string());
            })?;
        Ok(AuthFile {
            credentials,
            logger: logger.clone(),
        })
    }

    pub async fn authenticate_user(&self, user_id: &str, password: &str) -> Result<bool> {
        let hash = self.credentials.get(user_id).cloned();
        if hash.is_none() {
            warn_unknown_user(&self.logger, user_id);
        }
        verify_password(hash, password).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::hash_password;

    #[tokio::test]
    async fn authenticate_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secrets.json");
        let hash = hash_password("sha512", 10, "abc", "secret").unwrap();
        std::fs::write(&path, format!(r#"{{"admin": "{hash}"}}"#)).unwrap();
        let auth = AuthFile::new(path.to_str().unwrap(), &Logger::nop()).unwrap();
        assert!(auth.authenticate_user("admin", "secret").await.unwrap());
        assert!(!auth.authenticate_user("admin", "wrong").await.unwrap());
        assert!(!auth.authenticate_user("nobody", "secret").await.unwrap());
        assert!(AuthFile::new("", &Logger::nop()).is_err());
        assert!(AuthFile::new("does/not/exist.json", &Logger::nop()).is_err());
    }
}
