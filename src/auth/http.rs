//! Credentials loaded from an HTTP endpoint.
//!
//! The endpoint must return a JSON document `{"user": "<pbkdf2 hash>", ...}`.
//! The credentials are cached for `cacheDurationInSec` seconds (no cache when 0).

use std::sync::Arc;
use std::time::{Duration, Instant};

use uuid::Uuid;

use crate::config::AuthHttpConfig;
use crate::error::{Error, Result};

use super::{parse_credentials, verify_password, Credentials};

struct Cache {
    credentials: Arc<Credentials>,
    loaded_at: Instant,
}

pub struct AuthHttp {
    url: String,
    auth_token: String,
    cache_duration: Option<Duration>,
    client: reqwest::Client,
    cache: tokio::sync::Mutex<Option<Cache>>,
}

impl AuthHttp {
    pub fn new(config: &AuthHttpConfig, account_id: &Uuid) -> Result<AuthHttp> {
        if config.url.is_empty() {
            return Err(Error::msg("auth http url is empty"));
        }
        let timeout = match config.timeout {
            t @ 1..=60 => t as u64,
            _ => 60,
        };
        let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(timeout));
        if !config.proxy_url.is_empty() {
            let proxy = reqwest::Proxy::all(&config.proxy_url)
                .map_err(|e| Error::msg(format!("invalid auth http proxy url: {e}")))?;
            builder = builder.proxy(proxy);
        }
        let client = builder
            .build()
            .map_err(|e| Error::msg(format!("cannot create http client: {e}")))?;
        let cache_duration = (config.cache_duration_in_sec > 0)
            .then(|| Duration::from_secs(config.cache_duration_in_sec as u64));
        Ok(AuthHttp {
            url: config.url.replace("{accountid}", &account_id.to_string()),
            auth_token: config.auth_token.clone(),
            cache_duration,
            client,
            cache: tokio::sync::Mutex::new(None),
        })
    }

    async fn load_credentials(&self) -> Result<Credentials> {
        let mut request = self.client.get(&self.url);
        if !self.auth_token.is_empty() {
            request = request.header(
                "Authorization",
                format!("auth_token=\"{}\"", self.auth_token),
            );
        }
        let response = request
            .send()
            .await
            .map_err(|e| Error::msg(format!("LoadCredentialsFromHTTP: {e}")))?;
        let status = response.status();
        if !status.is_success() {
            return Err(Error::msg(format!(
                "LoadCredentialsFromHTTP: invalid response code {}",
                status.as_u16()
            )));
        }
        let body = response
            .text()
            .await
            .map_err(|e| Error::msg(format!("LoadCredentialsFromHTTP: {e}")))?;
        parse_credentials(&body)
    }

    async fn credentials(&self) -> Result<Arc<Credentials>> {
        let mut cache = self.cache.lock().await;
        if let (Some(cached), Some(duration)) = (cache.as_ref(), self.cache_duration) {
            if cached.loaded_at.elapsed() < duration {
                return Ok(cached.credentials.clone());
            }
        }
        let credentials = Arc::new(self.load_credentials().await?);
        *cache = Some(Cache {
            credentials: credentials.clone(),
            loaded_at: Instant::now(),
        });
        Ok(credentials)
    }

    pub async fn authenticate_user(&self, user_id: &str, password: &str) -> Result<bool> {
        let credentials = self.credentials().await?;
        verify_password(credentials.get(user_id).cloned(), password).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::hash_password;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn authenticate_from_http_endpoint() {
        let hash = hash_password("sha256", 5, "salt", "secret").unwrap();
        let body = format!(r#"{{"alice": "{hash}"}}"#);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buffer = vec![0u8; 4096];
                let n = socket.read(&mut buffer).await.unwrap();
                let request = String::from_utf8_lossy(&buffer[..n]).to_string();
                assert!(request
                    .starts_with("GET /account/00000000-0000-0000-0000-000000000000/secrets"));
                assert!(request
                    .to_lowercase()
                    .contains("authorization: auth_token=\"token\""));
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });

        let config = AuthHttpConfig {
            url: format!("http://{address}/account/{{accountid}}/secrets"),
            auth_token: "token".into(),
            timeout: 5,
            cache_duration_in_sec: 0,
            ..Default::default()
        };
        let auth = AuthHttp::new(&config, &Uuid::nil()).unwrap();
        assert!(auth.authenticate_user("alice", "secret").await.unwrap());
        assert!(!auth.authenticate_user("bob", "secret").await.unwrap());
        server.await.unwrap();
        // the endpoint is gone: the credentials cannot be loaded anymore
        assert!(auth.authenticate_user("alice", "secret").await.is_err());
    }
}
