//! Fixed window rate limiter middleware.
//!
//! Each account has its own limits per route and HTTP method, whatever the IP address
//! the requests come from (key example: `<account id>#/api/v1/stream/<uuid>/records#PUT`).
//! Failed requests (status code >= 400) are not counted.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use parking_lot::Mutex;

use super::jwt::AuthContext;

/// Above this count of keys, the expired windows are removed.
const PRUNE_THRESHOLD: usize = 10_000;

#[derive(Debug, Clone)]
pub enum LimiterKey {
    /// One window per account, path and method.
    AccountPathMethod,
    /// A single window shared by every request.
    Global(&'static str),
}

struct Window {
    hits: u64,
    reset_at: Instant,
}

pub struct RateLimiter {
    enabled: bool,
    max: u64,
    expiration: Duration,
    key: LimiterKey,
    windows: Mutex<HashMap<String, Window>>,
}

impl RateLimiter {
    /// Create a rate limiter allowing `max_requests` per `duration_in_seconds`
    /// (defaults: 5 requests per minute).
    pub fn new(
        enabled: bool,
        max_requests: i64,
        duration_in_seconds: i64,
        key: LimiterKey,
    ) -> Arc<RateLimiter> {
        Arc::new(RateLimiter {
            enabled,
            max: if max_requests > 0 {
                max_requests as u64
            } else {
                5
            },
            expiration: Duration::from_secs(if duration_in_seconds > 0 {
                duration_in_seconds as u64
            } else {
                60
            }),
            key,
            windows: Mutex::new(HashMap::new()),
        })
    }

    fn key(&self, request: &Request) -> String {
        match &self.key {
            LimiterKey::Global(key) => key.to_string(),
            LimiterKey::AccountPathMethod => {
                let account = request
                    .extensions()
                    .get::<AuthContext>()
                    .and_then(AuthContext::account_id)
                    // the API is not protected by JWT, therefore no account is used
                    .unwrap_or_else(|| "noaccount".to_string());
                format!("{}#{}#{}", account, request.uri().path(), request.method())
            }
        }
    }

    /// Count a hit, returns the hits count of the window and its reset time.
    fn hit(&self, key: &str) -> (u64, Instant) {
        let now = Instant::now();
        let mut windows = self.windows.lock();
        if windows.len() > PRUNE_THRESHOLD {
            windows.retain(|_, w| w.reset_at > now);
        }
        let window = windows.entry(key.to_string()).or_insert(Window {
            hits: 0,
            reset_at: now + self.expiration,
        });
        if now >= window.reset_at {
            window.hits = 0;
            window.reset_at = now + self.expiration;
        }
        window.hits += 1;
        (window.hits, window.reset_at)
    }

    fn cancel_hit(&self, key: &str, reset_at: Instant) {
        if let Some(window) = self.windows.lock().get_mut(key) {
            if window.reset_at == reset_at && window.hits > 0 {
                window.hits -= 1;
            }
        }
    }
}

pub async fn rate_limit(
    State(limiter): State<Arc<RateLimiter>>,
    request: Request,
    next: Next,
) -> Response {
    if !limiter.enabled {
        return next.run(request).await;
    }
    let key = limiter.key(&request);
    let (hits, reset_at) = limiter.hit(&key);
    let reset_in = reset_at
        .saturating_duration_since(Instant::now())
        .as_secs_f64()
        .ceil() as u64;
    if hits > limiter.max {
        let mut response = (StatusCode::TOO_MANY_REQUESTS, "Too Many Requests").into_response();
        response
            .headers_mut()
            .insert("Retry-After", HeaderValue::from(reset_in));
        return response;
    }

    let mut response = next.run(request).await;
    if response.status().as_u16() >= 400 {
        // failed requests are not counted
        limiter.cancel_hit(&key, reset_at);
    }
    let headers = response.headers_mut();
    headers.insert("X-RateLimit-Limit", HeaderValue::from(limiter.max));
    headers.insert(
        "X-RateLimit-Remaining",
        HeaderValue::from(limiter.max.saturating_sub(hits)),
    );
    headers.insert("X-RateLimit-Reset", HeaderValue::from(reset_in));
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows() {
        let limiter = RateLimiter::new(true, 2, 60, LimiterKey::Global("k"));
        assert_eq!(limiter.hit("a").0, 1);
        let (hits, reset_at) = limiter.hit("a");
        assert_eq!(hits, 2);
        limiter.cancel_hit("a", reset_at);
        assert_eq!(limiter.hit("a").0, 2);
        assert_eq!(limiter.hit("a").0, 3);
        assert_eq!(limiter.hit("b").0, 1);
    }

    #[test]
    fn defaults() {
        let limiter = RateLimiter::new(true, 0, 0, LimiterKey::Global("k"));
        assert_eq!(limiter.max, 5);
        assert_eq!(limiter.expiration, Duration::from_secs(60));
    }
}
