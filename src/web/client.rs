//! Information about the HTTP client of a request.

use std::convert::Infallible;
use std::net::SocketAddr;

use axum::extract::{ConnectInfo, FromRequestParts};
use axum::http::request::Parts;
use axum::http::{Extensions, HeaderMap};

/// Remote address, forwarded addresses and user agent of the client.
#[derive(Debug, Clone, Default)]
pub struct ClientInfo {
    pub ip: String,
    pub port: u16,
    /// Addresses of the `X-Forwarded-For` header.
    pub ips: Vec<String>,
    pub user_agent: String,
}

impl ClientInfo {
    pub fn from_parts(headers: &HeaderMap, extensions: &Extensions) -> ClientInfo {
        let (ip, port) = extensions
            .get::<ConnectInfo<SocketAddr>>()
            .map(|ConnectInfo(addr)| (addr.ip().to_string(), addr.port()))
            .unwrap_or_default();
        let ips = headers
            .get_all("x-forwarded-for")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .collect();
        let user_agent = headers
            .get(axum::http::header::USER_AGENT)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        ClientInfo {
            ip,
            port,
            ips,
            user_agent,
        }
    }

    /// Forwarded addresses joined with ";".
    pub fn ips_joined(&self) -> String {
        self.ips.join(";")
    }
}

impl<S: Send + Sync> FromRequestParts<S> for ClientInfo {
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(ClientInfo::from_parts(&parts.headers, &parts.extensions))
    }
}
