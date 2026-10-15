//! Helpers of the integration tests: the web API is called without network
//! (requests are sent directly to the router).

#![allow(dead_code)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use ministream::config::Config;
use ministream::logger::Logger;
use ministream::web::{build_router, AppState, ServerCommand};
use serde_json::Value;
use tokio::sync::mpsc::UnboundedReceiver;
use tower::ServiceExt;
use uuid::Uuid;

pub struct TestApp {
    pub router: Router,
    pub state: AppState,
    pub commands: UnboundedReceiver<ServerCommand>,
}

pub struct TestResponse {
    pub status: StatusCode,
    pub headers: axum::http::HeaderMap,
    pub text: String,
}

impl TestResponse {
    pub fn json(&self) -> Value {
        serde_json::from_str(&self.text)
            .unwrap_or_else(|e| panic!("invalid json response ({e}): {}", self.text))
    }
}

pub async fn app(config_yaml: &str) -> TestApp {
    let config = Config::from_yaml_str(config_yaml).expect("valid configuration");
    config.validate().expect("valid configuration");
    let (state, commands) = AppState::from_config(Arc::new(config), Logger::nop())
        .await
        .expect("server state");
    TestApp {
        router: build_router(state.clone()),
        state,
        commands,
    }
}

impl TestApp {
    pub async fn call(
        &self,
        method: &str,
        uri: &str,
        headers: &[(&str, &str)],
        body: impl Into<Body>,
    ) -> TestResponse {
        let mut builder = Request::builder().method(method).uri(uri);
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        let response = self
            .router
            .clone()
            .oneshot(builder.body(body.into()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        TestResponse {
            status,
            headers,
            text: String::from_utf8_lossy(&bytes).to_string(),
        }
    }

    pub async fn get(&self, uri: &str, headers: &[(&str, &str)]) -> TestResponse {
        self.call("GET", uri, headers, Body::empty()).await
    }

    pub async fn send_json(
        &self,
        method: &str,
        uri: &str,
        headers: &[(&str, &str)],
        body: Value,
    ) -> TestResponse {
        let mut all_headers = vec![("content-type", "application/json")];
        all_headers.extend_from_slice(headers);
        self.call(method, uri, &all_headers, body.to_string()).await
    }

    /// Wait until the stream has the expected count of readable records.
    pub async fn wait_readable(&self, stream_uuid: &str, count: u64) {
        let uuid = Uuid::parse_str(stream_uuid).unwrap();
        let start = Instant::now();
        loop {
            let stream = self.state.service.get_stream(&uuid).expect("stream exists");
            if stream.info().readable_messages.cpt_messages >= count {
                return;
            }
            assert!(
                start.elapsed() < Duration::from_secs(10),
                "records are not readable"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// Configuration of a server without security.
pub fn open_config(storage: &str) -> String {
    format!(
        r#"
logger:
    level: "off"
storage:
    logger:
        level: "off"
{storage}
streams:
    bulkFlushFrequency: 0
    bulkMaxSize: 100
    channelBufferSize: 1000
    maxAllowedIteratorsPerStream: 10
    maxMessagePerGetOperation: 100
webserver:
    jwt:
        enable: false
"#
    )
}

pub const IN_MEMORY_STORAGE: &str = r#"    type: "InMemory"
    inmemory:
        maxRecordsByStream: 0
        maxSize: "1gb""#;
