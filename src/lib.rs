//! Ministream is a simple real-time data stream service.
//!
//! Records are pushed and pulled with simple HTTP requests, streams are stored
//! in memory, in JSON lines files or in MySQL, and the whole service is driven
//! by a single YAML configuration file.

pub mod account;
pub mod apierror;
pub mod auth;
pub mod buffering;
pub mod cli;
pub mod config;
pub mod constants;
pub mod error;
pub mod generators;
pub mod humanize;
pub mod jq;
pub mod logger;
pub mod metrics;
pub mod rbac;
pub mod service;
pub mod storage;
pub mod stream;
pub mod types;
pub mod web;

pub use error::{Error, Result};

/// Version of the program.
///
/// It can be overridden at compile time with the `MINISTREAM_VERSION`
/// environment variable (ex: `MINISTREAM_VERSION=v1.2.3 cargo build --release`).
pub fn version() -> &'static str {
    match option_env!("MINISTREAM_VERSION") {
        Some(v) if !v.is_empty() => v,
        _ => concat!("v", env!("CARGO_PKG_VERSION")),
    }
}
