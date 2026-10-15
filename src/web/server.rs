//! HTTP(S) server: start the web API, wait for a stop request, then stop gracefully.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum_server::tls_rustls::RustlsConfig;
use axum_server::Handle;
use tokio::task::JoinSet;

use super::{build_router, AppState, ServerCommand};
use crate::config::Config;
use crate::error::{Error, Result};
use crate::logger::Logger;
use crate::{log_error, log_info, log_warn};

/// How the server stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerExit {
    /// The server must stop.
    Shutdown,
    /// The server must be restarted (the configuration file is reloaded).
    Restart,
}

/// Maximum duration to wait for the pending requests when the server stops.
const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// Resolve a listen address (`host:port`, `:port` meaning every interface).
pub async fn resolve_address(address: &str) -> Result<SocketAddr> {
    let address = address.trim();
    let address = if address.starts_with(':') {
        format!("0.0.0.0{address}")
    } else {
        address.to_string()
    };
    if let Ok(socket) = address.parse::<SocketAddr>() {
        return Ok(socket);
    }
    let mut resolved = tokio::net::lookup_host(address.as_str())
        .await
        .map_err(|e| Error::msg(format!("invalid address {address}: {e}")))?;
    resolved
        .next()
        .ok_or_else(|| Error::msg(format!("invalid address {address}")))
}

/// Install the cryptographic provider used by TLS (HTTPS server and HTTPS clients).
pub fn install_crypto_provider() {
    // an error means a provider is already installed
    let _ = rustls::crypto::ring::default_provider().install_default();
}

enum Stop {
    Exit(ServerExit),
    Failure(Error),
}

#[cfg(unix)]
async fn wait_for_signal(logger: &Logger) -> ServerExit {
    use tokio::signal::unix::{signal, SignalKind};
    let (Ok(mut terminate), Ok(mut hangup), Ok(mut quit)) = (
        signal(SignalKind::terminate()),
        signal(SignalKind::hangup()),
        signal(SignalKind::quit()),
    ) else {
        let _ = tokio::signal::ctrl_c().await;
        return ServerExit::Shutdown;
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            log_info!(logger, "Received SIGINT (Ctrl+C), shutting down server...", "topic" => "server", "method" => "HandleSignals");
            ServerExit::Shutdown
        }
        _ = terminate.recv() => {
            log_info!(logger, "Received SIGTERM, shutting down server...", "topic" => "server", "method" => "HandleSignals");
            ServerExit::Shutdown
        }
        _ = quit.recv() => {
            log_info!(logger, "Received SIGQUIT, shutting down server...", "topic" => "server", "method" => "HandleSignals");
            ServerExit::Shutdown
        }
        _ = hangup.recv() => {
            log_info!(logger, "Reloading configuration and restart server...", "topic" => "server", "method" => "HandleSignals");
            ServerExit::Restart
        }
    }
}

#[cfg(not(unix))]
async fn wait_for_signal(logger: &Logger) -> ServerExit {
    let _ = tokio::signal::ctrl_c().await;
    log_info!(logger, "Received Ctrl+C, shutting down server...", "topic" => "server", "method" => "HandleSignals");
    ServerExit::Shutdown
}

/// Run the server until it is asked to stop or to restart.
pub async fn run_server(config: Config) -> Result<ServerExit> {
    install_crypto_provider();
    let config = Arc::new(config);
    let logger = Logger::new(&config.logger)
        .map_err(|e| Error::msg(format!("invalid logger configuration: {e}")))?;
    log_info!(logger, "Server version", "topic" => "server", "version" => crate::version());

    if !config.auth.enable && (config.rbac.enable || config.web_server.jwt.enable) {
        log_warn!(logger, "Auth is disabled in configuration", "topic" => "server");
    }

    let (state, mut commands) = AppState::from_config(config.clone(), logger.clone()).await?;
    let router = build_router(state.clone());

    // forget the expired batch ids periodically
    let dedup_cleanup = tokio::spawn({
        let state = state.clone();
        async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                state.dedup.cleanup();
            }
        }
    });

    let mut handles = Vec::new();
    let mut servers = JoinSet::new();
    let web = &config.web_server;
    let start = async {
        if web.http.enable {
            let address = resolve_address(&web.http.address).await?;
            log_info!(logger, "Start HTTP web server", "topic" => "server", "method" => "Listen", "address" => address.to_string());
            let handle = Handle::new();
            let service = router
                .clone()
                .into_make_service_with_connect_info::<SocketAddr>();
            servers.spawn(
                axum_server::bind(address)
                    .handle(handle.clone())
                    .serve(service),
            );
            handles.push(handle);
        }
        if web.https.enable {
            let address = resolve_address(&web.https.address).await?;
            let tls = RustlsConfig::from_pem_file(&web.https.cert_file, &web.https.key_file)
                .await
                .map_err(|e| Error::msg(format!("cannot load TLS certificate or key: {e}")))?;
            log_info!(logger, "Start HTTPS web server", "topic" => "server", "method" => "Listen", "address" => address.to_string());
            let handle = Handle::new();
            let service = router
                .clone()
                .into_make_service_with_connect_info::<SocketAddr>();
            servers.spawn(
                axum_server::bind_rustls(address, tls)
                    .handle(handle.clone())
                    .serve(service),
            );
            handles.push(handle);
        }
        if handles.is_empty() {
            log_warn!(logger, "No web server enabled in configuration (webserver.http.enable and webserver.https.enable)", "topic" => "server");
        }
        Ok::<_, Error>(())
    };

    let stop = match start.await {
        Err(e) => Stop::Failure(e),
        Ok(()) => tokio::select! {
            command = commands.recv() => match command {
                Some(ServerCommand::Restart) => {
                    log_info!(logger, "Restarting server ...", "topic" => "server");
                    Stop::Exit(ServerExit::Restart)
                }
                Some(ServerCommand::Shutdown) | None => Stop::Exit(ServerExit::Shutdown),
            },
            exit = wait_for_signal(&logger) => Stop::Exit(exit),
            Some(result) = servers.join_next() => {
                let message = match result {
                    Ok(Ok(())) => "web server stopped unexpectedly".to_string(),
                    Ok(Err(e)) => format!("web server stopped with error: {e}"),
                    Err(e) => format!("web server task failed: {e}"),
                };
                log_error!(logger, "Web server stopped with error", "topic" => "server", "method" => "HandleSignals", "error" => message);
                Stop::Failure(Error::msg(message))
            }
        },
    };

    log_info!(logger, "Shutdown Server ...", "topic" => "server", "method" => "shutdownListener");
    for handle in &handles {
        handle.graceful_shutdown(Some(GRACEFUL_SHUTDOWN_TIMEOUT));
    }
    while servers.join_next().await.is_some() {}
    log_info!(logger, "Web server stopped", "topic" => "server", "method" => "shutdownListener");

    dedup_cleanup.abort();
    state.dedup.clear();
    state.service.stop().await;
    log_info!(logger, "Server stopped", "topic" => "server");

    match stop {
        Stop::Exit(exit) => Ok(exit),
        Stop::Failure(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn addresses() {
        assert_eq!(
            resolve_address("127.0.0.1:8080").await.unwrap().port(),
            8080
        );
        assert_eq!(
            resolve_address(":9000").await.unwrap().to_string(),
            "0.0.0.0:9000"
        );
        assert_eq!(
            resolve_address("localhost:8081").await.unwrap().port(),
            8081
        );
        assert!(resolve_address("nowhere").await.is_err());
    }
}
