//! Web API.

pub mod client;
pub mod dedup;
pub mod handlers;
pub mod jwt;
pub mod monitor;
pub mod ratelimiter;
pub mod rbac;
pub mod results;
pub mod server;
pub mod swagger;

use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{HeaderName, HeaderValue, Method, StatusCode};
use axum::middleware::{from_fn, from_fn_with_state, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, patch, post, put};
use axum::Router;
use chrono::Local;
use tokio::sync::mpsc;
use tower_http::cors::{AllowHeaders, AllowOrigin, CorsLayer};

use crate::account::Account;
use crate::auth::AuthManager;
use crate::config::Config;
use crate::error::Result;
use crate::logger::Logger;
use crate::metrics::Metrics;
use crate::rbac::{self as rbac_actions, RbacManager};
use crate::service::Service;
use crate::{log_error, log_warn};

use self::client::ClientInfo;
use self::dedup::RequestDeduplicator;
use self::handlers::{account, admin, stream};
use self::jwt::{jwt_protected, JwtManager};
use self::monitor::SystemMonitor;
use self::ratelimiter::{rate_limit, LimiterKey, RateLimiter};
use self::rbac::{rbac_middleware, RbacRoute};

/// Maximum size of a request body (10 MiB).
pub const BODY_LIMIT: usize = 10 * 1024 * 1024;
/// Time to live of the batch ids used for deduplication.
pub const BATCH_ID_TTL: Duration = Duration::from_secs(5 * 60);

/// Commands sent to the server (by the admin API).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerCommand {
    Shutdown,
    Restart,
}

/// Shared state of the web API.
#[derive(Clone)]
pub struct AppState {
    inner: Arc<AppInner>,
}

pub struct AppInner {
    pub config: Arc<Config>,
    pub logger: Logger,
    pub service: Arc<Service>,
    pub account: Account,
    pub auth: AuthManager,
    pub rbac: RbacManager,
    pub jwt: JwtManager,
    pub dedup: RequestDeduplicator,
    pub metrics: Arc<Metrics>,
    pub control: mpsc::UnboundedSender<ServerCommand>,
    pub started_at: Instant,
    pub system_monitor: Arc<SystemMonitor>,
}

impl Deref for AppState {
    type Target = AppInner;

    fn deref(&self) -> &AppInner {
        &self.inner
    }
}

impl AppState {
    /// Initialize every component of the server from its configuration
    /// (account, authentication, RBAC, JWT, storage and streams).
    pub async fn from_config(
        config: Arc<Config>,
        logger: Logger,
    ) -> Result<(AppState, mpsc::UnboundedReceiver<ServerCommand>)> {
        let account = Account::new(&config.account, &logger);
        let jwt = JwtManager::new(&config.web_server.jwt, &account.id);
        let auth = AuthManager::new(&config.auth, &account.id, &logger).inspect_err(|e| {
            log_error!(logger, "Cannot initialize authentication manager", "topic" => "auth manager", "error" => e.to_string());
        })?;
        let rbac = RbacManager::new(&logger, config.rbac.enable, &config.rbac.filename).inspect_err(|e| {
            log_error!(logger, "Error while loading RBAC", "topic" => "rbac", "method" => "NewRBACManager", "filename" => config.rbac.filename, "error" => e.to_string());
        })?;
        let metrics = Arc::new(Metrics::default());
        let service = Service::new(config.clone(), logger.clone(), metrics.clone()).await?;
        let (control, commands) = mpsc::unbounded_channel();
        let state = AppState {
            inner: Arc::new(AppInner {
                config,
                logger,
                service,
                account,
                auth,
                rbac,
                jwt,
                dedup: RequestDeduplicator::new(BATCH_ID_TTL),
                metrics,
                control,
                started_at: Instant::now(),
                system_monitor: Arc::new(SystemMonitor::new()),
            }),
        };
        Ok((state, commands))
    }
}

/// Count the HTTP requests by method and status code.
async fn count_requests(
    State(metrics): State<Arc<Metrics>>,
    request: Request,
    next: Next,
) -> Response {
    let method = request.method().to_string();
    let response = next.run(request).await;
    metrics.add_http_request(&method, response.status().as_u16());
    response
}

/// Access log: `[time] status - ip:port - latency method path`.
async fn access_log(client: ClientInfo, request: Request, next: Next) -> Response {
    let start = Instant::now();
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let response = next.run(request).await;
    println!(
        "[{}] {} - {}:{} - {:?} {} {}",
        Local::now().format("%Y-%m-%dT%H:%M:%S%z"),
        response.status().as_u16(),
        client.ip,
        client.port,
        start.elapsed(),
        method,
        path
    );
    response
}

fn cors_layer(config: &Config, logger: &Logger) -> CorsLayer {
    let cors = &config.web_server.cors;
    let split = |list: &str| -> Vec<String> {
        list.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    };
    let origins = split(&cors.allow_origins);
    let allow_origin = if origins.is_empty() || origins.iter().any(|o| o == "*") {
        AllowOrigin::any()
    } else {
        AllowOrigin::list(origins.iter().filter_map(|o| {
            HeaderValue::from_str(o)
                .inspect_err(|_| log_warn!(logger, "Invalid CORS origin ignored", "topic" => "server", "origin" => o))
                .ok()
        }))
    };
    let headers = split(&cors.allow_headers);
    let allow_headers = if headers.is_empty() {
        AllowHeaders::mirror_request()
    } else if headers.iter().any(|h| h == "*") {
        AllowHeaders::any()
    } else {
        AllowHeaders::list(
            headers
                .iter()
                .filter_map(|h| HeaderName::from_bytes(h.as_bytes()).ok()),
        )
    };
    CorsLayer::new()
        .allow_origin(allow_origin)
        .allow_headers(allow_headers)
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::HEAD,
            Method::PUT,
            Method::DELETE,
            Method::PATCH,
        ])
}

async fn welcome() -> &'static str {
    "Welcome to ministream!"
}

async fn health() -> &'static str {
    "OK"
}

async fn not_found(method: Method, uri: axum::http::Uri) -> Response {
    (
        StatusCode::NOT_FOUND,
        format!("Cannot {} {}", method, uri.path()),
    )
        .into_response()
}

/// Build the router of the web API.
pub fn build_router(state: AppState) -> Router {
    let config = state.config.clone();
    let limits = &config.web_server.rate_limiter;
    let stream_limiter = RateLimiter::new(
        limits.enable,
        limits.route_stream.max_requests,
        limits.route_stream.duration_in_seconds,
        LimiterKey::AccountPathMethod,
    );
    let account_limiter = RateLimiter::new(
        limits.enable,
        limits.route_account.max_requests,
        limits.route_account.duration_in_seconds,
        LimiterKey::AccountPathMethod,
    );
    let utils_limiter = RateLimiter::new(limits.enable, 5, 1, LimiterKey::Global("utils"));

    // RBAC check of the action of a route (ABAC rules are evaluated on the stream of the route)
    // (with `handler_abac`, the ABAC rules are handed to the handler, ex: to filter a list of streams)
    macro_rules! rbac {
        ($action:expr, handler_abac) => {
            from_fn_with_state(
                RbacRoute {
                    state: state.clone(),
                    action: $action,
                    abac_on_stream: false,
                },
                rbac_middleware,
            )
        };
        ($action:expr) => {
            from_fn_with_state(
                RbacRoute {
                    state: state.clone(),
                    action: $action,
                    abac_on_stream: true,
                },
                rbac_middleware,
            )
        };
    }
    let jwt = || from_fn_with_state(state.clone(), jwt_protected);

    // note: the most used routes are registered first
    let stream_routes = Router::new()
        .route(
            "/api/v1/stream/{streamuuid}/iterator/{streamiteratoruuid}/records",
            get(stream::get_records).route_layer(rbac!(rbac_actions::ACTION_GET_RECORDS)),
        )
        .route(
            "/api/v1/stream/{streamuuid}/records",
            put(stream::put_records).route_layer(rbac!(rbac_actions::ACTION_PUT_RECORDS)),
        )
        .route(
            "/api/v1/stream/{streamuuid}/record",
            put(stream::put_record).route_layer(rbac!(rbac_actions::ACTION_PUT_RECORD)),
        )
        .route(
            "/api/v1/stream/{streamuuid}/iterator",
            post(stream::create_records_iterator)
                .route_layer(rbac!(rbac_actions::ACTION_CREATE_RECORDS_ITERATOR)),
        )
        .route(
            "/api/v1/stream/{streamuuid}/iterator/{streamiteratoruuid}/stats",
            get(stream::get_records_iterator_stats)
                .route_layer(rbac!(rbac_actions::ACTION_GET_RECORDS_ITERATOR_STATS)),
        )
        .route(
            "/api/v1/stream/{streamuuid}/iterator/{streamiteratoruuid}",
            delete(stream::close_records_iterator)
                .route_layer(rbac!(rbac_actions::ACTION_CLOSE_RECORDS_ITERATOR)),
        )
        .route(
            "/api/v1/stream/{streamuuid}",
            get(stream::get_stream_information)
                .route_layer(rbac!(rbac_actions::ACTION_GET_STREAM_DESCRIPTION)),
        )
        .route(
            "/api/v1/stream/{streamuuid}",
            delete(stream::delete_stream).route_layer(rbac!(rbac_actions::ACTION_DELETE_STREAM)),
        )
        .route(
            "/api/v1/stream/{streamuuid}/properties",
            get(stream::get_stream_properties)
                .route_layer(rbac!(rbac_actions::ACTION_GET_STREAM_PROPERTIES)),
        )
        .route(
            "/api/v1/stream/{streamuuid}/properties",
            post(stream::set_stream_properties)
                .route_layer(rbac!(rbac_actions::ACTION_SET_STREAM_PROPERTIES)),
        )
        .route(
            "/api/v1/stream/{streamuuid}/properties",
            patch(stream::update_stream_properties)
                .route_layer(rbac!(rbac_actions::ACTION_UPDATE_STREAM_PROPERTIES)),
        )
        .route(
            "/api/v1/stream/",
            post(stream::create_stream)
                .route_layer(rbac!(rbac_actions::ACTION_CREATE_STREAM, handler_abac)),
        )
        .route(
            "/api/v1/stream",
            post(stream::create_stream)
                .route_layer(rbac!(rbac_actions::ACTION_CREATE_STREAM, handler_abac)),
        )
        .route(
            "/api/v1/stream/{streamuuid}/index/rebuild",
            post(stream::rebuild_index).route_layer(rbac!(rbac_actions::ACTION_REBUILD_INDEX)),
        )
        .route(
            "/api/v1/streams/",
            get(stream::list_streams)
                .route_layer(rbac!(rbac_actions::ACTION_LIST_STREAMS, handler_abac)),
        )
        .route(
            "/api/v1/streams",
            get(stream::list_streams)
                .route_layer(rbac!(rbac_actions::ACTION_LIST_STREAMS, handler_abac)),
        )
        .route(
            "/api/v1/streams/properties",
            get(stream::list_streams_properties).route_layer(rbac!(
                rbac_actions::ACTION_LIST_STREAMS_PROPERTIES,
                handler_abac
            )),
        )
        .route_layer(from_fn_with_state(stream_limiter, rate_limit))
        .route_layer(jwt());

    let account_routes = Router::new()
        .route("/api/v1/user/login", get(account::login_user))
        .route(
            "/api/v1/users/",
            get(account::list_users)
                .route_layer(rbac!(rbac_actions::ACTION_LIST_USERS, handler_abac))
                .route_layer(jwt()),
        )
        .route(
            "/api/v1/users",
            get(account::list_users)
                .route_layer(rbac!(rbac_actions::ACTION_LIST_USERS, handler_abac))
                .route_layer(jwt()),
        )
        .route("/api/v1/account/validate", get(account::validate_api_key))
        .route("/api/v1/account/login", get(account::login_account))
        .route(
            "/api/v1/account/",
            get(account::get_account)
                .route_layer(rbac!(rbac_actions::ACTION_GET_ACCOUNT, handler_abac))
                .route_layer(jwt()),
        )
        .route(
            "/api/v1/account",
            get(account::get_account)
                .route_layer(rbac!(rbac_actions::ACTION_GET_ACCOUNT, handler_abac))
                .route_layer(jwt()),
        )
        .route_layer(from_fn_with_state(account_limiter, rate_limit));

    let admin_routes = Router::new()
        .route(
            "/api/v1/admin/server/shutdown",
            post(admin::shutdown_server)
                .route_layer(rbac!(rbac_actions::ACTION_SHUTDOWN_SERVER, handler_abac)),
        )
        .route(
            "/api/v1/admin/server/restart",
            post(admin::restart_server)
                .route_layer(rbac!(rbac_actions::ACTION_RESTART_SERVER, handler_abac)),
        )
        .route(
            "/api/v1/admin/jwt/revoke",
            post(admin::jwt_revoke_all)
                .route_layer(rbac!(rbac_actions::ACTION_JWT_REVOKE_ALL, handler_abac)),
        )
        .route_layer(jwt());

    let mut router = Router::new()
        .merge(stream_routes)
        .merge(account_routes)
        .merge(admin_routes)
        .route(
            "/api/v1/utils/pbkdf2",
            post(admin::utils_pbkdf2).route_layer(from_fn_with_state(utils_limiter, rate_limit)),
        )
        .route("/api/v1/utils/ping", get(admin::ping))
        .route("/", get(welcome))
        .route("/livez", get(health))
        .route("/readyz", get(health));

    let web = &config.web_server;
    if web.monitor.enable {
        router = router.route("/monitor", get(monitor::monitor));
    }
    if web.metrics.enable {
        router = router.route("/metrics", get(monitor::metrics));
    }
    if web.swagger.enable {
        // swagger UI: http://127.0.0.1:8080/docs/index.html, document: http://127.0.0.1:8080/docs/doc.json
        router = router
            .route("/docs", get(swagger::redirect_to_index))
            .route("/docs/", get(swagger::index))
            .route("/docs/index.html", get(swagger::index))
            .route("/docs/doc.json", get(swagger::doc_json))
            .route("/swagger/doc.json", get(swagger::doc_json));
    }

    let mut router = router
        .fallback(not_found)
        .with_state(state.clone())
        .layer(DefaultBodyLimit::max(BODY_LIMIT));
    if web.metrics.enable {
        router = router.layer(from_fn_with_state(state.metrics.clone(), count_requests));
    }
    if web.logs.enable {
        router = router.layer(from_fn(access_log));
    }
    if web.cors.enable {
        router = router.layer(cors_layer(&config, &state.logger));
    }
    router
}
