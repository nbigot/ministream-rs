//! Configuration file (YAML).
//!
//! The format is the same as the one of the Go implementation of ministream:
//! every configuration template of the `config-templates` directory can be used as is.

use std::path::Path;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

use crate::logger::LoggerConfig;

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Config {
    pub storage: StorageConfig,
    pub data_directory: String,
    pub logger: LoggerConfig,
    pub account: AccountConfig,
    pub streams: StreamsConfig,
    pub auth: AuthConfig,
    pub rbac: RbacConfig,
    #[serde(rename = "auditlog")]
    pub audit_log: AuditLogConfig,
    #[serde(rename = "webserver")]
    pub web_server: WebServerConfig,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct AccountConfig {
    /// A unique id is necessary so that a JWT cannot be used on another server instance.
    /// A random id is generated at startup when empty.
    #[serde(deserialize_with = "uuid_or_nil")]
    pub id: Uuid,
    pub name: String,
    /// Secret API key used to log in as super user.
    /// A random key is generated (and logged) at startup when empty.
    #[serde(rename = "secretAPIKey")]
    pub secret_api_key: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct StorageConfig {
    /// Storage provider type: "JSONFile", "InMemory" or "MySQL".
    #[serde(rename = "type")]
    pub storage_type: String,
    pub logger: LoggerConfig,
    pub log_verbosity: i64,
    #[serde(rename = "jsonfile")]
    pub json_file: JsonFileStorageConfig,
    #[serde(rename = "inmemory")]
    pub in_memory: InMemoryStorageConfig,
    pub mysql: MySqlStorageConfig,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct JsonFileStorageConfig {
    pub data_directory: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct InMemoryStorageConfig {
    /// Maximum number of records per stream (0 means unlimited).
    pub max_records_by_stream: u64,
    /// Maximum size of the records of a stream (ex: "1gb", "512 MiB", empty means unlimited).
    pub max_size: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct MySqlStorageConfig {
    /// Data source name, either in the Go driver format
    /// (`user:password@tcp(localhost:3306)/ministream?tls=skip-verify`) or as an URL
    /// (`mysql://user:password@localhost:3306/ministream`).
    /// When the value starts with `$` it is the name of an environment variable holding the DSN.
    #[serde(rename = "dsn")]
    pub data_source_name: String,
    pub conn_max_lifetime: u64,
    pub max_idle_conns: u64,
    pub max_open_conns: u64,
    pub schema_name: String,
    pub catalog_table_name: String,
    pub stream_table_prefix: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct StreamsConfig {
    /// Delay in seconds before buffered records are flushed to the storage (0: immediately).
    pub bulk_flush_frequency: i64,
    /// Maximum count of records buffered before a flush.
    pub bulk_max_size: i64,
    /// Capacity of the ingestion channel of each stream.
    pub channel_buffer_size: i64,
    #[serde(rename = "maxAllowedIteratorsPerStream")]
    pub max_iterators_per_stream: i64,
    pub max_message_per_get_operation: u64,
    pub log_verbosity: i64,
    /// Maximum count of streams (0 means unlimited).
    pub max_allowed_streams: u64,
    /// Iterators not used for this duration are automatically closed (0 means never).
    pub iterator_idle_timeout_in_seconds: u64,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct AuthConfig {
    pub enable: bool,
    /// Authentication method: "FILE" or "HTTP".
    pub method: String,
    pub methods: AuthMethodsConfig,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct AuthMethodsConfig {
    pub file: AuthFileConfig,
    pub http: AuthHttpConfig,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct AuthFileConfig {
    pub filename: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct AuthHttpConfig {
    /// URL returning the credentials (`{accountid}` is replaced by the account id).
    pub url: String,
    #[serde(rename = "proxy")]
    pub proxy_url: String,
    pub auth_token: String,
    /// Timeout in seconds (1 to 60).
    pub timeout: i64,
    pub cache_duration_in_sec: i64,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct RbacConfig {
    pub enable: bool,
    pub filename: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct AuditLogConfig {
    pub enable: bool,
    pub enable_log_access_granted: bool,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct WebServerConfig {
    pub http: HttpConfig,
    pub https: HttpsConfig,
    pub logs: EnableConfig,
    pub cors: CorsConfig,
    pub rate_limiter: RateLimiterConfig,
    pub jwt: JwtConfig,
    pub monitor: EnableConfig,
    pub metrics: EnableConfig,
    pub swagger: EnableConfig,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct EnableConfig {
    pub enable: bool,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct HttpConfig {
    pub enable: bool,
    pub address: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct HttpsConfig {
    pub enable: bool,
    pub address: String,
    pub cert_file: String,
    pub key_file: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct CorsConfig {
    pub enable: bool,
    /// Comma separated list of allowed origins ("*" allows any origin).
    pub allow_origins: String,
    /// Comma separated list of allowed headers (empty mirrors the requested headers).
    pub allow_headers: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct RateLimiterConfig {
    pub enable: bool,
    pub route_stream: RouteRateLimitConfig,
    pub route_job: RouteRateLimitConfig,
    pub route_account: RouteRateLimitConfig,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct RouteRateLimitConfig {
    pub max_requests: i64,
    pub duration_in_seconds: i64,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct JwtConfig {
    pub enable: bool,
    /// Secret used to sign the tokens (a random secret is generated when empty).
    pub secret_key: String,
    pub token_expire_in_minutes: i64,
    pub iss: String,
    pub sub: String,
    pub aud: String,
    #[serde(alias = "account")]
    pub account_id: String,
    /// Tokens emitted before this date are rejected.
    #[serde(deserialize_with = "optional_datetime")]
    pub revoke_emitted_before_date: Option<DateTime<Utc>>,
}

impl Config {
    /// Load and validate a configuration file.
    pub fn load(path: impl AsRef<Path>) -> Result<Config, String> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path).map_err(|e| {
            format!(
                "error while opening configuration file {} : {}",
                path.display(),
                e
            )
        })?;
        let config = Config::from_yaml_str(&content).map_err(|e| {
            format!(
                "error while parsing configuration file {} : {}",
                path.display(),
                e
            )
        })?;
        config.validate()?;
        Ok(config)
    }

    /// Parse a configuration from a YAML document.
    ///
    /// Keys having a null value (ex: a section whose children are all commented out)
    /// are considered as missing and take their default value.
    pub fn from_yaml_str(content: &str) -> Result<Config, String> {
        let mut value: serde_yaml_ng::Value =
            serde_yaml_ng::from_str(content).map_err(|e| e.to_string())?;
        if value.is_null() {
            return Ok(Config::default());
        }
        strip_nulls(&mut value);
        serde_yaml_ng::from_value(value).map_err(|e| e.to_string())
    }

    /// Check the consistency of the configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.rbac.enable && self.rbac.filename.is_empty() {
            return Err("error in configuration file: you must specify a filename for RBAC".into());
        }
        if self.web_server.https.enable
            && (self.web_server.https.cert_file.is_empty()
                || self.web_server.https.key_file.is_empty())
        {
            return Err(
                "error in configuration file: you must specify certFile and keyFile for HTTPS"
                    .into(),
            );
        }
        Ok(())
    }

    /// Maximum count of records returned by a single "get records" request.
    pub fn max_records_per_get(&self) -> u64 {
        match self.streams.max_message_per_get_operation {
            0 => 10_000,
            n => n,
        }
    }
}

fn strip_nulls(value: &mut serde_yaml_ng::Value) {
    match value {
        serde_yaml_ng::Value::Mapping(mapping) => {
            mapping.retain(|_, v| !v.is_null());
            for (_, v) in mapping.iter_mut() {
                strip_nulls(v);
            }
        }
        serde_yaml_ng::Value::Sequence(sequence) => {
            for v in sequence.iter_mut() {
                strip_nulls(v);
            }
        }
        serde_yaml_ng::Value::Tagged(tagged) => strip_nulls(&mut tagged.value),
        _ => {}
    }
}

fn uuid_or_nil<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Uuid, D::Error> {
    let value = Option::<String>::deserialize(deserializer)?.unwrap_or_default();
    let value = value.trim();
    if value.is_empty() {
        return Ok(Uuid::nil());
    }
    Uuid::parse_str(value).map_err(serde::de::Error::custom)
}

fn optional_datetime<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<DateTime<Utc>>, D::Error> {
    let value = Option::<String>::deserialize(deserializer)?.unwrap_or_default();
    parse_datetime(&value).map_err(serde::de::Error::custom)
}

/// Parse a date time (RFC 3339, "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD"; UTC is assumed without offset).
pub fn parse_datetime(value: &str) -> Result<Option<DateTime<Utc>>, String> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Ok(Some(dt.with_timezone(&Utc)));
    }
    for format in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"] {
        if let Ok(dt) = NaiveDateTime::parse_from_str(value, format) {
            return Ok(Some(dt.and_utc()));
        }
    }
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        return Ok(Some(
            date.and_hms_opt(0, 0, 0).unwrap_or_default().and_utc(),
        ));
    }
    Err(format!("invalid date: {value}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_sections_take_default_values() {
        let config = Config::from_yaml_str(
            r#"
auth:
    enable: false
    method: "FILE"
    methods:
        file:
            #filename: "/app/data/secrets/secrets.json"
account:
    name: "demo"
    secretAPIKey:
storage:
    type: "InMemory"
    jsonfile:
    inmemory:
        maxRecordsByStream: 10
        maxSize: "1gb"
webserver:
    jwt:
        enable: true
        account: ""
        revokeEmittedBeforeDate: "2024-01-02T03:04:05Z"
    rateLimiter:
        routeStream:
            maxRequests: 100
"#,
        )
        .unwrap();
        assert_eq!(config.auth.methods.file.filename, "");
        assert_eq!(config.account.name, "demo");
        assert!(config.account.id.is_nil());
        assert_eq!(config.storage.storage_type, "InMemory");
        assert_eq!(config.storage.in_memory.max_records_by_stream, 10);
        assert!(config.web_server.jwt.enable);
        assert_eq!(
            config
                .web_server
                .jwt
                .revoke_emitted_before_date
                .unwrap()
                .to_rfc3339(),
            "2024-01-02T03:04:05+00:00"
        );
        assert_eq!(
            config.web_server.rate_limiter.route_stream.max_requests,
            100
        );
        assert_eq!(config.max_records_per_get(), 10_000);
    }

    #[test]
    fn empty_document_is_default_config() {
        let config = Config::from_yaml_str("").unwrap();
        assert_eq!(config.storage.storage_type, "");
    }

    #[test]
    fn rbac_requires_filename() {
        let config = Config::from_yaml_str("rbac:\n  enable: true\n").unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn all_config_templates_are_valid() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("config-templates");
        let mut count = 0;
        for entry in std::fs::read_dir(root).unwrap() {
            let path = entry.unwrap().path().join("config").join("config.yaml");
            if path.exists() {
                Config::load(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
                count += 1;
            }
        }
        assert!(count >= 5);
    }

    #[test]
    fn parse_datetimes() {
        assert!(parse_datetime("").unwrap().is_none());
        assert!(parse_datetime("2024-01-02").unwrap().is_some());
        assert!(parse_datetime("2024-01-02 10:11:12").unwrap().is_some());
        assert!(parse_datetime("2024-01-02T10:11:12+02:00")
            .unwrap()
            .is_some());
        assert!(parse_datetime("yesterday").is_err());
    }
}
