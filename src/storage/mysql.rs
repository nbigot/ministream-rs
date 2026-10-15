//! MySQL storage provider.
//!
//! - the catalog of streams is stored in the table `<schema>.<catalogTableName>`,
//! - the records of a stream are stored in the table `<schema>.<streamTablePrefix><uuid>`
//!   (dashes of the uuid are replaced by underscores), one row per record holding
//!   the record envelope `{"i": id, "d": date, "m": message}` in a JSON column.

use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::{Arc, LazyLock, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use regex::Regex;
use serde_json::Value;
use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions, MySqlSslMode};
use sqlx::{MySql, QueryBuilder};
use uuid::Uuid;

use super::catalog::Catalog;
use super::{NextRecord, StorageProvider, StreamIteratorHandler, StreamWriter, WrittenBatch};
use crate::config::{Config, MySqlStorageConfig};
use crate::error::{Error, Result};
use crate::logger::Logger;
use crate::types::{
    is_zero_time, json_size, zero_time, DeferedStreamRecord, IteratorType, SharedStreamInfo,
    StreamInfo, StreamIteratorRequest, StreamIteratorUuid, StreamMessagesInfo, StreamProperties,
    StreamUuid,
};
use crate::{log_debug, log_error, log_info};

static IDENTIFIER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z0-9_]+$").expect("valid regex"));

/// Maximum count of rows inserted by a single INSERT statement.
const INSERT_CHUNK_SIZE: usize = 500;
/// Count of rows read at once by an iterator.
const ITERATOR_BUFFER_SIZE: i64 = 100;

/// Validated MySQL settings.
#[derive(Debug, Clone)]
pub struct MySqlSettings {
    pub connect_options: MySqlConnectOptions,
    pub schema_name: String,
    pub catalog_table_name: String,
    pub stream_table_prefix: String,
    pub conn_max_lifetime: u64,
    pub max_open_conns: u64,
}

impl MySqlSettings {
    pub fn new(config: &MySqlStorageConfig) -> Result<MySqlSettings> {
        if config.data_source_name.is_empty() {
            return Err(Error::msg("empty data source name"));
        }
        // when the DSN value starts with "$" then it is an environment variable name
        let dsn = match config.data_source_name.strip_prefix('$') {
            Some(variable) => std::env::var(variable).map_err(|_| {
                Error::msg(format!(
                    "environment variable {variable} (mysql dsn) is not set"
                ))
            })?,
            None => config.data_source_name.clone(),
        };
        let with_default = |value: &str, default: &str| {
            if value.is_empty() {
                default.to_string()
            } else {
                value.to_string()
            }
        };
        let schema_name = with_default(&config.schema_name, "ministream");
        let catalog_table_name = with_default(&config.catalog_table_name, "streams");
        let stream_table_prefix = with_default(&config.stream_table_prefix, "stream_");
        if !IDENTIFIER.is_match(&schema_name) {
            return Err(Error::msg("invalid schema name"));
        }
        if !IDENTIFIER.is_match(&catalog_table_name) {
            return Err(Error::msg("invalid catalog table name"));
        }
        if !IDENTIFIER.is_match(&stream_table_prefix) {
            return Err(Error::msg("invalid stream table prefix"));
        }
        Ok(MySqlSettings {
            connect_options: parse_dsn(&dsn)?,
            schema_name,
            catalog_table_name,
            stream_table_prefix,
            conn_max_lifetime: config.conn_max_lifetime,
            max_open_conns: config.max_open_conns,
        })
    }

    fn catalog_table(&self) -> String {
        format!("`{}`.`{}`", self.schema_name, self.catalog_table_name)
    }

    pub fn stream_table_name(&self, stream_uuid: &StreamUuid) -> String {
        format!(
            "{}{}",
            self.stream_table_prefix,
            stream_uuid.to_string().replace('-', "_")
        )
    }

    fn stream_table(&self, stream_uuid: &StreamUuid) -> String {
        format!(
            "`{}`.`{}`",
            self.schema_name,
            self.stream_table_name(stream_uuid)
        )
    }
}

/// Parse a data source name, either an URL (`mysql://user:password@host:3306/database`)
/// or a DSN in the format of the Go MySQL driver
/// (`user:password@tcp(host:3306)/database?tls=skip-verify`).
pub fn parse_dsn(dsn: &str) -> Result<MySqlConnectOptions> {
    let invalid = |reason: &str| Error::msg(format!("invalid DSN: {reason}"));
    if dsn.starts_with("mysql://") || dsn.starts_with("mariadb://") {
        return MySqlConnectOptions::from_str(dsn).map_err(|e| invalid(&e.to_string()));
    }

    let slash = dsn
        .rfind('/')
        .ok_or_else(|| invalid("missing the slash separating the database name"))?;
    let (left, right) = (&dsn[..slash], &dsn[slash + 1..]);
    let (database, query) = right.split_once('?').unwrap_or((right, ""));
    let (credentials, address) = match left.rfind('@') {
        Some(at) => (Some(&left[..at]), &left[at + 1..]),
        None => (None, left),
    };

    let mut options = MySqlConnectOptions::new();
    if let Some(credentials) = credentials {
        let (user, password) = match credentials.split_once(':') {
            Some((user, password)) => (user, Some(password)),
            None => (credentials, None),
        };
        options = options.username(user);
        if let Some(password) = password {
            options = options.password(password);
        }
    }

    let (network, target) = match address.find('(') {
        Some(open) => {
            let close = address
                .rfind(')')
                .filter(|close| *close > open)
                .ok_or_else(|| invalid("network address not terminated (missing closing brace)"))?;
            (&address[..open], &address[open + 1..close])
        }
        None => (address, ""),
    };
    match network {
        "" | "tcp" => {
            let target = if target.is_empty() {
                "127.0.0.1:3306"
            } else {
                target
            };
            let (host, port) = split_host_port(target)?;
            options = options.host(host).port(port);
        }
        "unix" => options = options.socket(target),
        other => return Err(invalid(&format!("unsupported network \"{other}\""))),
    }

    if !database.is_empty() {
        options = options.database(database);
    }

    for parameter in query.split('&').filter(|p| !p.is_empty()) {
        let (key, value) = parameter.split_once('=').unwrap_or((parameter, ""));
        match key {
            "tls" => {
                let mode = match value {
                    "true" => MySqlSslMode::VerifyIdentity,
                    "skip-verify" => MySqlSslMode::Required,
                    "preferred" => MySqlSslMode::Preferred,
                    "false" => MySqlSslMode::Disabled,
                    other => return Err(invalid(&format!("unsupported tls value \"{other}\""))),
                };
                options = options.ssl_mode(mode);
            }
            "charset" => {
                if let Some(charset) = value.split(',').next() {
                    options = options.charset(charset);
                }
            }
            // other parameters of the Go driver (parseTime, loc, timeout, ...) are not relevant
            _ => {}
        }
    }
    Ok(options)
}

fn split_host_port(target: &str) -> Result<(&str, u16)> {
    let invalid = || Error::msg(format!("invalid DSN: invalid address \"{target}\""));
    if let Some(rest) = target.strip_prefix('[') {
        // IPv6 address: [::1]:3306
        let (host, rest) = rest.split_once(']').ok_or_else(invalid)?;
        let port = match rest.strip_prefix(':') {
            Some(port) => port.parse().map_err(|_| invalid())?,
            None => 3306,
        };
        return Ok((host, port));
    }
    match target.rsplit_once(':') {
        Some((host, port)) => Ok((host, port.parse().map_err(|_| invalid())?)),
        None => Ok((target, 3306)),
    }
}

/// MySQL storage provider.
pub struct MySqlStorage {
    logger: Logger,
    log_verbosity: i64,
    settings: Arc<MySqlSettings>,
    pool: OnceLock<MySqlPool>,
    catalog: Catalog,
}

impl MySqlStorage {
    pub fn new(config: &Config, logger: Logger) -> Result<MySqlStorage> {
        Ok(MySqlStorage {
            logger,
            log_verbosity: config.storage.log_verbosity,
            settings: Arc::new(MySqlSettings::new(&config.storage.mysql)?),
            pool: OnceLock::new(),
            catalog: Catalog::default(),
        })
    }

    fn pool(&self) -> Result<&MySqlPool> {
        self.pool
            .get()
            .ok_or_else(|| Error::msg("mysql storage provider is not initialized"))
    }

    async fn ensure_catalog_exists(&self, pool: &MySqlPool) -> Result<()> {
        let schema = &self.settings.schema_name;
        sqlx::raw_sql(&format!("CREATE SCHEMA IF NOT EXISTS `{schema}`"))
            .execute(pool)
            .await?;
        let query = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id CHAR(36) PRIMARY KEY,
                creation_date DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
                last_update DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
                cache_cpt_rows BIGINT DEFAULT 0,
                cache_size_in_bytes BIGINT DEFAULT 0,
                cache_first_msg_id BIGINT NULL,
                cache_last_msg_id BIGINT NULL,
                cache_first_msg_timestamp DATETIME(6) NULL,
                cache_last_msg_timestamp DATETIME(6) NULL,
                comment VARCHAR(255) DEFAULT NULL,
                properties JSON DEFAULT NULL
            )",
            self.settings.catalog_table()
        );
        sqlx::raw_sql(&query).execute(pool).await?;
        Ok(())
    }
}

type CatalogRow = (
    String,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Option<String>,
);

fn optional_time(t: &DateTime<Utc>) -> Option<DateTime<Utc>> {
    (!is_zero_time(t)).then_some(*t)
}

#[async_trait]
impl StorageProvider for MySqlStorage {
    fn name(&self) -> &'static str {
        "MySQL"
    }

    async fn init(&self) -> Result<()> {
        let mut pool_options = MySqlPoolOptions::new()
            .max_connections(match self.settings.max_open_conns {
                0 => 10,
                n => n.min(u32::MAX as u64) as u32,
            })
            .acquire_timeout(Duration::from_secs(30));
        if self.settings.conn_max_lifetime > 0 {
            pool_options =
                pool_options.max_lifetime(Duration::from_secs(self.settings.conn_max_lifetime));
        }
        let pool = pool_options
            .connect_with(self.settings.connect_options.clone())
            .await?;
        self.ensure_catalog_exists(&pool).await?;
        self.catalog.clear();
        self.pool
            .set(pool)
            .map_err(|_| Error::msg("mysql storage provider is already initialized"))?;
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        if let Some(pool) = self.pool.get() {
            pool.close().await;
        }
        Ok(())
    }

    fn stream_exists(&self, stream_uuid: &StreamUuid) -> bool {
        self.catalog.contains(stream_uuid)
    }

    async fn save_stream_catalog(&self) -> Result<()> {
        // nothing to do: the catalog is persisted in the SQL table
        Ok(())
    }

    async fn load_streams(&self) -> Result<Vec<SharedStreamInfo>> {
        log_info!(self.logger, "Loading streams", "topic" => "stream", "method" => "LoadStreamCatalog");
        let query = format!(
            "SELECT id, creation_date, last_update, cache_cpt_rows, cache_size_in_bytes, \
             cache_first_msg_id, cache_last_msg_id, cache_first_msg_timestamp, cache_last_msg_timestamp, \
             CAST(properties AS CHAR) FROM {}",
            self.settings.catalog_table()
        );
        let rows: Vec<CatalogRow> = sqlx::query_as(&query).fetch_all(self.pool()?).await?;

        self.catalog.clear();
        let mut infos = Vec::with_capacity(rows.len());
        for row in rows {
            let uuid = Uuid::parse_str(&row.0)
                .map_err(|e| Error::msg(format!("invalid stream id {} in catalog: {e}", row.0)))?;
            let properties: StreamProperties = match row.9.as_deref() {
                None | Some("") | Some("null") => StreamProperties::new(),
                Some(json) => serde_json::from_str(json).map_err(|e| {
                    Error::msg(format!("can't unmarshal properties of stream {uuid}: {e}"))
                })?,
            };
            let messages = StreamMessagesInfo {
                cpt_messages: row.3.unwrap_or(0).max(0) as u64,
                size_in_bytes: row.4.unwrap_or(0).max(0) as u64,
                first_msg_id: row.5.unwrap_or(0).max(0) as u64,
                last_msg_id: row.6.unwrap_or(0).max(0) as u64,
                first_msg_timestamp: row.7.unwrap_or_else(zero_time),
                last_msg_timestamp: row.8.unwrap_or_else(zero_time),
            };
            let info = StreamInfo {
                uuid,
                creation_date: row.1.unwrap_or_else(zero_time),
                last_update: row.2.unwrap_or_else(zero_time),
                properties,
                ingested_messages: messages.clone(),
                readable_messages: messages,
            }
            .into_shared();
            self.catalog.insert(info.clone());
            infos.push(info);
        }
        if infos.is_empty() {
            log_info!(self.logger, "No stream found", "topic" => "stream", "method" => "LoadStreams");
        } else {
            log_info!(self.logger, "Found streams", "topic" => "stream", "method" => "LoadStreams", "streams" => infos.len());
        }
        Ok(infos)
    }

    async fn on_create_stream(&self, info: &SharedStreamInfo) -> Result<()> {
        let snapshot = info.read().clone();
        let pool = self.pool()?;
        let stream_table = self.settings.stream_table(&snapshot.uuid);
        sqlx::raw_sql(&format!(
            "CREATE TABLE IF NOT EXISTS {stream_table} (id BIGINT PRIMARY KEY, timestamp DATETIME(6), message JSON)"
        ))
        .execute(pool)
        .await
        .inspect_err(|e| {
            log_error!(self.logger, "Can't create stream table", "topic" => "stream", "method" => "OnCreateStream", "stream.uuid" => snapshot.uuid, "error" => e.to_string());
        })?;

        let insert = sqlx::query(&format!(
            "INSERT INTO {} (id, creation_date, last_update, properties) VALUES (?, ?, ?, ?)",
            self.settings.catalog_table()
        ))
        .bind(snapshot.uuid.to_string())
        .bind(snapshot.creation_date)
        .bind(snapshot.last_update)
        .bind(serde_json::to_string(&snapshot.properties)?)
        .execute(pool)
        .await;
        if let Err(e) = insert {
            log_error!(self.logger, "Can't insert stream", "topic" => "stream", "method" => "OnCreateStream", "stream.uuid" => snapshot.uuid, "error" => e.to_string());
            let _ = sqlx::raw_sql(&format!("DROP TABLE IF EXISTS {stream_table}"))
                .execute(pool)
                .await;
            return Err(e.into());
        }

        self.catalog.insert(info.clone());
        Ok(())
    }

    async fn on_update_stream_properties(&self, info: &SharedStreamInfo) -> Result<()> {
        let snapshot = info.read().clone();
        sqlx::query(&format!(
            "UPDATE {} SET properties = ?, last_update = ? WHERE id = ?",
            self.settings.catalog_table()
        ))
        .bind(serde_json::to_string(&snapshot.properties)?)
        .bind(snapshot.last_update)
        .bind(snapshot.uuid.to_string())
        .execute(self.pool()?)
        .await?;
        Ok(())
    }

    fn get_stream_info(&self, stream_uuid: &StreamUuid) -> Result<SharedStreamInfo> {
        self.catalog.get(stream_uuid)
    }

    async fn build_index(&self, stream_uuid: &StreamUuid) -> Result<Value> {
        if !self.stream_exists(stream_uuid) {
            return Err(Error::not_found(format!("stream not found {stream_uuid}")));
        }
        log_info!(self.logger, "Build index started", "topic" => "index", "method" => "BuildIndex", "stream.uuid" => stream_uuid);
        let pool = self.pool()?;
        let table = self.settings.stream_table(stream_uuid);
        // rebuild/optimize the mysql index (optional but recommended for performance)
        sqlx::raw_sql(&format!("OPTIMIZE TABLE {table}"))
            .fetch_all(pool)
            .await?;
        let (count, first_id, last_id): (i64, Option<i64>, Option<i64>) = sqlx::query_as(&format!(
            "SELECT CAST(COUNT(*) AS SIGNED), MIN(id), MAX(id) FROM {table}"
        ))
        .fetch_one(pool)
        .await?;
        let timestamp_of = |id: Option<i64>| {
            let query = format!("SELECT timestamp FROM {table} WHERE id = ?");
            async move {
                match id {
                    None => Ok::<_, Error>(zero_time()),
                    Some(id) => {
                        let row: Option<(Option<DateTime<Utc>>,)> =
                            sqlx::query_as(&query).bind(id).fetch_optional(pool).await?;
                        Ok(row.and_then(|r| r.0).unwrap_or_else(zero_time))
                    }
                }
            }
        };
        let stats = serde_json::json!({
            "CptMessages": count,
            "FirstMsgId": first_id.unwrap_or(0),
            "LastMsgId": last_id.unwrap_or(0),
            "FirstMsgTimestamp": timestamp_of(first_id).await?,
            "LastMsgTimestamp": timestamp_of(last_id).await?,
        });
        log_info!(self.logger, "Build index ended", "topic" => "index", "method" => "BuildIndex", "stream.uuid" => stream_uuid, "index.rowsCount" => count);
        Ok(stats)
    }

    async fn new_stream_iterator_handler(
        &self,
        stream_uuid: &StreamUuid,
        _iterator_uuid: &StreamIteratorUuid,
    ) -> Result<Box<dyn StreamIteratorHandler>> {
        if !self.stream_exists(stream_uuid) {
            return Err(Error::not_found(format!("stream not found {stream_uuid}")));
        }
        Ok(Box::new(StreamIteratorHandlerMySql {
            pool: self.pool()?.clone(),
            table: self.settings.stream_table(stream_uuid),
            next_id: 0,
            buffer: VecDeque::new(),
        }))
    }

    async fn new_stream_writer(&self, info: &SharedStreamInfo) -> Result<Box<dyn StreamWriter>> {
        let uuid = info.read().uuid;
        Ok(Box::new(StreamWriterMySql {
            pool: self.pool()?.clone(),
            settings: self.settings.clone(),
            info: info.clone(),
            stream_uuid: uuid,
            logger: self.logger.clone(),
            log_verbosity: self.log_verbosity,
        }))
    }

    async fn delete_stream(&self, stream_uuid: &StreamUuid) -> Result<()> {
        let pool = self.pool()?;
        sqlx::query(&format!(
            "DELETE FROM {} WHERE id = ?",
            self.settings.catalog_table()
        ))
        .bind(stream_uuid.to_string())
        .execute(pool)
        .await?;
        sqlx::raw_sql(&format!(
            "DROP TABLE IF EXISTS {}",
            self.settings.stream_table(stream_uuid)
        ))
        .execute(pool)
        .await?;
        self.catalog.remove(stream_uuid);
        Ok(())
    }
}

struct StreamWriterMySql {
    pool: MySqlPool,
    settings: Arc<MySqlSettings>,
    info: SharedStreamInfo,
    stream_uuid: StreamUuid,
    logger: Logger,
    log_verbosity: i64,
}

#[async_trait]
impl StreamWriter for StreamWriterMySql {
    async fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn open(&mut self) -> Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        log_debug!(self.logger, "Stream writer mysql closed", "topic" => "streamwriter", "method" => "close", "stream.uuid" => self.stream_uuid);
        Ok(())
    }

    async fn write(&mut self, records: &mut Vec<DeferedStreamRecord>) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        if self.log_verbosity > 0 {
            log_debug!(self.logger, "write records into mysql", "topic" => "stream", "method" => "Write", "stream.uuid" => self.stream_uuid, "records.cpt" => records.len());
        }

        let mut batch = WrittenBatch::default();
        let mut rows = Vec::with_capacity(records.len());
        for record in records.iter() {
            let json = serde_json::to_string(record)?;
            batch.add(record, json_size(&record.msg));
            rows.push((record.id as i64, record.creation_date, json));
        }

        let stream_table = self.settings.stream_table(&self.stream_uuid);
        let mut transaction = self.pool.begin().await?;
        for chunk in rows.chunks(INSERT_CHUNK_SIZE) {
            let mut builder: QueryBuilder<MySql> = QueryBuilder::new(format!(
                "INSERT INTO {stream_table} (`id`, `timestamp`, `message`) "
            ));
            builder.push_values(chunk, |mut b, (id, timestamp, json)| {
                b.push_bind(*id)
                    .push_bind(*timestamp)
                    .push_bind(json.as_str());
            });
            builder.build().execute(&mut *transaction).await.inspect_err(|e| {
                log_error!(self.logger, "can't insert records into stream", "topic" => "stream", "method" => "Write", "stream.uuid" => self.stream_uuid, "error" => e.to_string());
            })?;
        }

        // update the cached information of the catalog in the same transaction
        let mut updated = self.info.read().clone();
        batch.apply_to(&mut updated);
        let readable = &updated.readable_messages;
        sqlx::query(&format!(
            "UPDATE {} SET cache_cpt_rows = ?, cache_size_in_bytes = ?, cache_first_msg_id = ?, \
             cache_last_msg_id = ?, cache_first_msg_timestamp = ?, cache_last_msg_timestamp = ?, last_update = ? \
             WHERE id = ?",
            self.settings.catalog_table()
        ))
        .bind(readable.cpt_messages as i64)
        .bind(readable.size_in_bytes as i64)
        .bind(readable.first_msg_id as i64)
        .bind(readable.last_msg_id as i64)
        .bind(optional_time(&readable.first_msg_timestamp))
        .bind(optional_time(&readable.last_msg_timestamp))
        .bind(updated.last_update)
        .bind(self.stream_uuid.to_string())
        .execute(&mut *transaction)
        .await?;

        transaction.commit().await?;

        // the stream information is updated once the transaction is committed
        batch.apply(&self.info);
        records.clear();
        Ok(())
    }
}

struct StreamIteratorHandlerMySql {
    pool: MySqlPool,
    table: String,
    /// Id of the next record to read.
    next_id: i64,
    buffer: VecDeque<(i64, String)>,
}

impl StreamIteratorHandlerMySql {
    async fn max_id(&self) -> Result<Option<i64>> {
        let (max,): (Option<i64>,) = sqlx::query_as(&format!("SELECT MAX(id) FROM {}", self.table))
            .fetch_one(&self.pool)
            .await?;
        Ok(max)
    }
}

#[async_trait]
impl StreamIteratorHandler for StreamIteratorHandlerMySql {
    async fn open(&mut self) -> Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.buffer.clear();
        Ok(())
    }

    async fn seek(&mut self, request: &StreamIteratorRequest) -> Result<()> {
        let message_id = i64::try_from(request.message_id)
            .map_err(|_| Error::not_found("message id not found"))?;
        self.next_id = match request.iterator_type {
            IteratorType::FirstMessage => 0,
            IteratorType::LastMessage => self.max_id().await?.unwrap_or(0),
            IteratorType::AfterLastMessage => self.max_id().await?.map(|m| m + 1).unwrap_or(0),
            IteratorType::AtMessageId => {
                let row: Option<(i64,)> =
                    sqlx::query_as(&format!("SELECT id FROM {} WHERE id = ?", self.table))
                        .bind(message_id)
                        .fetch_optional(&self.pool)
                        .await?;
                row.map(|r| r.0)
                    .ok_or_else(|| Error::not_found("message id not found"))?
            }
            IteratorType::AfterMessageId => {
                if message_id > self.max_id().await?.unwrap_or(0) {
                    return Err(Error::not_found("message id not found"));
                }
                message_id + 1
            }
            IteratorType::AtTimestamp => {
                let timestamp = request
                    .timestamp
                    .ok_or_else(|| Error::msg("missing timestamp"))?;
                let (first,): (Option<i64>,) = sqlx::query_as(&format!(
                    "SELECT MIN(id) FROM {} WHERE timestamp >= ?",
                    self.table
                ))
                .bind(timestamp)
                .fetch_one(&self.pool)
                .await?;
                match first {
                    Some(id) => id,
                    None => self.max_id().await?.map(|m| m + 1).unwrap_or(0),
                }
            }
        };
        self.buffer.clear();
        Ok(())
    }

    async fn get_next_record(&mut self) -> Result<NextRecord> {
        if self.buffer.is_empty() {
            // read ahead a chunk of records
            let rows: Vec<(i64, Option<String>)> = sqlx::query_as(&format!(
                "SELECT id, CAST(message AS CHAR) FROM {} WHERE id >= ? ORDER BY id ASC LIMIT ?",
                self.table
            ))
            .bind(self.next_id)
            .bind(ITERATOR_BUFFER_SIZE)
            .fetch_all(&self.pool)
            .await?;
            self.buffer.extend(
                rows.into_iter()
                    .map(|(id, json)| (id, json.unwrap_or_default())),
            );
        }
        let Some((id, json)) = self.buffer.pop_front() else {
            return Ok(NextRecord::End);
        };
        self.next_id = id + 1;
        let id = id.max(0) as u64;
        match serde_json::from_str::<Value>(&json) {
            Ok(envelope) => Ok(NextRecord::Record(id, envelope)),
            Err(e) => Ok(NextRecord::Invalid(Some(id), e.into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_driver_dsn() {
        let options = parse_dsn(
            "user:p@ss:word@tcp(db.local:3307)/ministream?tls=skip-verify&parseTime=true",
        )
        .unwrap();
        assert_eq!(options.get_host(), "db.local");
        assert_eq!(options.get_port(), 3307);
        assert_eq!(options.get_username(), "user");
        assert_eq!(options.get_database(), Some("ministream"));

        let options = parse_dsn("root@/test").unwrap();
        assert_eq!(options.get_host(), "127.0.0.1");
        assert_eq!(options.get_port(), 3306);
        assert_eq!(options.get_username(), "root");

        let options = parse_dsn("u:p@tcp([::1]:3308)/db").unwrap();
        assert_eq!(options.get_host(), "::1");
        assert_eq!(options.get_port(), 3308);

        assert!(parse_dsn("u:p@tcp(host:3306").is_err());
        assert!(parse_dsn("no slash").is_err());
        assert!(parse_dsn("u@tcp(h:1)/db?tls=maybe").is_err());
        assert!(parse_dsn("u@pipe(h)/db").is_err());
    }

    #[test]
    fn url_dsn() {
        let options = parse_dsn("mysql://user:password@localhost:3306/database").unwrap();
        assert_eq!(options.get_host(), "localhost");
        assert_eq!(options.get_database(), Some("database"));
    }

    #[test]
    fn settings() {
        let mut config = MySqlStorageConfig {
            data_source_name: "u:p@tcp(localhost:3306)/ministream".into(),
            stream_table_prefix: "prefix_".into(),
            ..Default::default()
        };
        let settings = MySqlSettings::new(&config).unwrap();
        assert_eq!(settings.schema_name, "ministream");
        assert_eq!(settings.catalog_table_name, "streams");
        let uuid = Uuid::parse_str("9cf1eba6-0f6d-425e-ada9-e5b6cf293ffc").unwrap();
        assert_eq!(
            settings.stream_table_name(&uuid),
            "prefix_9cf1eba6_0f6d_425e_ada9_e5b6cf293ffc"
        );

        config.schema_name = "bad;name".into();
        assert!(MySqlSettings::new(&config).is_err());
        config.schema_name = String::new();
        config.data_source_name = String::new();
        assert!(MySqlSettings::new(&config).is_err());
        config.data_source_name = "$MINISTREAM_TEST_UNDEFINED_VARIABLE".into();
        assert!(MySqlSettings::new(&config).is_err());
    }
}
