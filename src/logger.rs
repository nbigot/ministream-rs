//! Structured logger.
//!
//! The configuration format is compatible with the `zap` logger configuration
//! used by the original Go implementation of ministream (`logger` and
//! `storage.logger` sections of the configuration file), so existing
//! configuration files keep working and produce the same kind of JSON lines.

use std::io::Write;
use std::sync::Arc;

use chrono::{Local, SecondsFormat, Utc};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Log level (ordered from the most verbose to the least verbose).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Level {
    Debug,
    Info,
    Warn,
    Error,
    DPanic,
    Panic,
    Fatal,
    /// Disables logging entirely.
    Off,
}

impl Level {
    pub fn as_str(&self) -> &'static str {
        match self {
            Level::Debug => "debug",
            Level::Info => "info",
            Level::Warn => "warn",
            Level::Error => "error",
            Level::DPanic => "dpanic",
            Level::Panic => "panic",
            Level::Fatal => "fatal",
            Level::Off => "off",
        }
    }

    pub fn parse(s: &str) -> Result<Level, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "debug" => Ok(Level::Debug),
            "" | "info" => Ok(Level::Info),
            "warn" | "warning" => Ok(Level::Warn),
            "error" => Ok(Level::Error),
            "dpanic" => Ok(Level::DPanic),
            "panic" => Ok(Level::Panic),
            "fatal" => Ok(Level::Fatal),
            "off" | "none" => Ok(Level::Off),
            other => Err(format!("unrecognized level: \"{other}\"")),
        }
    }
}

/// Logger configuration (zap compatible).
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct LoggerConfig {
    pub level: String,
    pub development: bool,
    pub disable_caller: bool,
    pub disable_stacktrace: bool,
    pub encoding: String,
    pub encoder_config: EncoderConfig,
    pub output_paths: Vec<String>,
    pub error_output_paths: Vec<String>,
    pub initial_fields: Map<String, Value>,
}

/// Encoder configuration (zap compatible).
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct EncoderConfig {
    pub message_key: String,
    pub level_key: String,
    pub time_key: String,
    pub name_key: String,
    pub caller_key: String,
    pub function_key: String,
    pub stacktrace_key: String,
    pub skip_line_ending: bool,
    pub line_ending: String,
    pub level_encoder: String,
    pub time_encoder: String,
    pub duration_encoder: String,
    pub caller_encoder: String,
    pub console_separator: String,
}

#[derive(Debug, Clone, Copy)]
enum TimeEncoder {
    Iso8601,
    Rfc3339,
    Rfc3339Nano,
    Epoch,
    EpochMillis,
    EpochNanos,
}

#[derive(Debug, Clone, Copy)]
enum LevelEncoder {
    Lowercase,
    LowercaseColor,
    Capital,
    CapitalColor,
}

enum Output {
    Stdout,
    Stderr,
    File(Mutex<std::fs::File>),
    Memory(Arc<Mutex<Vec<u8>>>),
}

impl Output {
    fn write(&self, data: &[u8]) {
        // errors while writing logs cannot be reported anywhere, ignore them
        let _ = match self {
            Output::Stdout => {
                let mut out = std::io::stdout().lock();
                out.write_all(data).and_then(|_| out.flush())
            }
            Output::Stderr => {
                let mut out = std::io::stderr().lock();
                out.write_all(data).and_then(|_| out.flush())
            }
            Output::File(file) => file.lock().write_all(data),
            Output::Memory(buffer) => {
                buffer.lock().extend_from_slice(data);
                Ok(())
            }
        };
    }
}

struct Inner {
    level: Level,
    json: bool,
    message_key: String,
    level_key: String,
    time_key: String,
    time_encoder: TimeEncoder,
    level_encoder: LevelEncoder,
    line_ending: String,
    console_separator: String,
    fields: Vec<(String, Value)>,
    outputs: Arc<Vec<Output>>,
}

/// A cheap to clone structured logger.
#[derive(Clone)]
pub struct Logger {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Logger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Logger")
            .field("level", &self.inner.level)
            .finish()
    }
}

fn non_empty(value: &str, default: &str) -> String {
    if value.is_empty() {
        default.to_string()
    } else {
        value.to_string()
    }
}

impl Logger {
    /// Build a logger from its configuration.
    pub fn new(config: &LoggerConfig) -> Result<Logger, String> {
        let level = Level::parse(&config.level)?;
        let json = match config.encoding.to_ascii_lowercase().as_str() {
            "" | "json" => true,
            "console" => false,
            other => return Err(format!("no encoder registered for name \"{other}\"")),
        };
        let ec = &config.encoder_config;
        let time_encoder = match ec.time_encoder.as_str() {
            "iso8601" | "ISO8601" => TimeEncoder::Iso8601,
            "rfc3339" | "RFC3339" => TimeEncoder::Rfc3339,
            "rfc3339nano" | "RFC3339Nano" => TimeEncoder::Rfc3339Nano,
            "millis" => TimeEncoder::EpochMillis,
            "nanos" => TimeEncoder::EpochNanos,
            _ => TimeEncoder::Epoch,
        };
        let level_encoder = match ec.level_encoder.as_str() {
            "capital" => LevelEncoder::Capital,
            "capitalColor" => LevelEncoder::CapitalColor,
            "color" => LevelEncoder::LowercaseColor,
            _ => LevelEncoder::Lowercase,
        };
        let line_ending = if ec.skip_line_ending {
            String::new()
        } else {
            non_empty(&ec.line_ending, "\n")
        };

        let mut outputs = Vec::new();
        let paths = if config.output_paths.is_empty() {
            vec!["stdout".to_string()]
        } else {
            config.output_paths.clone()
        };
        for path in paths {
            let output = match path.as_str() {
                "stdout" => Output::Stdout,
                "stderr" => Output::Stderr,
                p => {
                    let p = p.strip_prefix("file://").unwrap_or(p);
                    let file = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(p)
                        .map_err(|e| format!("cannot open log output \"{p}\": {e}"))?;
                    Output::File(Mutex::new(file))
                }
            };
            outputs.push(output);
        }

        Ok(Logger {
            inner: Arc::new(Inner {
                level,
                json,
                message_key: non_empty(&ec.message_key, "message"),
                level_key: non_empty(&ec.level_key, "level"),
                time_key: non_empty(&ec.time_key, "timestamp"),
                time_encoder,
                level_encoder,
                line_ending,
                console_separator: non_empty(&ec.console_separator, "\t"),
                fields: config
                    .initial_fields
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                outputs: Arc::new(outputs),
            }),
        })
    }

    /// Default logger: JSON lines on stdout at info level.
    pub fn default_stdout() -> Logger {
        let config = LoggerConfig {
            encoder_config: EncoderConfig {
                time_encoder: "iso8601".into(),
                ..Default::default()
            },
            ..Default::default()
        };
        Logger::new(&config).expect("default logger configuration is valid")
    }

    /// A logger that discards everything.
    pub fn nop() -> Logger {
        Logger {
            inner: Arc::new(Inner {
                level: Level::Off,
                json: true,
                message_key: "message".into(),
                level_key: "level".into(),
                time_key: "timestamp".into(),
                time_encoder: TimeEncoder::Iso8601,
                level_encoder: LevelEncoder::Lowercase,
                line_ending: "\n".into(),
                console_separator: "\t".into(),
                fields: Vec::new(),
                outputs: Arc::new(Vec::new()),
            }),
        }
    }

    /// A logger writing JSON lines into a memory buffer (useful for tests).
    pub fn memory(level: Level) -> (Logger, Arc<Mutex<Vec<u8>>>) {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let logger = Logger {
            inner: Arc::new(Inner {
                level,
                json: true,
                message_key: "message".into(),
                level_key: "level".into(),
                time_key: "timestamp".into(),
                time_encoder: TimeEncoder::Iso8601,
                level_encoder: LevelEncoder::Lowercase,
                line_ending: "\n".into(),
                console_separator: "\t".into(),
                fields: Vec::new(),
                outputs: Arc::new(vec![Output::Memory(buffer.clone())]),
            }),
        };
        (logger, buffer)
    }

    /// Return a child logger which adds the given fields to every entry.
    pub fn with_fields(&self, fields: &[(&str, Value)]) -> Logger {
        let inner = &self.inner;
        let mut all_fields = inner.fields.clone();
        all_fields.extend(fields.iter().map(|(k, v)| (k.to_string(), v.clone())));
        Logger {
            inner: Arc::new(Inner {
                level: inner.level,
                json: inner.json,
                message_key: inner.message_key.clone(),
                level_key: inner.level_key.clone(),
                time_key: inner.time_key.clone(),
                time_encoder: inner.time_encoder,
                level_encoder: inner.level_encoder,
                line_ending: inner.line_ending.clone(),
                console_separator: inner.console_separator.clone(),
                fields: all_fields,
                outputs: inner.outputs.clone(),
            }),
        }
    }

    pub fn level(&self) -> Level {
        self.inner.level
    }

    #[inline]
    pub fn enabled(&self, level: Level) -> bool {
        level != Level::Off && level >= self.inner.level
    }

    /// Write a log entry (prefer the `log_*!` macros).
    pub fn log(&self, level: Level, message: &str, fields: &[(&str, Value)]) {
        if !self.enabled(level) {
            return;
        }
        let inner = &self.inner;
        let mut line = String::with_capacity(256);
        if inner.json {
            line.push('{');
            let mut first = true;
            push_json_kv(
                &mut line,
                &mut first,
                &inner.level_key,
                &Value::String(self.encode_level(level)),
            );
            push_json_kv(&mut line, &mut first, &inner.time_key, &self.encode_time());
            push_json_kv(
                &mut line,
                &mut first,
                &inner.message_key,
                &Value::String(message.to_string()),
            );
            for (k, v) in &inner.fields {
                push_json_kv(&mut line, &mut first, k, v);
            }
            for (k, v) in fields {
                push_json_kv(&mut line, &mut first, k, v);
            }
            line.push('}');
        } else {
            let sep = &inner.console_separator;
            match self.encode_time() {
                Value::String(s) => line.push_str(&s),
                other => line.push_str(&other.to_string()),
            }
            line.push_str(sep);
            line.push_str(&self.encode_level(level));
            line.push_str(sep);
            line.push_str(message);
            if !inner.fields.is_empty() || !fields.is_empty() {
                let mut map = Map::new();
                for (k, v) in &inner.fields {
                    map.insert(k.clone(), v.clone());
                }
                for (k, v) in fields {
                    map.insert(k.to_string(), v.clone());
                }
                line.push_str(sep);
                line.push_str(&Value::Object(map).to_string());
            }
        }
        line.push_str(&inner.line_ending);
        for output in inner.outputs.iter() {
            output.write(line.as_bytes());
        }
    }

    fn encode_level(&self, level: Level) -> String {
        let name = level.as_str();
        let color = match level {
            Level::Debug => "35",
            Level::Info => "34",
            Level::Warn => "33",
            _ => "31",
        };
        match self.inner.level_encoder {
            LevelEncoder::Lowercase => name.to_string(),
            LevelEncoder::Capital => name.to_ascii_uppercase(),
            LevelEncoder::LowercaseColor => format!("\x1b[{color}m{name}\x1b[0m"),
            LevelEncoder::CapitalColor => {
                format!("\x1b[{color}m{}\x1b[0m", name.to_ascii_uppercase())
            }
        }
    }

    fn encode_time(&self) -> Value {
        match self.inner.time_encoder {
            TimeEncoder::Iso8601 => {
                Value::String(Local::now().format("%Y-%m-%dT%H:%M:%S%.3f%z").to_string())
            }
            TimeEncoder::Rfc3339 => {
                Value::String(Local::now().to_rfc3339_opts(SecondsFormat::Secs, false))
            }
            TimeEncoder::Rfc3339Nano => {
                Value::String(Local::now().to_rfc3339_opts(SecondsFormat::AutoSi, false))
            }
            TimeEncoder::Epoch => {
                let now = Utc::now();
                let secs = now.timestamp() as f64 + f64::from(now.timestamp_subsec_nanos()) / 1e9;
                serde_json::Number::from_f64(secs).map_or(Value::Null, Value::Number)
            }
            TimeEncoder::EpochMillis => {
                let now = Utc::now();
                let ms = now.timestamp_millis() as f64
                    + f64::from(now.timestamp_subsec_nanos() % 1_000_000) / 1e6;
                serde_json::Number::from_f64(ms).map_or(Value::Null, Value::Number)
            }
            TimeEncoder::EpochNanos => {
                Value::from(Utc::now().timestamp_nanos_opt().unwrap_or_default())
            }
        }
    }
}

fn push_json_kv(line: &mut String, first: &mut bool, key: &str, value: &Value) {
    if !*first {
        line.push(',');
    }
    *first = false;
    line.push_str(&Value::String(key.to_string()).to_string());
    line.push(':');
    line.push_str(&value.to_string());
}

/// Convert any serializable value into a JSON value for logging purposes.
pub fn to_value<T: Serialize + ?Sized>(value: &T) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

/// Log an entry at the given level with structured fields.
///
/// ```ignore
/// log_at!(logger, Level::Info, "Start stream", "topic" => "stream", "stream.uuid" => uuid);
/// ```
#[macro_export]
macro_rules! log_at {
    ($logger:expr, $level:expr, $msg:expr $(, $key:expr => $value:expr)* $(,)?) => {{
        let logger: &$crate::logger::Logger = &$logger;
        if logger.enabled($level) {
            logger.log($level, &$msg, &[$(($key, $crate::logger::to_value(&$value))),*]);
        }
    }};
}

#[macro_export]
macro_rules! log_debug {
    ($logger:expr, $($rest:tt)*) => { $crate::log_at!($logger, $crate::logger::Level::Debug, $($rest)*) };
}

#[macro_export]
macro_rules! log_info {
    ($logger:expr, $($rest:tt)*) => { $crate::log_at!($logger, $crate::logger::Level::Info, $($rest)*) };
}

#[macro_export]
macro_rules! log_warn {
    ($logger:expr, $($rest:tt)*) => { $crate::log_at!($logger, $crate::logger::Level::Warn, $($rest)*) };
}

#[macro_export]
macro_rules! log_error {
    ($logger:expr, $($rest:tt)*) => { $crate::log_at!($logger, $crate::logger::Level::Error, $($rest)*) };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_entry_contains_fields_in_order() {
        let (logger, buffer) = Logger::memory(Level::Debug);
        let logger = logger.with_fields(&[("app", Value::from("ministream"))]);
        log_info!(logger, "hello", "topic" => "server", "count" => 3u64);
        let output = String::from_utf8(buffer.lock().clone()).unwrap();
        let entry: Value = serde_json::from_str(output.trim()).unwrap();
        assert_eq!(entry["level"], "info");
        assert_eq!(entry["message"], "hello");
        assert_eq!(entry["app"], "ministream");
        assert_eq!(entry["topic"], "server");
        assert_eq!(entry["count"], 3);
        assert!(output.find("\"app\"").unwrap() < output.find("\"topic\"").unwrap());
    }

    #[test]
    fn level_filtering() {
        let (logger, buffer) = Logger::memory(Level::Warn);
        log_info!(logger, "ignored");
        log_debug!(logger, "ignored");
        log_warn!(logger, "kept");
        let output = String::from_utf8(buffer.lock().clone()).unwrap();
        assert_eq!(output.lines().count(), 1);
        assert!(!Logger::nop().enabled(Level::Fatal));
    }

    #[test]
    fn parse_levels() {
        assert_eq!(Level::parse("").unwrap(), Level::Info);
        assert_eq!(Level::parse("DEBUG").unwrap(), Level::Debug);
        assert_eq!(Level::parse("warning").unwrap(), Level::Warn);
        assert!(Level::parse("verbose").is_err());
    }
}
