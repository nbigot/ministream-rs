//! Monitoring endpoints: `/monitor` (JSON or HTML page) and `/metrics` (Prometheus).

use std::fmt::Write as _;

use axum::extract::State;
use axum::http::{header, HeaderMap};
use axum::response::{Html, IntoResponse, Response};
use axum::Json;
use parking_lot::Mutex;
use serde::Serialize;
use sysinfo::{Pid, ProcessesToUpdate, System};

use super::AppState;

/// Process and system statistics.
pub struct SystemMonitor {
    system: Mutex<System>,
    pid: Option<Pid>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ProcessStats {
    /// CPU usage in percent (100% is one fully used core).
    pub cpu: f32,
    /// Resident memory in bytes.
    pub ram: u64,
    pub virtual_memory: u64,
    /// CPU time in seconds.
    pub cpu_time: f64,
    /// Start time (seconds since the unix epoch).
    pub start_time: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct OsStats {
    pub cpu: f32,
    pub ram: u64,
    pub total_ram: u64,
    pub load_avg: f64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct SystemStats {
    pub pid: ProcessStats,
    pub os: OsStats,
}

impl Default for SystemMonitor {
    fn default() -> Self {
        SystemMonitor::new()
    }
}

impl SystemMonitor {
    pub fn new() -> SystemMonitor {
        SystemMonitor {
            system: Mutex::new(System::new()),
            pid: sysinfo::get_current_pid().ok(),
        }
    }

    /// Refresh and return the statistics (CPU usages are computed since the previous call).
    pub fn stats(&self) -> SystemStats {
        let mut system = self.system.lock();
        system.refresh_cpu_usage();
        system.refresh_memory();
        let mut stats = SystemStats {
            os: OsStats {
                cpu: system.global_cpu_usage(),
                ram: system.used_memory(),
                total_ram: system.total_memory(),
                load_avg: System::load_average().one,
            },
            ..Default::default()
        };
        if let Some(pid) = self.pid {
            system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
            if let Some(process) = system.process(pid) {
                stats.pid = ProcessStats {
                    cpu: process.cpu_usage(),
                    ram: process.memory(),
                    virtual_memory: process.virtual_memory(),
                    cpu_time: process.accumulated_cpu_time() as f64 / 1000.0,
                    start_time: process.start_time(),
                };
            }
        }
        stats
    }
}

#[derive(Debug, Clone, Serialize)]
struct MinistreamStats {
    version: String,
    storage: String,
    uptime_seconds: u64,
    streams: usize,
    iterators: usize,
    records_ingested: u64,
    records_sent: u64,
}

#[derive(Debug, Clone, Serialize)]
struct MonitorResponse {
    pid: ProcessStats,
    os: OsStats,
    ministream: MinistreamStats,
}

fn ministream_stats(state: &AppState) -> MinistreamStats {
    let streams = state.service.get_streams_filtered(&[]);
    MinistreamStats {
        version: crate::version().to_string(),
        storage: state.service.storage().name().to_string(),
        uptime_seconds: state.started_at.elapsed().as_secs(),
        streams: streams.len(),
        iterators: streams.iter().map(|s| s.iterators_count()).sum(),
        records_ingested: state.metrics.records_ingested(),
        records_sent: state.metrics.records_sent(),
    }
}

const MONITOR_PAGE: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Ministream Monitor</title>
<style>
  :root { color-scheme: light dark; font-family: system-ui, sans-serif; }
  body { margin: 0 auto; max-width: 960px; padding: 16px; }
  h1 { font-size: 1.4rem; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; }
  .card { border: 1px solid #8884; border-radius: 8px; padding: 12px; }
  .label { opacity: .7; font-size: .85rem; }
  .value { font-size: 1.5rem; font-variant-numeric: tabular-nums; }
</style>
</head>
<body>
<h1>Ministream Monitor</h1>
<div class="grid" id="cards"></div>
<script>
const fmtBytes = b => { const u = ["B","KB","MB","GB","TB"]; let i = 0; while (b >= 1024 && i < u.length - 1) { b /= 1024; i++; } return b.toFixed(1) + " " + u[i]; };
async function refresh() {
  try {
    const r = await fetch(location.pathname, { headers: { Accept: "application/json" } });
    const s = await r.json();
    const cards = [
      ["Process CPU", s.pid.cpu.toFixed(1) + " %"], ["Process RAM", fmtBytes(s.pid.ram)],
      ["System CPU", s.os.cpu.toFixed(1) + " %"], ["System RAM", fmtBytes(s.os.ram) + " / " + fmtBytes(s.os.total_ram)],
      ["Load average", s.os.load_avg.toFixed(2)], ["Uptime", s.ministream.uptime_seconds + " s"],
      ["Streams", s.ministream.streams], ["Iterators", s.ministream.iterators],
      ["Records ingested", s.ministream.records_ingested], ["Records sent", s.ministream.records_sent],
      ["Storage", s.ministream.storage], ["Version", s.ministream.version],
    ];
    document.getElementById("cards").innerHTML = cards.map(([l, v]) =>
      `<div class="card"><div class="label">${l}</div><div class="value">${v}</div></div>`).join("");
  } catch (e) { console.error(e); }
}
refresh(); setInterval(refresh, 3000);
</script>
</body>
</html>
"#;

/// Monitor (`GET /monitor`): JSON statistics when `Accept: application/json`, an HTML page otherwise.
pub async fn monitor(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let wants_json = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("application/json"));
    if !wants_json {
        return Html(MONITOR_PAGE).into_response();
    }
    let monitor = state.system_monitor.clone();
    let system = tokio::task::spawn_blocking(move || monitor.stats())
        .await
        .unwrap_or_default();
    Json(MonitorResponse {
        pid: system.pid,
        os: system.os,
        ministream: ministream_stats(&state),
    })
    .into_response()
}

fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// Prometheus metrics (`GET /metrics`).
pub async fn metrics(State(state): State<AppState>) -> Response {
    let monitor = state.system_monitor.clone();
    let system = tokio::task::spawn_blocking(move || monitor.stats())
        .await
        .unwrap_or_default();
    let stats = ministream_stats(&state);
    let mut out = String::with_capacity(4096);
    let mut metric = |name: &str, kind: &str, help: &str, samples: Vec<(String, String)>| {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} {kind}");
        for (labels, value) in samples {
            let _ = writeln!(out, "{name}{labels} {value}");
        }
    };

    metric(
        "ministream_info",
        "gauge",
        "Information about the ministream server.",
        vec![(
            format!(
                "{{version=\"{}\",storage=\"{}\"}}",
                escape_label(&stats.version),
                escape_label(&stats.storage)
            ),
            "1".into(),
        )],
    );
    metric(
        "ministream_uptime_seconds",
        "gauge",
        "Uptime of the server in seconds.",
        vec![(String::new(), stats.uptime_seconds.to_string())],
    );
    metric(
        "ministream_streams",
        "gauge",
        "Number of streams.",
        vec![(String::new(), stats.streams.to_string())],
    );
    metric(
        "ministream_stream_iterators",
        "gauge",
        "Number of opened records iterators.",
        vec![(String::new(), stats.iterators.to_string())],
    );
    metric(
        "ministream_records_ingested_total",
        "counter",
        "Number of records ingested since the server started.",
        vec![(String::new(), stats.records_ingested.to_string())],
    );
    metric(
        "ministream_records_sent_total",
        "counter",
        "Number of records sent to the consumers since the server started.",
        vec![(String::new(), stats.records_sent.to_string())],
    );

    let streams = state.service.get_streams_filtered(&[]);
    let infos: Vec<_> = streams.iter().map(|s| s.info()).collect();
    let per_stream = |f: &dyn Fn(&crate::types::StreamInfo) -> u64| -> Vec<(String, String)> {
        infos
            .iter()
            .map(|info| (format!("{{stream=\"{}\"}}", info.uuid), f(info).to_string()))
            .collect()
    };
    metric(
        "ministream_stream_ingested_records",
        "gauge",
        "Number of records ingested in a stream.",
        per_stream(&|i| i.ingested_messages.cpt_messages),
    );
    metric(
        "ministream_stream_readable_records",
        "gauge",
        "Number of records readable in a stream.",
        per_stream(&|i| i.readable_messages.cpt_messages),
    );
    metric(
        "ministream_stream_readable_bytes",
        "gauge",
        "Size in bytes of the records readable in a stream.",
        per_stream(&|i| i.readable_messages.size_in_bytes),
    );

    let requests: Vec<(String, String)> = state
        .metrics
        .http_requests()
        .into_iter()
        .map(|((method, code), count)| {
            (
                format!("{{method=\"{}\",code=\"{}\"}}", escape_label(&method), code),
                count.to_string(),
            )
        })
        .collect();
    metric(
        "ministream_http_requests_total",
        "counter",
        "Number of HTTP requests by method and status code.",
        requests,
    );

    metric(
        "process_resident_memory_bytes",
        "gauge",
        "Resident memory size in bytes.",
        vec![(String::new(), system.pid.ram.to_string())],
    );
    metric(
        "process_virtual_memory_bytes",
        "gauge",
        "Virtual memory size in bytes.",
        vec![(String::new(), system.pid.virtual_memory.to_string())],
    );
    metric(
        "process_cpu_seconds_total",
        "counter",
        "Total user and system CPU time spent in seconds.",
        vec![(String::new(), system.pid.cpu_time.to_string())],
    );
    metric(
        "process_start_time_seconds",
        "gauge",
        "Start time of the process since unix epoch in seconds.",
        vec![(String::new(), system.pid.start_time.to_string())],
    );

    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        out,
    )
        .into_response()
}
