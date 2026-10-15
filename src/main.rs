//! Ministream server.
//!
//! ```sh
//! ministream -config config-templates/minimal-inmemory/config/config.yaml
//! ```

use std::process::ExitCode;

use ministream::cli::{is_set, parse_flags, FlagSpec};
use ministream::config::Config;
use ministream::web::server::{run_server, ServerExit};

// mimalloc instead of the default allocator: with the allocator of musl (static Linux builds, docker
// image), reads were 2x slower and jq filters 6x slower.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const USAGE: &str = "Usage of ministream:
  -config string
        Filepath to config.yaml (default \"config.yaml\")
  -version
        Show version";

fn main() -> ExitCode {
    let specs = [
        FlagSpec {
            name: "config",
            takes_value: true,
        },
        FlagSpec {
            name: "version",
            takes_value: false,
        },
        FlagSpec {
            name: "help",
            takes_value: false,
        },
        FlagSpec {
            name: "h",
            takes_value: false,
        },
    ];
    let flags = match parse_flags(std::env::args().skip(1), &specs) {
        Ok(flags) => flags,
        Err(e) => {
            eprintln!("{e}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    if is_set(&flags, "help") || is_set(&flags, "h") {
        println!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    if is_set(&flags, "version") {
        println!("Version: {}", ministream::version());
        return ExitCode::SUCCESS;
    }
    let config_path = flags
        .get("config")
        .cloned()
        .unwrap_or_else(|| "config.yaml".to_string());

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("cannot start the async runtime: {e}");
            return ExitCode::FAILURE;
        }
    };

    // start/restart the server forever (a restart reloads the configuration) unless an error occurs
    runtime.block_on(async {
        loop {
            let config = match Config::load(&config_path) {
                Ok(config) => config,
                Err(e) => {
                    eprintln!("{e}");
                    return ExitCode::FAILURE;
                }
            };
            match run_server(config).await {
                Ok(ServerExit::Restart) => continue,
                Ok(ServerExit::Shutdown) => return ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("Server stopped with error: {e}");
                    return ExitCode::FAILURE;
                }
            }
        }
    })
}
