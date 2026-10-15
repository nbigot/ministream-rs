//! Generate PBKDF2 password hashes for the credentials file of ministream.
//!
//! ```sh
//! generatepasswords -digest sha512 -iterations 1000 -salt randomStringAbcdef012346789 -password thisIsMySecretPassword
//! generatepasswords -digest sha512 -iterations 1000 -count 10
//! ```

use std::process::ExitCode;

use ministream::auth::hash_password;
use ministream::cli::{is_set, parse_flags, FlagSpec};
use ministream::generators::{generate_random_password, generate_random_salt};

const USAGE: &str = "Usage of generatepasswords:
  -count int
        number of passwords to generate (default 1)
  -digest string
        digest type (allowed values are: sha512, sha256, sha1, md5) (default \"sha512\")
  -iterations int
        number of iterations (default 1000)
  -password string
        a password (a random password is generated when empty)
  -salt string
        a random string (a random salt is generated when empty)";

fn main() -> ExitCode {
    let specs = [
        FlagSpec {
            name: "digest",
            takes_value: true,
        },
        FlagSpec {
            name: "iterations",
            takes_value: true,
        },
        FlagSpec {
            name: "salt",
            takes_value: true,
        },
        FlagSpec {
            name: "password",
            takes_value: true,
        },
        FlagSpec {
            name: "count",
            takes_value: true,
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
    let fail = |message: String| {
        eprintln!("{message}\n{USAGE}");
        ExitCode::from(2)
    };
    let flags = match parse_flags(std::env::args().skip(1), &specs) {
        Ok(flags) => flags,
        Err(e) => return fail(e),
    };
    if is_set(&flags, "help") || is_set(&flags, "h") {
        println!("{USAGE}");
        return ExitCode::SUCCESS;
    }

    let digest = flags.get("digest").map(String::as_str).unwrap_or("sha512");
    if !matches!(digest, "sha512" | "sha256" | "sha1" | "md5") {
        return fail("digest allowed values are: sha512, sha256, sha1, md5".into());
    }
    let iterations: i64 = match flags.get("iterations").map(|v| v.parse()) {
        None => 1000,
        Some(Ok(n)) if n > 0 => n,
        Some(_) => return fail("iterations must be a positive integer".into()),
    };
    let count: u64 = match flags.get("count").map(|v| v.parse()) {
        None => 1,
        Some(Ok(n)) if n > 0 => n,
        Some(_) => return fail("count must be a positive integer".into()),
    };
    let salt = flags.get("salt").cloned().unwrap_or_default();
    let password = flags.get("password").cloned().unwrap_or_default();

    println!("Generate {count} passwords");
    for _ in 0..count {
        let salt = if salt.is_empty() {
            generate_random_salt(20)
        } else {
            salt.clone()
        };
        let password = if password.is_empty() {
            generate_random_password(32)
        } else {
            password.clone()
        };
        match hash_password(digest, iterations, &salt, &password) {
            Ok(hash) => println!(
                "Digest: {digest} Iterations: {iterations} Salt: {salt} Password: {password} Hash: {hash}"
            ),
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        }
    }
    ExitCode::SUCCESS
}
