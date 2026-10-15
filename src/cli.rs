//! Minimal command line flags parser accepting the Go `flag` package syntax
//! (`-name value`, `-name=value`, `--name value`, `--name=value`, `-boolflag`),
//! so that the command lines of the Go implementation keep working.

use std::collections::HashMap;

/// Definition of a flag.
pub struct FlagSpec {
    pub name: &'static str,
    /// True when the flag takes a value, false for a boolean flag.
    pub takes_value: bool,
}

/// Parse the command line arguments (without the program name).
pub fn parse_flags(
    args: impl IntoIterator<Item = String>,
    specs: &[FlagSpec],
) -> Result<HashMap<&'static str, String>, String> {
    let mut flags = HashMap::new();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        let Some(stripped) = arg.strip_prefix("--").or_else(|| arg.strip_prefix('-')) else {
            return Err(format!("unexpected argument: {arg}"));
        };
        let (name, inline_value) = match stripped.split_once('=') {
            Some((name, value)) => (name, Some(value.to_string())),
            None => (stripped, None),
        };
        let spec = specs
            .iter()
            .find(|s| s.name == name)
            .ok_or_else(|| format!("flag provided but not defined: -{name}"))?;
        let value = match (spec.takes_value, inline_value) {
            (_, Some(value)) => value,
            (true, None) => args
                .next()
                .ok_or_else(|| format!("flag needs an argument: -{name}"))?,
            (false, None) => "true".to_string(),
        };
        flags.insert(spec.name, value);
    }
    Ok(flags)
}

/// Value of a boolean flag.
pub fn is_set(flags: &HashMap<&'static str, String>, name: &str) -> bool {
    flags
        .get(name)
        .is_some_and(|v| matches!(v.as_str(), "true" | "1" | "t" | "T" | "TRUE" | "True"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SPECS: &[FlagSpec] = &[
        FlagSpec {
            name: "config",
            takes_value: true,
        },
        FlagSpec {
            name: "version",
            takes_value: false,
        },
    ];

    fn parse(args: &[&str]) -> Result<HashMap<&'static str, String>, String> {
        parse_flags(args.iter().map(|s| s.to_string()), SPECS)
    }

    #[test]
    fn go_syntax() {
        assert_eq!(parse(&["-config", "a.yaml"]).unwrap()["config"], "a.yaml");
        assert_eq!(parse(&["--config=b.yaml"]).unwrap()["config"], "b.yaml");
        assert!(is_set(&parse(&["-version"]).unwrap(), "version"));
        assert!(!is_set(&parse(&["-version=false"]).unwrap(), "version"));
        assert!(parse(&["-config"]).is_err());
        assert!(parse(&["-unknown"]).is_err());
        assert!(parse(&["positional"]).is_err());
    }
}
