//! Human readable byte sizes ("1gb", "512 MiB", "42", ...).

/// Parse a human readable byte size.
///
/// Both SI (`kB`, `MB`, `GB`, ... powers of 1000) and IEC (`KiB`, `MiB`, `GiB`, ...
/// powers of 1024) units are supported, the parsing is case insensitive.
pub fn parse_bytes(input: &str) -> Result<u64, String> {
    let s = input.trim();
    let split = s
        .char_indices()
        .find(|(_, c)| !(c.is_ascii_digit() || *c == '.' || *c == ','))
        .map(|(i, _)| i)
        .unwrap_or(s.len());
    let number = s[..split].replace(',', "");
    let unit = s[split..].trim().to_ascii_lowercase();

    let value: f64 = number
        .parse()
        .map_err(|_| format!("invalid size: \"{input}\""))?;

    let multiplier: f64 = match unit.as_str() {
        "" | "b" | "byte" | "bytes" => 1.0,
        "k" | "kb" => 1e3,
        "ki" | "kib" => 1024.0,
        "m" | "mb" => 1e6,
        "mi" | "mib" => 1024f64.powi(2),
        "g" | "gb" => 1e9,
        "gi" | "gib" => 1024f64.powi(3),
        "t" | "tb" => 1e12,
        "ti" | "tib" => 1024f64.powi(4),
        "p" | "pb" => 1e15,
        "pi" | "pib" => 1024f64.powi(5),
        "e" | "eb" => 1e18,
        "ei" | "eib" => 1024f64.powi(6),
        _ => return Err(format!("unhandled size name: \"{unit}\"")),
    };

    let bytes = value * multiplier;
    if !bytes.is_finite() || bytes >= u64::MAX as f64 {
        return Err(format!("too large: \"{input}\""));
    }
    Ok(bytes as u64)
}

#[cfg(test)]
mod tests {
    use super::parse_bytes;

    #[test]
    fn parse() {
        assert_eq!(parse_bytes("42").unwrap(), 42);
        assert_eq!(parse_bytes("1gb").unwrap(), 1_000_000_000);
        assert_eq!(parse_bytes("1 GiB").unwrap(), 1_073_741_824);
        assert_eq!(parse_bytes("1.5MB").unwrap(), 1_500_000);
        assert_eq!(parse_bytes("2,000 kB").unwrap(), 2_000_000);
        assert!(parse_bytes("abc").is_err());
        assert!(parse_bytes("1 parsec").is_err());
        assert!(parse_bytes("99999999 EB").is_err());
    }
}
