//! PBKDF2 password hashes.
//!
//! String format: `$pbkdf2-<digest>$i=<iterations>$<salt>$<hash>` where the hash is
//! the hexadecimal representation of the derived key.

use std::sync::LazyLock;

use regex::Regex;
use subtle::ConstantTimeEq;

use crate::error::{Error, Result};

static HASH_FORMAT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^\$pbkdf2-([^$]+)\$i=(\d+)\$([^$]+)\$([^\r\n\t ]+)").expect("valid regex")
});

/// HMAC digest algorithm used by PBKDF2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Digest {
    Md5,
    Sha1,
    Sha256,
    Sha512,
}

impl Digest {
    pub fn parse(digest: &str) -> Result<Digest> {
        match digest {
            "md5" => Ok(Digest::Md5),
            "sha1" => Ok(Digest::Sha1),
            "sha256" => Ok(Digest::Sha256),
            "sha512" => Ok(Digest::Sha512),
            other => Err(Error::msg(format!("not implemented hash method: {other}"))),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Digest::Md5 => "md5",
            Digest::Sha1 => "sha1",
            Digest::Sha256 => "sha256",
            Digest::Sha512 => "sha512",
        }
    }

    /// Size in bytes of the derived key.
    fn key_len(&self) -> usize {
        match self {
            Digest::Md5 => 16,
            Digest::Sha1 => 20,
            Digest::Sha256 => 32,
            Digest::Sha512 => 64,
        }
    }

    /// Derive a key and return its hexadecimal representation.
    pub fn derive_hex(&self, password: &str, salt: &str, iterations: u32) -> String {
        let mut key = vec![0u8; self.key_len()];
        let (password, salt) = (password.as_bytes(), salt.as_bytes());
        match self {
            Digest::Md5 => pbkdf2::pbkdf2_hmac::<md5::Md5>(password, salt, iterations, &mut key),
            Digest::Sha1 => pbkdf2::pbkdf2_hmac::<sha1::Sha1>(password, salt, iterations, &mut key),
            Digest::Sha256 => {
                pbkdf2::pbkdf2_hmac::<sha2::Sha256>(password, salt, iterations, &mut key)
            }
            Digest::Sha512 => {
                pbkdf2::pbkdf2_hmac::<sha2::Sha512>(password, salt, iterations, &mut key)
            }
        }
        hex::encode(key)
    }
}

/// A parsed PBKDF2 password hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pbkdf2Hash {
    digest: Digest,
    iterations: u32,
    salt: String,
    hash: String,
}

impl Pbkdf2Hash {
    /// Parse a hash in the `$pbkdf2-<digest>$i=<iterations>$<salt>$<hash>` format.
    pub fn parse(hash: &str) -> Result<Pbkdf2Hash> {
        if hash.is_empty() {
            return Err(Error::msg("empty hash"));
        }
        let parts = HASH_FORMAT
            .captures(hash)
            .ok_or_else(|| Error::msg("invalid hash format"))?;
        let digest = Digest::parse(&parts[1])?;
        let iterations: u32 = parts[2]
            .parse()
            .map_err(|_| Error::msg(format!("invalid iterations: {}", &parts[2])))?;
        if iterations == 0 {
            return Err(Error::msg("invalid iterations: 0"));
        }
        Ok(Pbkdf2Hash {
            digest,
            iterations,
            salt: parts[3].to_string(),
            hash: parts[4].to_ascii_lowercase(),
        })
    }

    /// Check a password against the hash (constant time comparison).
    pub fn verify(&self, password: &str) -> bool {
        let computed = self
            .digest
            .derive_hex(password, &self.salt, self.iterations);
        computed.as_bytes().ct_eq(self.hash.as_bytes()).into()
    }
}

impl std::fmt::Display for Pbkdf2Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "$pbkdf2-{}$i={}${}${}",
            self.digest.as_str(),
            self.iterations,
            self.salt,
            self.hash
        )
    }
}

/// Hash a password and return its string representation.
pub fn hash_password(digest: &str, iterations: i64, salt: &str, password: &str) -> Result<String> {
    let digest = Digest::parse(digest)?;
    let iterations = u32::try_from(iterations)
        .ok()
        .filter(|i| *i > 0)
        .ok_or_else(|| Error::msg("iterations must be a positive integer"))?;
    if salt.contains('$') {
        return Err(Error::msg("salt cannot contain the character '$'"));
    }
    let hash = Pbkdf2Hash {
        digest,
        iterations,
        salt: salt.to_string(),
        hash: digest.derive_hex(password, salt, iterations),
    };
    Ok(hash.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vectors() {
        // reference values computed with the Go implementation of ministream
        // (go run cmd/generatepasswords/generatepasswords.go -digest <digest> -iterations 1000 ...)
        let salt = "randomStringAbcdef012346789";
        let password = "thisIsMySecretPassword";
        assert_eq!(
            hash_password("sha512", 1000, salt, password).unwrap(),
            "$pbkdf2-sha512$i=1000$randomStringAbcdef012346789$f633c9d4a6bf25f309e8ab68fc685b9fd27b678bc26854ceb21d7fff0d42691ba8287a1d4703edfe5c630a04531a452b0034223d81efd502ed0d149981e3f816"
        );
        assert_eq!(
            hash_password("sha256", 1000, salt, password).unwrap(),
            "$pbkdf2-sha256$i=1000$randomStringAbcdef012346789$e7ef1350732294958b0801ad8f389703a93048a90f36260a6212f1e6bb9f68cf"
        );
        assert_eq!(
            hash_password("md5", 1000, salt, password).unwrap(),
            "$pbkdf2-md5$i=1000$randomStringAbcdef012346789$b8fd733188e5aab7964606662d5e64cf"
        );
        // RFC 6070 test vector (PBKDF2-HMAC-SHA1, 1 iteration)
        assert_eq!(
            Digest::Sha1.derive_hex("password", "salt", 1),
            "0c60c80f961f0e71f3a9b524af6012062fe037a6"
        );
        // RFC 7914 test vector (PBKDF2-HMAC-SHA256, 1 iteration, first 32 bytes)
        assert_eq!(
            Digest::Sha256.derive_hex("passwd", "salt", 1),
            "55ac046e56e3089fec1691c22544b605f94185216dde0465e68b9d57c20dacbc"
        );
    }

    #[test]
    fn roundtrip() {
        for digest in ["md5", "sha1", "sha256", "sha512"] {
            let hash = hash_password(digest, 10, "salt1234", "secret").unwrap();
            let parsed = Pbkdf2Hash::parse(&hash).unwrap();
            assert_eq!(parsed.to_string(), hash);
            assert!(parsed.verify("secret"));
            assert!(!parsed.verify("Secret"));
        }
    }

    #[test]
    fn invalid_hashes() {
        assert!(Pbkdf2Hash::parse("").is_err());
        assert!(Pbkdf2Hash::parse("plain text").is_err());
        assert!(Pbkdf2Hash::parse("$pbkdf2-sha3$i=1$salt$abcd").is_err());
        assert!(Pbkdf2Hash::parse("$pbkdf2-sha256$i=0$salt$abcd").is_err());
        assert!(hash_password("sha512", 0, "salt", "pwd").is_err());
        assert!(hash_password("whirlpool", 1, "salt", "pwd").is_err());
    }
}
