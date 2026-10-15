//! Random strings generators (salts, passwords, API keys).
//!
//! All generators use a cryptographically secure random number generator.

use rand::Rng;

pub const SALT_CHARS: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
pub const PASSWORD_CHARS: &str =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-.";
pub const SECRET_API_KEY_CHARS: &str =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

pub fn generate_random_salt(length: usize) -> String {
    generate_random_string(length, SALT_CHARS)
}

pub fn generate_random_password(length: usize) -> String {
    generate_random_string(length, PASSWORD_CHARS)
}

pub fn generate_random_secret_api_key(length: usize) -> String {
    generate_random_string(length, SECRET_API_KEY_CHARS)
}

pub fn generate_random_string(length: usize, chars: &str) -> String {
    let chars: Vec<char> = chars.chars().collect();
    let mut rng = rand::rng();
    (0..length)
        .map(|_| chars[rng.random_range(0..chars.len())])
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_strings() {
        let key = generate_random_secret_api_key(32);
        assert_eq!(key.len(), 32);
        assert!(key.chars().all(|c| SECRET_API_KEY_CHARS.contains(c)));
        assert_ne!(key, generate_random_secret_api_key(32));
        assert!(generate_random_password(40)
            .chars()
            .all(|c| PASSWORD_CHARS.contains(c)));
        assert_eq!(generate_random_salt(0), "");
    }
}
