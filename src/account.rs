//! Account of the ministream server instance.

use subtle::ConstantTimeEq;
use uuid::Uuid;

use crate::config::AccountConfig;
use crate::generators::generate_random_secret_api_key;
use crate::log_info;
use crate::logger::Logger;

#[derive(Debug, Clone)]
pub struct Account {
    /// A unique id is necessary so that a JWT cannot be used on another server instance.
    pub id: Uuid,
    pub name: String,
    secret_api_key: String,
}

impl Account {
    /// Initialize the account from its configuration, generating a random id
    /// and/or a random secret API key when they are not configured.
    pub fn new(config: &AccountConfig, logger: &Logger) -> Account {
        let mut id = config.id;
        if id.is_nil() {
            id = Uuid::new_v4();
            log_info!(logger, "Set random account id", "topic" => "account", "method" => "Initialize", "accountid" => id);
        }
        let mut secret_api_key = config.secret_api_key.clone();
        if secret_api_key.is_empty() {
            // Generate a random secret API key because it is not set in the configuration file,
            // and output it in the logs for the administrator to know it.
            secret_api_key = generate_random_secret_api_key(32);
            log_info!(logger, "Set random SecretAPIKey", "topic" => "account", "method" => "Initialize", "secretapikey" => secret_api_key);
        }
        Account {
            id,
            name: config.name.clone(),
            secret_api_key,
        }
    }

    /// Check an API key (constant time comparison).
    pub fn check_api_key(&self, api_key: &str) -> bool {
        !api_key.is_empty() && bool::from(api_key.as_bytes().ct_eq(self.secret_api_key.as_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_values_when_not_configured() {
        let account = Account::new(&AccountConfig::default(), &Logger::nop());
        assert!(!account.id.is_nil());
        assert_eq!(account.secret_api_key.len(), 32);
        assert!(!account.check_api_key(""));
        assert!(account.check_api_key(&account.secret_api_key.clone()));
    }

    #[test]
    fn configured_values() {
        let config = AccountConfig {
            id: Uuid::parse_str("4ce589e2-b483-467b-8b59-758b339801d0").unwrap(),
            name: "demo".into(),
            secret_api_key: "s3cr3t".into(),
        };
        let account = Account::new(&config, &Logger::nop());
        assert_eq!(account.id, config.id);
        assert!(account.check_api_key("s3cr3t"));
        assert!(!account.check_api_key("s3cr3"));
    }
}
