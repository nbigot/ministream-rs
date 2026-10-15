//! Role based access control (RBAC) with optional attribute based access control (ABAC).
//!
//! The RBAC configuration is a JSON file defining users, roles and rules:
//! a user has roles, a role has rules, and a rule grants actions.
//! A rule may also have an ABAC jq expression which must evaluate to `true`
//! on the resource for the action to be granted.

use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;

use crate::error::{Error, Result};
use crate::jq::JqFilter;
use crate::log_info;
use crate::logger::Logger;

pub const ACTION_GET_RECORDS: &str = "GetRecords";
pub const ACTION_CREATE_RECORDS_ITERATOR: &str = "CreateRecordsIterator";
pub const ACTION_PUT_RECORDS: &str = "PutRecords";
pub const ACTION_PUT_RECORD: &str = "PutRecord";
pub const ACTION_GET_RECORDS_ITERATOR_STATS: &str = "GetRecordsIteratorStats";
pub const ACTION_LIST_STREAMS: &str = "ListStreams";
pub const ACTION_LIST_STREAMS_PROPERTIES: &str = "ListStreamsProperties";
pub const ACTION_GET_STREAM_DESCRIPTION: &str = "GetStreamDescription";
pub const ACTION_GET_STREAM_PROPERTIES: &str = "GetStreamProperties";
pub const ACTION_SET_STREAM_PROPERTIES: &str = "SetStreamProperties";
pub const ACTION_UPDATE_STREAM_PROPERTIES: &str = "UpdateStreamProperties";
pub const ACTION_CREATE_STREAM: &str = "CreateStream";
pub const ACTION_DELETE_STREAM: &str = "DeleteStream";
pub const ACTION_CLOSE_RECORDS_ITERATOR: &str = "CloseRecordsIterator";
pub const ACTION_REBUILD_INDEX: &str = "RebuildIndex";
pub const ACTION_LIST_USERS: &str = "ListUsers";
pub const ACTION_GET_ACCOUNT: &str = "GetAccount";
pub const ACTION_SHUTDOWN_SERVER: &str = "ShutdownServer";
pub const ACTION_RESTART_SERVER: &str = "RestartServer";
pub const ACTION_JWT_REVOKE_ALL: &str = "JWTRevokeAll";

/// All the valid actions.
pub const ACTION_LIST: &[&str] = &[
    ACTION_GET_RECORDS,
    ACTION_CREATE_RECORDS_ITERATOR,
    ACTION_PUT_RECORDS,
    ACTION_PUT_RECORD,
    ACTION_GET_RECORDS_ITERATOR_STATS,
    ACTION_LIST_STREAMS,
    ACTION_LIST_STREAMS_PROPERTIES,
    ACTION_GET_STREAM_DESCRIPTION,
    ACTION_GET_STREAM_PROPERTIES,
    ACTION_SET_STREAM_PROPERTIES,
    ACTION_UPDATE_STREAM_PROPERTIES,
    ACTION_CREATE_STREAM,
    ACTION_DELETE_STREAM,
    ACTION_CLOSE_RECORDS_ITERATOR,
    ACTION_REBUILD_INDEX,
    ACTION_LIST_USERS,
    ACTION_GET_ACCOUNT,
    ACTION_SHUTDOWN_SERVER,
    ACTION_RESTART_SERVER,
    ACTION_JWT_REVOKE_ALL,
];

/// ABAC rule: a jq expression evaluated on the resource attributes.
#[derive(Debug, Clone)]
pub struct Abac {
    pub jq_def: String,
    pub filter: JqFilter,
}

impl Abac {
    fn new(jq_def: &str) -> Result<Option<Abac>> {
        if jq_def.is_empty() {
            return Ok(None);
        }
        let filter =
            JqFilter::parse(jq_def).map_err(|e| Error::msg(format!("can't create ABAC: {e}")))?;
        Ok(Some(Abac {
            jq_def: jq_def.to_string(),
            filter,
        }))
    }

    /// Evaluate the ABAC rule on the given attributes.
    ///
    /// The jq expression must return a boolean, the access is denied when it yields no output.
    pub fn check(&self, attributes: &serde_json::Value) -> Result<bool> {
        match self.filter.first(attributes).map_err(Error::msg)? {
            Some(serde_json::Value::Bool(granted)) => Ok(granted),
            Some(_) => Err(Error::msg("abac jq filter did not return a boolean value")),
            None => Ok(false),
        }
    }
}

#[derive(Debug)]
pub struct Rule {
    pub id: String,
    pub actions: Vec<String>,
    pub abac: Option<Abac>,
}

#[derive(Debug)]
pub struct Role {
    pub id: String,
    pub description: String,
    pub rules: Vec<Arc<Rule>>,
}

#[derive(Debug)]
pub struct User {
    pub id: String,
    pub description: String,
    pub roles: Vec<Arc<Role>>,
}

impl User {
    pub fn role_ids(&self) -> Vec<String> {
        self.roles.iter().map(|r| r.id.clone()).collect()
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct RbacDocument {
    users: Vec<UserDocument>,
    roles: Vec<RoleDocument>,
    rules: Vec<RuleDocument>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct UserDocument {
    id: String,
    description: String,
    roles: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct RoleDocument {
    id: String,
    description: String,
    rules: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct RuleDocument {
    id: String,
    actions: Vec<String>,
    abac: String,
}

/// RBAC configuration.
#[derive(Debug, Default)]
pub struct Rbac {
    pub users: HashMap<String, Arc<User>>,
    pub roles: HashMap<String, Arc<Role>>,
    pub rules: HashMap<String, Arc<Rule>>,
}

impl Rbac {
    /// Parse a RBAC JSON document.
    pub fn from_json(document: &str) -> Result<Rbac> {
        let doc: RbacDocument = serde_json::from_str(document)?;
        let mut rbac = Rbac::default();

        for r in doc.rules {
            if rbac.rules.contains_key(&r.id) {
                return Err(Error::msg(format!("RBAC Rule id must be unique: {}", r.id)));
            }
            let rule = Rule {
                id: r.id.clone(),
                actions: r.actions,
                abac: Abac::new(&r.abac)?,
            };
            rbac.rules.insert(r.id, Arc::new(rule));
        }

        for r in doc.roles {
            if rbac.roles.contains_key(&r.id) {
                return Err(Error::msg(format!("RBAC Role id must be unique: {}", r.id)));
            }
            let mut rules = Vec::with_capacity(r.rules.len());
            for rule_id in &r.rules {
                let rule = rbac.rules.get(rule_id).ok_or_else(|| {
                    Error::msg(format!(
                        "RBAC Rule id {} not found for role: {}",
                        rule_id, r.id
                    ))
                })?;
                rules.push(rule.clone());
            }
            let role = Role {
                id: r.id.clone(),
                description: r.description,
                rules,
            };
            rbac.roles.insert(r.id, Arc::new(role));
        }

        for u in doc.users {
            if rbac.users.contains_key(&u.id) {
                return Err(Error::msg(format!("RBAC user id must be unique: {}", u.id)));
            }
            let mut roles = Vec::with_capacity(u.roles.len());
            for role_id in &u.roles {
                let role = rbac.roles.get(role_id).ok_or_else(|| {
                    Error::msg(format!(
                        "RBAC Role id {} not found for user: {}",
                        role_id, u.id
                    ))
                })?;
                roles.push(role.clone());
            }
            let user = User {
                id: u.id.clone(),
                description: u.description,
                roles,
            };
            rbac.users.insert(u.id, Arc::new(user));
        }

        Ok(rbac)
    }

    pub fn get_user(&self, user_id: &str) -> Option<Arc<User>> {
        self.users.get(user_id).cloned()
    }

    /// Sorted list of the user ids.
    pub fn get_user_list(&self) -> Vec<String> {
        let mut users: Vec<String> = self.users.keys().cloned().collect();
        users.sort();
        users
    }

    /// Resolve role names into roles.
    pub fn get_roles(&self, role_names: &[String]) -> Result<Vec<Arc<Role>>> {
        role_names
            .iter()
            .map(|name| {
                self.roles
                    .get(name)
                    .cloned()
                    .ok_or_else(|| Error::msg(format!("unknown role {name}")))
            })
            .collect()
    }

    /// Check that every action of every rule is a known action.
    pub fn validate_actions(&self, valid_actions: &[&str]) -> Result<()> {
        for rule in self.rules.values() {
            for action in &rule.actions {
                if !valid_actions.contains(&action.as_str()) {
                    return Err(Error::msg(format!(
                        "RBAC Rule action unknown: {} in rule id {}",
                        action, rule.id
                    )));
                }
            }
        }
        Ok(())
    }
}

/// RBAC manager (holds the RBAC configuration when RBAC is enabled).
#[derive(Debug, Default)]
pub struct RbacManager {
    rbac: Option<Rbac>,
}

impl RbacManager {
    pub fn disabled() -> RbacManager {
        RbacManager { rbac: None }
    }

    pub fn new(logger: &Logger, enable: bool, filename: &str) -> Result<RbacManager> {
        log_info!(logger, "Loading server RBAC auth configuration", "topic" => "rbac", "method" => "NewRBACManager");
        if !enable {
            log_info!(logger, "RBAC is disabled in configuration", "topic" => "rbac", "method" => "NewRBACManager");
            return Ok(RbacManager::disabled());
        }
        log_info!(logger, "Loading RBAC", "topic" => "rbac", "method" => "LoadConfiguration", "filename" => filename);
        let document = std::fs::read_to_string(filename)
            .map_err(|e| Error::msg(format!("Can't open RBAC file {filename}: {e}")))?;
        let rbac = Rbac::from_json(&document)
            .map_err(|e| Error::msg(format!("Can't load RBAC file {filename}: {e}")))?;
        rbac.validate_actions(ACTION_LIST)?;
        Ok(RbacManager::from_rbac(rbac))
    }

    pub fn from_rbac(rbac: Rbac) -> RbacManager {
        RbacManager { rbac: Some(rbac) }
    }

    pub fn is_enabled(&self) -> bool {
        self.rbac.is_some()
    }

    pub fn rbac(&self) -> Option<&Rbac> {
        self.rbac.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn template() -> Rbac {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("config-templates/secured-inmemory/data/secrets/rbac.json");
        Rbac::from_json(&std::fs::read_to_string(path).unwrap()).unwrap()
    }

    #[test]
    fn load_template() {
        let rbac = template();
        rbac.validate_actions(ACTION_LIST).unwrap();
        assert_eq!(
            rbac.get_user_list(),
            vec!["admin", "consumer", "dba", "demo", "producer"]
        );
        let admin = rbac.get_user("admin").unwrap();
        assert_eq!(admin.role_ids(), vec!["role_admin"]);
        assert_eq!(admin.roles[0].rules.len(), 4);
        assert!(rbac.get_roles(&["role_dba".into(), "nope".into()]).is_err());
        let demo_rule = rbac.rules.get("rule_demo").unwrap();
        let abac = demo_rule.abac.as_ref().unwrap();
        assert!(abac
            .check(&json!({"properties": {"project": "demo", "env": "test"}}))
            .unwrap());
        assert!(!abac
            .check(&json!({"properties": {"project": "other"}}))
            .unwrap());
    }

    #[test]
    fn invalid_documents() {
        assert!(Rbac::from_json(r#"{"rules": [{"id": "a"}, {"id": "a"}]}"#).is_err());
        assert!(Rbac::from_json(r#"{"roles": [{"id": "r", "rules": ["missing"]}]}"#).is_err());
        assert!(Rbac::from_json(r#"{"users": [{"id": "u", "roles": ["missing"]}]}"#).is_err());
        assert!(Rbac::from_json(r#"{"rules": [{"id": "a", "abac": ".a =="}]}"#).is_err());
        let rbac = Rbac::from_json(r#"{"rules": [{"id": "a", "actions": ["Fly"]}]}"#).unwrap();
        assert!(rbac.validate_actions(ACTION_LIST).is_err());
    }

    #[test]
    fn abac_must_return_a_boolean() {
        let abac = Abac::new(".name").unwrap().unwrap();
        assert!(abac.check(&json!({"name": "x"})).is_err());
        let abac = Abac::new("select(.x)").unwrap().unwrap();
        assert!(!abac.check(&json!({"x": false})).unwrap());
    }
}
