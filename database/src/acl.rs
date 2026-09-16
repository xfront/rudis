//! ACL (Access Control List) module.
//!
//! Implements user management and permission checking for the rudis server.

use std::collections::{HashMap, HashSet};

/// Represents a key pattern for ACL key permissions.
#[derive(Debug, Clone)]
pub enum KeyPattern {
    /// Allow access to all keys.
    AllKeys,
    /// Allow access to keys matching a glob pattern.
    Pattern(Vec<u8>),
}

/// An ACL selector (Redis 7.0+), defining key and command permissions.
#[derive(Debug, Clone)]
pub struct AclSelector {
    /// Whether this selector allows all keys.
    pub all_keys: bool,
    /// Key patterns this selector allows.
    pub key_patterns: Vec<KeyPattern>,
    /// Whether this selector allows all commands.
    pub all_commands: bool,
    /// Commands this selector allows.
    pub allowed_commands: HashSet<String>,
    /// Commands this selector denies (takes precedence over allowed).
    pub denied_commands: HashSet<String>,
}

impl Default for AclSelector {
    fn default() -> Self {
        AclSelector {
            all_keys: false,
            key_patterns: Vec::new(),
            all_commands: false,
            allowed_commands: HashSet::new(),
            denied_commands: HashSet::new(),
        }
    }
}

/// An ACL user.
#[derive(Debug, Clone)]
pub struct AclUser {
    /// Whether the user is enabled.
    pub enabled: bool,
    /// Passwords (stored as plain text for simplicity; in production use hashed).
    pub passwords: Vec<String>,
    /// Whether no password is required (nopass flag).
    pub nopass: bool,
    /// The default selector (index 0).
    pub default_selector: AclSelector,
    /// Additional selectors (Redis 7.0+).
    pub selectors: Vec<AclSelector>,
    /// Flags: "on", "off", "allkeys", "allcommands", "nopass".
    pub flags: HashSet<String>,
}

impl AclUser {
    pub fn new() -> Self {
        AclUser {
            enabled: false,
            passwords: Vec::new(),
            nopass: false,
            default_selector: AclSelector::default(),
            selectors: Vec::new(),
            flags: HashSet::new(),
        }
    }

    /// Create the default "default" user with full permissions.
    pub fn default_user() -> Self {
        let mut user = AclUser::new();
        user.enabled = true;
        user.nopass = true;
        user.default_selector.all_keys = true;
        user.default_selector.all_commands = true;
        user.flags.insert("on".to_owned());
        user.flags.insert("allkeys".to_owned());
        user.flags.insert("allcommands".to_owned());
        user.flags.insert("nopass".to_owned());
        user
    }

    /// Check if the given password matches any stored password.
    pub fn check_password(&self, password: &str) -> bool {
        if self.nopass {
            return true;
        }
        self.passwords.iter().any(|p| p == password)
    }

    /// Check if the user is allowed to execute a command.
    pub fn can_execute(&self, command: &str) -> bool {
        if self.default_selector.all_commands {
            return !self.default_selector.denied_commands.contains(command);
        }
        self.default_selector.allowed_commands.contains(command)
            && !self.default_selector.denied_commands.contains(command)
    }

    /// Check if the user is allowed to access a key.
    pub fn can_access_key(&self, _key: &[u8]) -> bool {
        if self.default_selector.all_keys {
            return true;
        }
        // For now, if there are key patterns, we do a simple check
        // A full implementation would use glob matching
        self.default_selector.key_patterns.is_empty() || self.default_selector.all_keys
    }

    /// Format the user for ACL LIST output.
    pub fn to_acl_string(&self, name: &str) -> String {
        let mut parts = vec![format!("user {}", name)];
        if self.enabled {
            parts.push("on".to_owned());
        } else {
            parts.push("off".to_owned());
        }
        if self.nopass {
            parts.push("nopass".to_owned());
        } else {
            for pwd in &self.passwords {
                parts.push(format!("#{}", pwd));
            }
        }
        if self.default_selector.all_keys {
            parts.push("~*".to_owned());
        }
        if self.default_selector.all_commands {
            parts.push("+@all".to_owned());
        } else {
            for cmd in &self.default_selector.allowed_commands {
                parts.push(format!("+{}", cmd));
            }
            for cmd in &self.default_selector.denied_commands {
                parts.push(format!("-{}", cmd));
            }
        }
        parts.join(" ")
    }
}

/// The ACL subsystem, managing all users.
#[derive(Debug, Clone)]
pub struct Acl {
    /// All users, keyed by username.
    pub users: HashMap<String, AclUser>,
    // The current authenticated username for a connection is per-connection
    // state stored in the Client struct, not here. The ACL provides the lookup.
}

impl Default for Acl {
    fn default() -> Self {
        Self::new()
    }
}

impl Acl {
    pub fn new() -> Self {
        let mut users = HashMap::new();
        users.insert("default".to_owned(), AclUser::default_user());
        Acl { users }
    }

    /// Authenticate a user. Returns true if authentication succeeds.
    pub fn authenticate(&self, username: &str, password: &str) -> bool {
        match self.users.get(username) {
            Some(user) => {
                if !user.enabled {
                    return false;
                }
                user.check_password(password)
            }
            None => false,
        }
    }

    /// Check if a user can execute a command.
    pub fn can_execute(&self, username: &str, command: &str) -> bool {
        match self.users.get(username) {
            Some(user) => user.can_execute(command),
            None => false,
        }
    }

    /// Check if a user can access a key.
    pub fn can_access_key(&self, username: &str, key: &[u8]) -> bool {
        match self.users.get(username) {
            Some(user) => user.can_access_key(key),
            None => false,
        }
    }

    /// Set or create a user with the given rules.
    pub fn set_user(&mut self, username: &str, rules: &[&str]) -> Result<(), String> {
        let user = self.users.entry(username.to_owned()).or_insert_with(AclUser::new);
        for rule in rules {
            let rule = *rule;
            if rule == "on" {
                user.enabled = true;
                user.flags.insert("on".to_owned());
                user.flags.remove("off");
            } else if rule == "off" {
                user.enabled = false;
                user.flags.insert("off".to_owned());
                user.flags.remove("on");
            } else if rule == "nopass" {
                user.nopass = true;
                user.passwords.clear();
                user.flags.insert("nopass".to_owned());
            } else if rule == "resetpass" {
                user.nopass = false;
                user.passwords.clear();
                user.flags.remove("nopass");
            } else if rule.starts_with('>') {
                // Add password
                user.passwords.push(rule[1..].to_owned());
                user.nopass = false;
                user.flags.remove("nopass");
            } else if rule.starts_with('#') {
                // Add hashed password
                user.passwords.push(rule[1..].to_owned());
                user.nopass = false;
                user.flags.remove("nopass");
            } else if rule == "allkeys" || rule == "~*" {
                user.default_selector.all_keys = true;
                user.flags.insert("allkeys".to_owned());
            } else if rule == "allcommands" || rule == "+@all" {
                user.default_selector.all_commands = true;
                user.flags.insert("allcommands".to_owned());
            } else if rule.starts_with('+') {
                user.default_selector.allowed_commands.insert(rule[1..].to_owned());
            } else if rule.starts_with('-') {
                user.default_selector.denied_commands.insert(rule[1..].to_owned());
            } else if rule.starts_with('~') {
                user.default_selector.key_patterns.push(
                    KeyPattern::Pattern(rule[1..].as_bytes().to_vec())
                );
            } else {
                return Err(format!("ERR Unrecognized ACL rule '{}'", rule));
            }
        }
        Ok(())
    }

    /// Delete a user. Returns true if the user existed.
    pub fn del_user(&mut self, username: &str) -> bool {
        if username == "default" {
            return false; // Cannot delete the default user
        }
        self.users.remove(username).is_some()
    }

    /// Generate a random password of the given length (in bytes, encoded as hex).
    pub fn genpass(bits: usize) -> String {
        let bytes = bits / 8;
        let mut password = Vec::with_capacity(bytes);
        for _ in 0..bytes {
            password.push(rand::random::<u8>());
        }
        password.iter().map(|b| format!("{:02x}", b)).collect()
    }

    /// Get the list of ACL command categories.
    pub fn categories() -> Vec<&'static str> {
        vec![
            "keyspace", "read", "write", "set", "sortedset", "list", "hash", "string",
            "bitmap", "hyperloglog", "geo", "stream", "pubsub", "admin", "fast", "slow",
            "blocking", "dangerous", "connection", "transaction", "scripting", "server",
        ]
    }
}

#[cfg(test)]
mod test_acl {
    use super::*;

    #[test]
    fn test_default_user() {
        let acl = Acl::new();
        assert!(acl.authenticate("default", ""));
        assert!(acl.authenticate("default", "anything"));
        assert!(acl.can_execute("default", "get"));
        assert!(acl.can_execute("default", "set"));
    }

    #[test]
    fn test_set_user_with_password() {
        let mut acl = Acl::new();
        acl.set_user("testuser", &["on", ">secret123"]).unwrap();
        assert!(acl.authenticate("testuser", "secret123"));
        assert!(!acl.authenticate("testuser", "wrong"));
    }

    #[test]
    fn test_set_user_disabled() {
        let mut acl = Acl::new();
        acl.set_user("testuser", &["off", ">secret123"]).unwrap();
        assert!(!acl.authenticate("testuser", "secret123"));
    }

    #[test]
    fn test_del_user() {
        let mut acl = Acl::new();
        acl.set_user("testuser", &["on"]).unwrap();
        assert!(acl.del_user("testuser"));
        assert!(!acl.del_user("testuser"));
        assert!(!acl.del_user("default")); // Can't delete default
    }

    #[test]
    fn test_command_permissions() {
        let mut acl = Acl::new();
        acl.set_user("limited", &["on", "nopass", "-@all", "+get", "+set"]).unwrap();
        assert!(acl.can_execute("limited", "get"));
        assert!(acl.can_execute("limited", "set"));
        assert!(!acl.can_execute("limited", "del"));
    }

    #[test]
    fn test_genpass() {
        let pass = Acl::genpass(256);
        assert_eq!(pass.len(), 64); // 256 bits = 32 bytes = 64 hex chars
    }

    #[test]
    fn test_nopass() {
        let mut acl = Acl::new();
        acl.set_user("nopass_user", &["on", "nopass"]).unwrap();
        assert!(acl.authenticate("nopass_user", ""));
        assert!(acl.authenticate("nopass_user", "anything"));
    }

    #[test]
    fn test_to_acl_string() {
        let mut acl = Acl::new();
        acl.set_user("testuser", &["on", ">secret", "+@all", "~*"]).unwrap();
        let user = acl.users.get("testuser").unwrap();
        let s = user.to_acl_string("testuser");
        assert!(s.contains("user testuser"));
        assert!(s.contains("on"));
    }
}
