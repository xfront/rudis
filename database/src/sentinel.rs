//! Redis Sentinel data structures and state management.
//!
//! Implements sentinel monitoring, failure detection, and failover
//! for Redis master-replica setups.

use std::collections::HashMap;
use std::fmt;

/// Sentinel state: monitors one or more master instances.
#[derive(Debug, Clone)]
pub struct SentinelState {
    /// This sentinel's unique ID (40 hex chars, 20 bytes).
    pub sentinel_id: [u8; 20],
    /// Monitored masters, keyed by name.
    pub masters: HashMap<String, MonitoredMaster>,
    /// Known sentinels monitoring the same masters (discovered via pub/sub).
    pub known_sentinels: HashMap<String, Vec<SentinelInfo>>,
    /// Current run ID (re-generated on restart).
    pub run_id: String,
}

/// Information about a monitored master instance.
#[derive(Debug, Clone)]
pub struct MonitoredMaster {
    /// Master name (user-defined).
    pub name: String,
    /// Master IP address.
    pub ip: String,
    /// Master port.
    pub port: u16,
    /// Quorum: number of sentinels that must agree on ODOWN.
    pub quorum: u32,
    /// Milliseconds after which a non-responsive master is considered SDOWN.
    pub down_after_ms: u64,
    /// Number of replicas that can be reconfigured simultaneously.
    pub parallel_syncs: u32,
    /// Failover timeout in milliseconds.
    pub failover_timeout: u64,
    /// Notification script (optional).
    pub notification_script: Option<String>,
    /// Client reconfiguration script (optional).
    pub client_reconfig_script: Option<String>,
    /// Current state of the master.
    pub state: MasterState,
    /// Timestamp of last PING sent to master.
    pub last_ping: i64,
    /// Timestamp of last PONG received from master.
    pub last_pong: i64,
    /// Timestamp when SDOWN was detected.
    pub sdown_since: i64,
    /// Known replicas of this master.
    pub replicas: Vec<ReplicaInfo>,
    /// Epoch of the last failover for this master.
    pub failover_epoch: u64,
    /// Whether a failover is in progress.
    pub failover_in_progress: bool,
}

/// State of a monitored master.
#[derive(Debug, Clone, PartialEq)]
pub enum MasterState {
    /// Master is responsive.
    Ok,
    /// Master is subjectively down (this sentinel thinks it's down).
    Sdown,
    /// Master is objectively down (quorum of sentinels agree).
    Odown,
}

impl fmt::Display for MasterState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MasterState::Ok => write!(f, "ok"),
            MasterState::Sdown => write!(f, "sdown"),
            MasterState::Odown => write!(f, "odown"),
        }
    }
}

/// Information about a replica of a monitored master.
#[derive(Debug, Clone)]
pub struct ReplicaInfo {
    /// Replica IP.
    pub ip: String,
    /// Replica port.
    pub port: u16,
    /// Replica state.
    pub state: ReplicaState,
    /// Last PING sent.
    pub last_ping: i64,
    /// Last PONG received.
    pub last_pong: i64,
    /// Master link status.
    pub master_link_status: bool,
    /// Replica priority.
    pub priority: u32,
}

/// State of a replica.
#[derive(Debug, Clone, PartialEq)]
pub enum ReplicaState {
    Ok,
    Sdown,
}

impl fmt::Display for ReplicaState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplicaState::Ok => write!(f, "ok"),
            ReplicaState::Sdown => write!(f, "sdown"),
        }
    }
}

/// Information about another sentinel monitoring the same master.
#[derive(Debug, Clone)]
pub struct SentinelInfo {
    /// Sentinel's run ID.
    pub run_id: String,
    /// Sentinel IP.
    pub ip: String,
    /// Sentinel port.
    pub port: u16,
}

impl SentinelState {
    /// Create a new sentinel state.
    pub fn new() -> Self {
        let sentinel_id = crate::cluster::generate_node_id();
        let run_id = util::get_random_hex_chars(40);
        SentinelState {
            sentinel_id,
            masters: HashMap::new(),
            known_sentinels: HashMap::new(),
            run_id,
        }
    }

    /// Get the sentinel ID as a hex string.
    pub fn sentinel_id_hex(&self) -> String {
        crate::cluster::node_id_to_hex(&self.sentinel_id)
    }

    /// Add a master to monitor.
    pub fn add_master(&mut self, name: String, ip: String, port: u16, quorum: u32) {
        let master = MonitoredMaster {
            name: name.clone(),
            ip,
            port,
            quorum,
            down_after_ms: 30000,
            parallel_syncs: 1,
            failover_timeout: 180000,
            notification_script: None,
            client_reconfig_script: None,
            state: MasterState::Ok,
            last_ping: 0,
            last_pong: util::mstime(),
            sdown_since: 0,
            replicas: Vec::new(),
            failover_epoch: 0,
            failover_in_progress: false,
        };
        self.masters.insert(name, master);
    }

    /// Remove a monitored master.
    pub fn remove_master(&mut self, name: &str) -> bool {
        self.masters.remove(name).is_some()
    }

    /// Get a master by name.
    pub fn get_master(&self, name: &str) -> Option<&MonitoredMaster> {
        self.masters.get(name)
    }

    /// Get a mutable master by name.
    pub fn get_master_mut(&mut self, name: &str) -> Option<&mut MonitoredMaster> {
        self.masters.get_mut(name)
    }

    /// Check health of all monitored masters.
    /// Returns a list of masters that are SDOWN.
    pub fn check_health(&mut self) -> Vec<String> {
        let now = util::mstime();
        let mut sdown_masters = Vec::new();

        for (name, master) in self.masters.iter_mut() {
            let elapsed = now - master.last_pong;
            if elapsed > master.down_after_ms as i64 {
                if master.state == MasterState::Ok {
                    master.state = MasterState::Sdown;
                    master.sdown_since = now;
                }
                sdown_masters.push(name.clone());
            } else {
                if master.state == MasterState::Sdown {
                    master.state = MasterState::Ok;
                    master.sdown_since = 0;
                }
            }
        }

        sdown_masters
    }

    /// Record a PONG from a master.
    pub fn record_pong(&mut self, name: &str) {
        if let Some(master) = self.masters.get_mut(name) {
            master.last_pong = util::mstime();
            if master.state == MasterState::Sdown {
                master.state = MasterState::Ok;
                master.sdown_since = 0;
            }
        }
    }

    /// Get master info as a formatted string (for SENTINEL MASTER).
    pub fn master_info_string(master: &MonitoredMaster) -> String {
        let now = util::mstime();
        format!(
            "name:{}\r\nip:{}\r\nport:{}\r\nquorum:{}\r\n\
             down-after-milliseconds:{}\r\nparallel-syncs:{}\r\n\
             failover-timeout:{}\r\nflags:{}\r\n\
             num-slaves:{}\r\nnum-other-sentinels:0\r\n\
             last-ping-sent:{}\r\nlast-ok-ping-received:{}\r\n\
             master-link-down-time:0\r\n",
            master.name,
            master.ip,
            master.port,
            master.quorum,
            master.down_after_ms,
            master.parallel_syncs,
            master.failover_timeout,
            master.state,
            master.replicas.len(),
            master.last_ping,
            now - master.last_pong,
        )
    }

    /// Reset masters matching a pattern.
    pub fn reset_pattern(&mut self, pattern: &str) -> usize {
        let names: Vec<String> = self.masters.keys()
            .filter(|name| util::glob_match(pattern.as_bytes(), name.as_bytes(), true))
            .cloned()
            .collect();
        let count = names.len();
        for name in names {
            if let Some(master) = self.masters.get_mut(&name) {
                master.state = MasterState::Ok;
                master.sdown_since = 0;
                master.last_pong = util::mstime();
                master.replicas.clear();
            }
        }
        count
    }
}

#[cfg(test)]
mod test_sentinel {
    use super::*;

    #[test]
    fn test_sentinel_new() {
        let state = SentinelState::new();
        assert_eq!(state.masters.len(), 0);
        assert_eq!(state.sentinel_id_hex().len(), 40);
    }

    #[test]
    fn test_add_remove_master() {
        let mut state = SentinelState::new();
        state.add_master("mymaster".to_owned(), "127.0.0.1".to_owned(), 6379, 2);
        assert!(state.get_master("mymaster").is_some());
        assert_eq!(state.get_master("mymaster").unwrap().quorum, 2);

        assert!(state.remove_master("mymaster"));
        assert!(state.get_master("mymaster").is_none());
        assert!(!state.remove_master("nonexistent"));
    }

    #[test]
    fn test_master_health() {
        let mut state = SentinelState::new();
        state.add_master("mymaster".to_owned(), "127.0.0.1".to_owned(), 6379, 2);

        // Initially OK
        assert_eq!(state.get_master("mymaster").unwrap().state, MasterState::Ok);

        // Simulate old pong
        state.masters.get_mut("mymaster").unwrap().last_pong = 0;
        let sdown = state.check_health();
        assert_eq!(sdown.len(), 1);
        assert_eq!(sdown[0], "mymaster");
        assert_eq!(state.get_master("mymaster").unwrap().state, MasterState::Sdown);

        // Record pong -> back to OK
        state.record_pong("mymaster");
        assert_eq!(state.get_master("mymaster").unwrap().state, MasterState::Ok);
    }

    #[test]
    fn test_master_info_string() {
        let mut state = SentinelState::new();
        state.add_master("mymaster".to_owned(), "127.0.0.1".to_owned(), 6379, 2);
        let master = state.get_master("mymaster").unwrap();
        let info = SentinelState::master_info_string(master);
        assert!(info.contains("name:mymaster"));
        assert!(info.contains("ip:127.0.0.1"));
        assert!(info.contains("port:6379"));
    }

    #[test]
    fn test_reset_pattern() {
        let mut state = SentinelState::new();
        state.add_master("master1".to_owned(), "127.0.0.1".to_owned(), 6379, 2);
        state.add_master("master2".to_owned(), "127.0.0.1".to_owned(), 6380, 2);
        state.add_master("other".to_owned(), "127.0.0.1".to_owned(), 6381, 2);

        let count = state.reset_pattern("master*");
        assert_eq!(count, 2);
    }
}
