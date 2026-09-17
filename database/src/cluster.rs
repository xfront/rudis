//! Redis Cluster data structures and state management.
//!
//! Implements cluster node identity, slot table, gossip protocol messages,
//! and cluster health tracking for Redis Cluster compatibility.

use std::collections::HashMap;
use std::fmt;

/// Number of hash slots in the cluster (CRC16 % 16384).
pub const CLUSTER_SLOTS: usize = 16384;

/// Length of a node ID in bytes (20 bytes = 40 hex chars).
pub const NODE_ID_LEN: usize = 20;

/// Role of a node in the cluster.
#[derive(Debug, Clone, PartialEq)]
pub enum NodeRole {
    Master,
    Replica,
}

impl fmt::Display for NodeRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeRole::Master => write!(f, "master"),
            NodeRole::Replica => write!(f, "slave"),
        }
    }
}

/// Health state of the cluster.
#[derive(Debug, Clone, PartialEq)]
pub enum ClusterHealth {
    Ok,
    Fail,
}

impl fmt::Display for ClusterHealth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClusterHealth::Ok => write!(f, "ok"),
            ClusterHealth::Fail => write!(f, "fail"),
        }
    }
}

/// Flags for a cluster node.
#[derive(Debug, Clone, PartialEq)]
pub enum NodeFlags {
    None,
    Myself,
    Master,
    Replica,
    Pfail,
    Fail,
    Handshake,
    NoAddr,
}

impl fmt::Display for NodeFlags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeFlags::None => write!(f, "noflags"),
            NodeFlags::Myself => write!(f, "myself"),
            NodeFlags::Master => write!(f, "master"),
            NodeFlags::Replica => write!(f, "slave"),
            NodeFlags::Pfail => write!(f, "fail?"),
            NodeFlags::Fail => write!(f, "fail"),
            NodeFlags::Handshake => write!(f, "handshake"),
            NodeFlags::NoAddr => write!(f, "noaddr"),
        }
    }
}

/// Slot migration state.
#[derive(Debug, Clone, PartialEq)]
pub enum SlotState {
    /// Slot is stable (owned by a node).
    Stable,
    /// Slot is being migrated TO this node from another.
    Importing(String),
    /// Slot is being migrated FROM this node to another.
    Migrating(String),
}

/// Information about a known cluster node.
#[derive(Debug, Clone)]
pub struct ClusterNode {
    /// 20-byte node ID.
    pub id: [u8; NODE_ID_LEN],
    /// IP address.
    pub ip: String,
    /// Data port.
    pub port: u16,
    /// Cluster bus port (data port + 10000).
    pub bus_port: u16,
    /// Node flags.
    pub flags: NodeFlags,
    /// Configuration epoch.
    pub config_epoch: u64,
    /// Which slots this node owns (16384 bits).
    pub slots: Vec<bool>,
    /// Timestamp of last PING sent.
    pub ping_sent: i64,
    /// Timestamp of last PONG received.
    pub pong_recv: i64,
    /// Whether a cluster bus link is established.
    pub link_established: bool,
    /// If this is a replica, the node ID of its master.
    pub replica_of: Option<[u8; NODE_ID_LEN]>,
}

impl ClusterNode {
    pub fn new(id: [u8; NODE_ID_LEN], ip: String, port: u16) -> Self {
        ClusterNode {
            id,
            ip: ip.clone(),
            port,
            bus_port: port.saturating_add(10000),
            flags: NodeFlags::None,
            config_epoch: 0,
            slots: vec![false; CLUSTER_SLOTS],
            ping_sent: 0,
            pong_recv: 0,
            link_established: false,
            replica_of: None,
        }
    }

    /// Get the hex string representation of the node ID.
    pub fn id_hex(&self) -> String {
        node_id_to_hex(&self.id)
    }

    /// Count the number of slots owned by this node.
    pub fn slot_count(&self) -> usize {
        self.slots.iter().filter(|&&s| s).count()
    }

    /// Get slot ranges as a human-readable string (e.g., "0-5460 10923-16383").
    pub fn slot_ranges_string(&self) -> String {
        slot_ranges_to_string(&self.slots)
    }
}

/// The main cluster state, stored per-database.
#[derive(Debug, Clone)]
pub struct ClusterState {
    /// Whether cluster mode is enabled.
    pub enabled: bool,
    /// This node's 20-byte ID.
    pub my_id: [u8; NODE_ID_LEN],
    /// This node's IP.
    pub my_ip: String,
    /// This node's data port.
    pub my_port: u16,
    /// This node's cluster bus port.
    pub my_bus_port: u16,
    /// Known nodes (including myself), keyed by node ID.
    pub nodes: HashMap<[u8; NODE_ID_LEN], ClusterNode>,
    /// Global slot -> node_id mapping.
    pub slot_owners: Vec<Option<[u8; NODE_ID_LEN]>>,
    /// Slots owned by this node.
    pub my_slots: Vec<bool>,
    /// Per-slot migration state.
    pub slot_states: Vec<SlotState>,
    /// Overall cluster health.
    pub state: ClusterHealth,
    /// Number of masters with at least one slot.
    pub size: usize,
    /// Current epoch (monotonically increasing).
    pub current_epoch: u64,
    /// This node's configuration epoch.
    pub config_epoch: u64,
    /// This node's role.
    pub role: NodeRole,
    /// If replica, the master's node ID.
    pub replica_of: Option<[u8; NODE_ID_LEN]>,
    /// This node's flags.
    pub flags: NodeFlags,
    /// Cluster config file path.
    pub config_file: String,
    /// Node timeout in milliseconds.
    pub node_timeout: u64,
}

impl ClusterState {
    /// Create a new cluster state.
    pub fn new(enabled: bool, ip: String, port: u16, config_file: String, node_timeout: u64) -> Self {
        let my_id = generate_node_id();
        ClusterState {
            enabled,
            my_id,
            my_ip: ip,
            my_port: port,
            my_bus_port: port.saturating_add(10000),
            nodes: HashMap::new(),
            slot_owners: vec![None; CLUSTER_SLOTS],
            my_slots: vec![false; CLUSTER_SLOTS],
            slot_states: vec![SlotState::Stable; CLUSTER_SLOTS],
            state: ClusterHealth::Ok,
            size: 0,
            current_epoch: 0,
            config_epoch: 0,
            role: NodeRole::Master,
            replica_of: None,
            flags: NodeFlags::Myself,
            config_file,
            node_timeout,
        }
    }

    /// Register this node in the nodes table (call after construction).
    pub fn register_self(&mut self) {
        let mut self_node = ClusterNode::new(self.my_id, self.my_ip.clone(), self.my_port);
        self_node.flags = NodeFlags::Myself;
        self_node.config_epoch = self.config_epoch;
        self_node.slots = self.my_slots.clone();
        self_node.pong_recv = util::mstime();
        self_node.link_established = true;
        self.nodes.insert(self.my_id, self_node);
    }

    /// Add a slot to this node.
    pub fn add_slot(&mut self, slot: usize) -> Result<(), String> {
        if slot >= CLUSTER_SLOTS {
            return Err(format!("ERR Invalid slot {} (max {})", slot, CLUSTER_SLOTS - 1));
        }
        if self.slot_owners[slot].is_some() {
            return Err(format!("ERR Slot {} is already busy", slot));
        }
        self.slot_owners[slot] = Some(self.my_id);
        self.my_slots[slot] = true;
        // Update self node
        if let Some(node) = self.nodes.get_mut(&self.my_id) {
            node.slots[slot] = true;
        }
        self.recalc_size();
        Ok(())
    }

    /// Delete a slot from this node.
    pub fn del_slot(&mut self, slot: usize) -> Result<(), String> {
        if slot >= CLUSTER_SLOTS {
            return Err(format!("ERR Invalid slot {} (max {})", slot, CLUSTER_SLOTS - 1));
        }
        if self.slot_owners[slot] != Some(self.my_id) {
            return Err(format!("ERR Slot {} is not owned by this node", slot));
        }
        self.slot_owners[slot] = None;
        self.my_slots[slot] = false;
        if let Some(node) = self.nodes.get_mut(&self.my_id) {
            node.slots[slot] = false;
        }
        self.recalc_size();
        Ok(())
    }

    /// Set slot migration state.
    pub fn set_slot_state(&mut self, slot: usize, state: SlotState) -> Result<(), String> {
        if slot >= CLUSTER_SLOTS {
            return Err(format!("ERR Invalid slot {}", slot));
        }
        self.slot_states[slot] = state;
        Ok(())
    }

    /// Flush all slots from this node.
    pub fn flush_slots(&mut self) {
        for i in 0..CLUSTER_SLOTS {
            if self.my_slots[i] {
                self.slot_owners[i] = None;
                self.my_slots[i] = false;
            }
        }
        if let Some(node) = self.nodes.get_mut(&self.my_id) {
            node.slots = vec![false; CLUSTER_SLOTS];
        }
        self.recalc_size();
    }

    /// Reset cluster state.
    pub fn reset(&mut self, hard: bool) -> Result<(), String> {
        if hard {
            self.flush_slots();
            self.config_epoch = 0;
            self.current_epoch = 0;
            self.role = NodeRole::Master;
            self.replica_of = None;
            // Remove all nodes except self
            let self_id = self.my_id;
            self.nodes.retain(|id, _| *id == self_id);
            if let Some(node) = self.nodes.get_mut(&self_id) {
                node.config_epoch = 0;
                node.slots = vec![false; CLUSTER_SLOTS];
                node.flags = NodeFlags::Myself;
                node.replica_of = None;
            }
        } else {
            // Soft reset: only if no slots assigned
            if self.my_slots.iter().any(|&s| s) {
                return Err("ERR Can't reset cluster with slots assigned".to_owned());
            }
            let self_id = self.my_id;
            self.nodes.retain(|id, _| *id == self_id);
        }
        Ok(())
    }

    /// Recalculate cluster size (number of masters with slots).
    pub fn recalc_size(&mut self) {
        let mut masters_with_slots = 0;
        for node in self.nodes.values() {
            if node.flags == NodeFlags::Master || node.flags == NodeFlags::Myself {
                if node.slot_count() > 0 {
                    masters_with_slots += 1;
                }
            }
        }
        self.size = masters_with_slots;
    }

    /// Check if the cluster is in a healthy state (all slots covered).
    pub fn check_health(&mut self) {
        let all_covered = self.slot_owners.iter().all(|s| s.is_some());
        self.state = if all_covered { ClusterHealth::Ok } else { ClusterHealth::Fail };
    }

    /// Get the node that owns a given slot.
    pub fn get_slot_owner(&self, slot: usize) -> Option<&ClusterNode> {
        self.slot_owners[slot].and_then(|id| self.nodes.get(&id))
    }

    /// Add or update a known node.
    pub fn add_node(&mut self, id: [u8; NODE_ID_LEN], ip: String, port: u16) {
        if !self.nodes.contains_key(&id) {
            let node = ClusterNode::new(id, ip, port);
            self.nodes.insert(id, node);
        }
    }

    /// Remove a node from the cluster table.
    pub fn remove_node(&mut self, id: &[u8; NODE_ID_LEN]) -> bool {
        if *id == self.my_id {
            return false;
        }
        self.nodes.remove(id).is_some()
    }

    /// Get cluster info as a formatted string (for CLUSTER INFO).
    pub fn info_string(&self) -> String {
        let slots_assigned = self.slot_owners.iter().filter(|s| s.is_some()).count();
        let slots_ok = slots_assigned; // simplified
        let known_nodes = self.nodes.len();

        format!(
            "cluster_enabled:{}\r\n\
             cluster_state:{}\r\n\
             cluster_slots_assigned:{}\r\n\
             cluster_slots_ok:{}\r\n\
             cluster_slots_pfail:0\r\n\
             cluster_slots_fail:0\r\n\
             cluster_known_nodes:{}\r\n\
             cluster_size:{}\r\n\
             cluster_current_epoch:{}\r\n\
             cluster_my_epoch:{}\r\n\
             cluster_stats_messages_sent:0\r\n\
             cluster_stats_messages_received:0\r\n",
            if self.enabled { 1 } else { 0 },
            self.state,
            slots_assigned,
            slots_ok,
            known_nodes,
            self.size,
            self.current_epoch,
            self.config_epoch,
        )
    }

    /// Get nodes list as CLUSTER NODES format.
    pub fn nodes_string(&self) -> String {
        let mut result = String::new();
        for node in self.nodes.values() {
            let id_hex = node.id_hex();
            let addr = format!("{}:{}@{}", node.ip, node.port, node.bus_port);
            let flags = if node.flags == NodeFlags::Myself {
                "myself,master".to_owned()
            } else {
                format!("{}", node.flags)
            };
            let ping = node.ping_sent;
            let pong = node.pong_recv;
            let epoch = node.config_epoch;
            let link = if node.link_established { "connected" } else { "disconnected" };
            let slots = node.slot_ranges_string();
            let replica_of = node.replica_of.as_ref()
                .map(|id| format!(" {}", node_id_to_hex(id)))
                .unwrap_or_default();

            result.push_str(&format!(
                "{} {} {} {} {} {} {}{}{}\r\n",
                id_hex, addr, flags, ping, pong, epoch, link,
                if slots.is_empty() { String::new() } else { format!(" {}", slots) },
                replica_of,
            ));
        }
        result
    }

    /// Get slots mapping as CLUSTER SLOTS format (nested arrays).
    pub fn slots_array(&self) -> Vec<(usize, usize, String, u16, String)> {
        // Returns (start_slot, end_slot, ip, port, node_id_hex)
        let mut ranges = Vec::new();
        let mut start = None;
        for i in 0..CLUSTER_SLOTS {
            match (&start, &self.slot_owners[i]) {
                (Some(s), Some(owner)) if *s == i - 1 => {
                    // Continue range
                    start = Some(i);
                }
                (_, Some(owner)) => {
                    // Start new range - first close previous if any
                    if let Some(s) = start.take() {
                        // This is handled below
                    }
                    start = Some(i);
                }
                (Some(s), None) => {
                    // End of range
                    if let Some(owner_id) = self.slot_owners[i - 1] {
                        if let Some(node) = self.nodes.get(&owner_id) {
                            ranges.push((*s, i - 1, node.ip.clone(), node.port, node.id_hex()));
                        }
                    }
                    start = None;
                }
                (None, None) => {}
            }
        }
        // Close final range
        if let Some(s) = start {
            if let Some(owner_id) = self.slot_owners[CLUSTER_SLOTS - 1] {
                if let Some(node) = self.nodes.get(&owner_id) {
                    ranges.push((s, CLUSTER_SLOTS - 1, node.ip.clone(), node.port, node.id_hex()));
                }
            }
        }
        ranges
    }

    /// Count keys in a given slot (requires Database access, handled in command layer).
    pub fn count_keys_in_slot(&self, _slot: usize) -> usize {
        // This is a placeholder; actual implementation needs Database access
        0
    }
}

/// Generate a random 20-byte node ID.
pub fn generate_node_id() -> [u8; NODE_ID_LEN] {
    let mut id = [0u8; NODE_ID_LEN];
    for byte in &mut id {
        *byte = rand::random::<u8>();
    }
    id
}

/// Convert a 20-byte node ID to a 40-character hex string.
pub fn node_id_to_hex(id: &[u8; NODE_ID_LEN]) -> String {
    id.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Parse a 40-character hex string into a 20-byte node ID.
pub fn hex_to_node_id(hex: &str) -> Result<[u8; NODE_ID_LEN], String> {
    if hex.len() != 40 {
        return Err(format!("ERR Invalid node ID length: {} (expected 40)", hex.len()));
    }
    let mut id = [0u8; NODE_ID_LEN];
    for i in 0..NODE_ID_LEN {
        id[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|_| "ERR Invalid hex in node ID".to_owned())?;
    }
    Ok(id)
}

/// Convert slot bitmap to range string (e.g., "0-5460 10923-16383").
pub fn slot_ranges_to_string(slots: &[bool]) -> String {
    let mut ranges = Vec::new();
    let mut start = None;
    for (i, &owned) in slots.iter().enumerate() {
        match (start, owned) {
            (None, true) => start = Some(i),
            (Some(s), false) => {
                if s == i - 1 {
                    ranges.push(format!("{}", s));
                } else {
                    ranges.push(format!("{}-{}", s, i - 1));
                }
                start = None;
            }
            _ => {}
        }
    }
    // Close final range
    if let Some(s) = start {
        let last = slots.len() - 1;
        if s == last {
            ranges.push(format!("{}", s));
        } else {
            ranges.push(format!("{}-{}", s, last));
        }
    }
    ranges.join(" ")
}

/// Save cluster state to nodes.conf file.
pub fn save_cluster_config(state: &ClusterState, path: &str) -> Result<(), String> {
    use std::io::Write;
    let mut content = String::new();

    // Write vars
    content.push_str(&format!("vars currentEpoch {} configEpoch {} myId {} myIp {} myPort {} myBusPort {} role {} replicaOf {}\n",
        state.current_epoch,
        state.config_epoch,
        node_id_to_hex(&state.my_id),
        state.my_ip,
        state.my_port,
        state.my_bus_port,
        state.role,
        state.replica_of.as_ref().map(|id| node_id_to_hex(id)).unwrap_or_else(|| "none".to_owned()),
    ));

    // Write nodes
    for node in state.nodes.values() {
        let flags_str = match node.flags {
            NodeFlags::Myself => "myself,master",
            NodeFlags::Master => "master",
            NodeFlags::Replica => "slave",
            _ => "noflags",
        };
        let slots = node.slot_ranges_string();
        let replica_of = node.replica_of.as_ref()
            .map(|id| format!(" {}", node_id_to_hex(id)))
            .unwrap_or_default();
        content.push_str(&format!("node {} {} {} {} {} {} {}{}{}\n",
            node_id_to_hex(&node.id),
            node.ip,
            node.port,
            node.bus_port,
            flags_str,
            node.config_epoch,
            slots,
            if slots.is_empty() { String::new() } else { String::new() },
            replica_of,
        ));
    }

    std::fs::write(path, &content).map_err(|e| format!("ERR Failed to save cluster config: {}", e))
}

/// Load cluster state from nodes.conf file.
pub fn load_cluster_config(state: &mut ClusterState, path: &str) -> Result<(), String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("ERR Failed to load cluster config: {}", e))?;

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }

        match parts[0] {
            "vars" => {
                // Parse vars line
                let mut i = 1;
                while i + 1 < parts.len() {
                    match parts[i] {
                        "currentEpoch" => state.current_epoch = parts[i + 1].parse().unwrap_or(0),
                        "configEpoch" => state.config_epoch = parts[i + 1].parse().unwrap_or(0),
                        "myId" => {
                            if let Ok(id) = hex_to_node_id(parts[i + 1]) {
                                state.my_id = id;
                            }
                        }
                        "myIp" => state.my_ip = parts[i + 1].to_owned(),
                        "myPort" => state.my_port = parts[i + 1].parse().unwrap_or(0),
                        "myBusPort" => state.my_bus_port = parts[i + 1].parse().unwrap_or(0),
                        "role" => {
                            state.role = if parts[i + 1] == "master" { NodeRole::Master } else { NodeRole::Replica };
                        }
                        "replicaOf" => {
                            if parts[i + 1] != "none" {
                                if let Ok(id) = hex_to_node_id(parts[i + 1]) {
                                    state.replica_of = Some(id);
                                }
                            }
                        }
                        _ => {}
                    }
                    i += 2;
                }
            }
            "node" => {
                if parts.len() < 7 {
                    continue;
                }
                if let Ok(id) = hex_to_node_id(parts[1]) {
                    let ip = parts[2].to_owned();
                    let port: u16 = parts[3].parse().unwrap_or(0);
                    let bus_port: u16 = parts[4].parse().unwrap_or(port + 10000);
                    let flags_str = parts[5];
                    let epoch: u64 = parts[6].parse().unwrap_or(0);

                    let mut node = ClusterNode::new(id, ip, port);
                    node.bus_port = bus_port;
                    node.config_epoch = epoch;

                    // Parse flags
                    if flags_str.contains("myself") {
                        node.flags = NodeFlags::Myself;
                    } else if flags_str.contains("slave") {
                        node.flags = NodeFlags::Replica;
                    } else if flags_str.contains("master") {
                        node.flags = NodeFlags::Master;
                    }

                    // Parse slot ranges (remaining parts)
                    for part in &parts[7..] {
                        if part.contains('-') {
                            let range: Vec<&str> = part.split('-').collect();
                            if range.len() == 2 {
                                if let (Ok(start), Ok(end)) = (range[0].parse::<usize>(), range[1].parse::<usize>()) {
                                    for s in start..=end {
                                        if s < CLUSTER_SLOTS {
                                            node.slots[s] = true;
                                        }
                                    }
                                }
                            }
                        } else if let Ok(slot) = part.parse::<usize>() {
                            if slot < CLUSTER_SLOTS {
                                node.slots[slot] = true;
                            }
                        }
                    }

                    // Update slot owners if this is myself
                    if node.flags == NodeFlags::Myself {
                        for (i, &owned) in node.slots.iter().enumerate() {
                            if owned {
                                state.slot_owners[i] = Some(id);
                                state.my_slots[i] = true;
                            }
                        }
                    }

                    state.nodes.insert(id, node);
                }
            }
            _ => {}
        }
    }

    state.register_self();
    state.recalc_size();
    Ok(())
}

#[cfg(test)]
mod test_cluster {
    use super::*;

    #[test]
    fn test_node_id_roundtrip() {
        let id = generate_node_id();
        let hex = node_id_to_hex(&id);
        assert_eq!(hex.len(), 40);
        let parsed = hex_to_node_id(&hex).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_slot_ranges() {
        let mut slots = vec![false; CLUSTER_SLOTS];
        slots[0] = true;
        slots[1] = true;
        slots[2] = true;
        assert_eq!(slot_ranges_to_string(&slots), "0-2");

        slots[5460] = true;
        slots[5461] = true;
        assert_eq!(slot_ranges_to_string(&slots), "0-2 5460-5461");
    }

    #[test]
    fn test_cluster_state_add_del_slots() {
        let mut state = ClusterState::new(true, "127.0.0.1".to_owned(), 6379, "nodes.conf".to_owned(), 15000);
        state.register_self();

        assert!(state.add_slot(0).is_ok());
        assert!(state.add_slot(1).is_ok());
        assert!(state.add_slot(5460).is_ok());
        assert_eq!(state.my_slots.iter().filter(|&&s| s).count(), 3);

        // Can't add already busy slot
        assert!(state.add_slot(0).is_err());

        // Delete
        assert!(state.del_slot(1).is_ok());
        assert_eq!(state.my_slots.iter().filter(|&&s| s).count(), 2);
    }

    #[test]
    fn test_cluster_info() {
        let state = ClusterState::new(true, "127.0.0.1".to_owned(), 6379, "nodes.conf".to_owned(), 15000);
        let info = state.info_string();
        assert!(info.contains("cluster_enabled:1"));
        assert!(info.contains("cluster_state:"));
    }

    #[test]
    fn test_cluster_reset() {
        let mut state = ClusterState::new(true, "127.0.0.1".to_owned(), 6379, "nodes.conf".to_owned(), 15000);
        state.register_self();
        state.add_slot(0).unwrap();
        assert!(state.my_slots[0]);

        // Soft reset fails with slots
        assert!(state.reset(false).is_err());

        // Hard reset succeeds
        assert!(state.reset(true).is_ok());
        assert!(!state.my_slots.iter().any(|&s| s));
    }

    #[test]
    fn test_save_load_config() {
        let mut state = ClusterState::new(true, "127.0.0.1".to_owned(), 6379, "nodes.conf".to_owned(), 15000);
        state.register_self();
        state.add_slot(0).unwrap();
        state.add_slot(1).unwrap();
        state.current_epoch = 5;

        let path = "/tmp/test_cluster_nodes.conf";
        assert!(save_cluster_config(&state, path).is_ok());

        let mut loaded = ClusterState::new(true, "127.0.0.1".to_owned(), 6379, "nodes.conf".to_owned(), 15000);
        assert!(load_cluster_config(&mut loaded, path).is_ok());
        assert_eq!(loaded.current_epoch, 5);

        // Cleanup
        let _ = std::fs::remove_file(path);
    }
}
