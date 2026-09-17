//! Cluster bus: TCP listener on port+10000 for inter-node Gossip communication.
//!
//! Implements a simplified Redis Cluster bus protocol using RESP arrays:
//! - PING/PONG: carry sender info, slot bitmap, known-node gossip sections
//! - MEET: introduce a new node to the cluster
//! - FAIL: mark a node as failed

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use database::cluster::{
    self, ClusterNode, ClusterState, NodeFlags, SlotState,
    CLUSTER_SLOTS, NODE_ID_LEN,
};
use database::shard::ShardedDatabase;
use logger::{log, sendlog, Level, Logger};

/// Cluster bus message types.
#[derive(Debug, Clone)]
pub enum ClusterMsg {
    /// PING: heartbeat with sender info and gossip data.
    Ping {
        sender_id: [u8; NODE_ID_LEN],
        ip: String,
        port: u16,
        bus_port: u16,
        config_epoch: u64,
        slots: Vec<bool>,
        gossip: Vec<GossipEntry>,
    },
    /// PONG: reply to PING (same payload structure).
    Pong {
        sender_id: [u8; NODE_ID_LEN],
        ip: String,
        port: u16,
        bus_port: u16,
        config_epoch: u64,
        slots: Vec<bool>,
        gossip: Vec<GossipEntry>,
    },
    /// MEET: request to join the cluster.
    Meet {
        sender_id: [u8; NODE_ID_LEN],
        ip: String,
        port: u16,
        bus_port: u16,
    },
    /// FAIL: declare a node as failed.
    Fail {
        target_id: [u8; NODE_ID_LEN],
    },
}

/// A gossip section entry: information about one known node.
#[derive(Debug, Clone)]
pub struct GossipEntry {
    pub node_id: [u8; NODE_ID_LEN],
    pub ip: String,
    pub port: u16,
    pub bus_port: u16,
    pub flags: u16,
    pub config_epoch: u64,
}

/// Encode a ClusterMsg into RESP array format.
pub fn encode_msg(msg: &ClusterMsg) -> Vec<u8> {
    let mut buf = Vec::with_capacity(512);
    match msg {
        ClusterMsg::Ping { sender_id, ip, port, bus_port, config_epoch, slots, gossip } |
        ClusterMsg::Pong { sender_id, ip, port, bus_port, config_epoch, slots, gossip } => {
            let cmd = match msg {
                ClusterMsg::Ping { .. } => "PING",
                _ => "PONG",
            };
            let id_hex = cluster::node_id_to_hex(sender_id);
            let slot_str = encode_slots_compact(slots);
            let gossip_str = encode_gossip(gossip);
            // *8\r\n$cmd\r\n$id_hex\r\n$ip\r\n:port\r\n:bus_port\r\n:epoch\r\n$slots\r\n$gossip\r\n
            write_resp_array(&mut buf, &[
                cmd,
                &id_hex,
                ip,
                &port.to_string(),
                &bus_port.to_string(),
                &config_epoch.to_string(),
                &slot_str,
                &gossip_str,
            ]);
        }
        ClusterMsg::Meet { sender_id, ip, port, bus_port } => {
            let id_hex = cluster::node_id_to_hex(sender_id);
            write_resp_array(&mut buf, &[
                "MEET",
                &id_hex,
                ip,
                &port.to_string(),
                &bus_port.to_string(),
            ]);
        }
        ClusterMsg::Fail { target_id } => {
            let id_hex = cluster::node_id_to_hex(target_id);
            write_resp_array(&mut buf, &[
                "FAIL",
                &id_hex,
            ]);
        }
    }
    buf
}

/// Decode a RESP array into a ClusterMsg.
pub fn decode_msg(parts: &[String]) -> Result<ClusterMsg, String> {
    if parts.is_empty() {
        return Err("empty message".to_owned());
    }
    match parts[0].to_ascii_uppercase().as_str() {
        "PING" | "PONG" => {
            if parts.len() < 8 {
                return Err(format!("{} message requires 8 args, got {}", parts[0], parts.len()));
            }
            let sender_id = cluster::hex_to_node_id(&parts[1])?;
            let ip = parts[2].clone();
            let port: u16 = parts[3].parse().map_err(|_| "invalid port".to_owned())?;
            let bus_port: u16 = parts[4].parse().map_err(|_| "invalid bus_port".to_owned())?;
            let config_epoch: u64 = parts[5].parse().map_err(|_| "invalid epoch".to_owned())?;
            let slots = decode_slots_compact(&parts[6]);
            let gossip = decode_gossip(&parts[7]);
            if parts[0].to_ascii_uppercase() == "PING" {
                Ok(ClusterMsg::Ping { sender_id, ip, port, bus_port, config_epoch, slots, gossip })
            } else {
                Ok(ClusterMsg::Pong { sender_id, ip, port, bus_port, config_epoch, slots, gossip })
            }
        }
        "MEET" => {
            if parts.len() < 5 {
                return Err("MEET message requires 5 args".to_owned());
            }
            let sender_id = cluster::hex_to_node_id(&parts[1])?;
            let ip = parts[2].clone();
            let port: u16 = parts[3].parse().map_err(|_| "invalid port".to_owned())?;
            let bus_port: u16 = parts[4].parse().map_err(|_| "invalid bus_port".to_owned())?;
            Ok(ClusterMsg::Meet { sender_id, ip, port, bus_port })
        }
        "FAIL" => {
            if parts.len() < 2 {
                return Err("FAIL message requires 2 args".to_owned());
            }
            let target_id = cluster::hex_to_node_id(&parts[1])?;
            Ok(ClusterMsg::Fail { target_id })
        }
        other => Err(format!("unknown cluster message type: {}", other)),
    }
}

/// Process an incoming cluster message, updating the cluster state.
pub fn process_msg(msg: ClusterMsg, state: &mut ClusterState) {
    let now = util::mstime();
    match msg {
        ClusterMsg::Ping { sender_id, ip, port, bus_port, config_epoch, slots, gossip } |
        ClusterMsg::Pong { sender_id, ip, port, bus_port, config_epoch, slots, gossip } => {
            // Update or add the sender node
            if let Some(node) = state.nodes.get_mut(&sender_id) {
                node.pong_recv = now;
                node.link_established = true;
                node.config_epoch = config_epoch;
                node.slots = slots.clone();
            } else {
                let mut node = ClusterNode::new(sender_id, ip.clone(), port);
                node.bus_port = bus_port;
                node.config_epoch = config_epoch;
                node.slots = slots.clone();
                node.pong_recv = now;
                node.link_established = true;
                state.nodes.insert(sender_id, node);
            }

            // Update slot owners based on the sender's slot bitmap
            // Only trust the node if its config_epoch is >= ours
            if config_epoch >= state.config_epoch {
                for (i, &owned) in slots.iter().enumerate() {
                    if owned && i < CLUSTER_SLOTS {
                        state.slot_owners[i] = Some(sender_id);
                    }
                }
            }

            // Process gossip entries: learn about new nodes
            for entry in &gossip {
                if !state.nodes.contains_key(&entry.node_id) && entry.node_id != state.my_id {
                    let mut new_node = ClusterNode::new(entry.node_id, entry.ip.clone(), entry.port);
                    new_node.bus_port = entry.bus_port;
                    new_node.config_epoch = entry.config_epoch;
                    state.nodes.insert(entry.node_id, new_node);
                }
            }

            state.recalc_size();
        }
        ClusterMsg::Meet { sender_id, ip, port, bus_port } => {
            // Add the meeting node
            let mut node = ClusterNode::new(sender_id, ip.clone(), port);
            node.bus_port = bus_port;
            node.pong_recv = now;
            node.link_established = true;
            state.nodes.insert(sender_id, node);
            state.recalc_size();
        }
        ClusterMsg::Fail { target_id } => {
            // Mark the target node as failed
            if let Some(node) = state.nodes.get_mut(&target_id) {
                node.flags = NodeFlags::Fail;
            }
        }
    }
}

/// Build a PING/PONG message from current cluster state.
pub fn build_ping_msg(state: &ClusterState, is_pong: bool) -> ClusterMsg {
    let gossip: Vec<GossipEntry> = state.nodes.values()
        .take(3) // Send gossip about up to 3 random nodes
        .filter(|n| n.id != state.my_id)
        .map(|n| GossipEntry {
            node_id: n.id,
            ip: n.ip.clone(),
            port: n.port,
            bus_port: n.bus_port,
            flags: match n.flags {
                NodeFlags::Myself => 0x0008,
                NodeFlags::Master => 0x0004,
                NodeFlags::Replica => 0x0002,
                NodeFlags::Pfail => 0x0010,
                NodeFlags::Fail => 0x0020,
                _ => 0,
            },
            config_epoch: n.config_epoch,
        })
        .collect();

    if is_pong {
        ClusterMsg::Pong {
            sender_id: state.my_id,
            ip: state.my_ip.clone(),
            port: state.my_port,
            bus_port: state.my_bus_port,
            config_epoch: state.config_epoch,
            slots: state.my_slots.clone(),
            gossip,
        }
    } else {
        ClusterMsg::Ping {
            sender_id: state.my_id,
            ip: state.my_ip.clone(),
            port: state.my_port,
            bus_port: state.my_bus_port,
            config_epoch: state.config_epoch,
            slots: state.my_slots.clone(),
            gossip,
        }
    }
}

/// Send a cluster message to a specific address.
pub fn send_msg(addr: &str, msg: &ClusterMsg) -> Result<(), String> {
    let mut stream = TcpStream::connect(addr)
        .map_err(|e| format!("failed to connect to {}: {}", addr, e))?;
    stream.set_write_timeout(Some(Duration::from_secs(5))).ok();
    let data = encode_msg(msg);
    stream.write_all(&data)
        .map_err(|e| format!("failed to write to {}: {}", addr, e))?;
    Ok(())
}

/// Read a RESP message from a TCP stream and return the parsed parts.
pub fn read_msg(stream: &mut TcpStream) -> Result<Vec<String>, String> {
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
    let mut buf = vec![0u8; 4096];
    let mut total = Vec::new();
    loop {
        match stream.read(&mut buf) {
            Ok(0) => return Err("connection closed".to_owned()),
            Ok(n) => {
                total.extend_from_slice(&buf[..n]);
                // Check if we have a complete RESP message
                if let Some(parts) = try_parse_resp(&total) {
                    return Ok(parts);
                }
                if total.len() > 65536 {
                    return Err("message too large".to_owned());
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if !total.is_empty() {
                    if let Some(parts) = try_parse_resp(&total) {
                        return Ok(parts);
                    }
                }
                return Err("read timeout".to_owned());
            }
            Err(e) => return Err(format!("read error: {}", e)),
        }
    }
}

/// Try to parse a RESP array from a buffer. Returns None if incomplete.
fn try_parse_resp(buf: &[u8]) -> Option<Vec<String>> {
    let s = String::from_utf8_lossy(buf);
    if !s.starts_with('*') {
        // Try as inline command
        if s.contains("\r\n") {
            let line = s.split("\r\n").next()?;
            let parts: Vec<String> = line.split_whitespace().map(|s| s.to_owned()).collect();
            if !parts.is_empty() {
                return Some(parts);
            }
        }
        return None;
    }
    // Parse RESP array
    let end_of_first_line = s.find("\r\n")?;
    let count_str = &s[1..end_of_first_line];
    let count: usize = count_str.parse().ok()?;
    let mut pos = end_of_first_line + 2;
    let mut parts = Vec::with_capacity(count);
    for _ in 0..count {
        if pos >= s.len() {
            return None; // incomplete
        }
        if s.as_bytes()[pos] == b'$' {
            let line_end = s[pos..].find("\r\n")? + pos;
            let len_str = &s[pos + 1..line_end];
            let len: usize = len_str.parse().ok()?;
            let data_start = line_end + 2;
            let data_end = data_start + len;
            if data_end + 2 > s.len() {
                return None; // incomplete
            }
            parts.push(s[data_start..data_end].to_owned());
            pos = data_end + 2;
        } else if s.as_bytes()[pos] == b':' {
            let line_end = s[pos..].find("\r\n")? + pos;
            parts.push(s[pos + 1..line_end].to_owned());
            pos = line_end + 2;
        } else {
            // Inline or simple string
            let line_end = s[pos..].find("\r\n")? + pos;
            parts.push(s[pos..line_end].to_owned());
            pos = line_end + 2;
        }
    }
    if parts.len() == count {
        Some(parts)
    } else {
        None
    }
}

/// Encode slot bitmap as a compact hex string (16384 bits = 2048 bytes = 4096 hex chars).
fn encode_slots_compact(slots: &[bool]) -> String {
    let mut bytes = vec![0u8; (CLUSTER_SLOTS + 7) / 8];
    for (i, &owned) in slots.iter().enumerate() {
        if owned {
            bytes[i / 8] |= 1 << (i % 8);
        }
    }
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Decode slot bitmap from compact hex string.
fn decode_slots_compact(hex: &str) -> Vec<bool> {
    let mut slots = vec![false; CLUSTER_SLOTS];
    let bytes: Vec<u8> = (0..hex.len())
        .step_by(2)
        .filter_map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
        .collect();
    for (byte_idx, &byte) in bytes.iter().enumerate() {
        for bit in 0..8 {
            let slot = byte_idx * 8 + bit;
            if slot < CLUSTER_SLOTS && byte & (1 << bit) != 0 {
                slots[slot] = true;
            }
        }
    }
    slots
}

/// Encode gossip entries as a semicolon-separated string.
fn encode_gossip(entries: &[GossipEntry]) -> String {
    entries.iter().map(|e| {
        format!(
            "{},{},{},{},{},{}",
            cluster::node_id_to_hex(&e.node_id),
            e.ip,
            e.port,
            e.bus_port,
            e.flags,
            e.config_epoch,
        )
    }).collect::<Vec<_>>().join(";")
}

/// Decode gossip entries from semicolon-separated string.
fn decode_gossip(s: &str) -> Vec<GossipEntry> {
    if s.is_empty() {
        return Vec::new();
    }
    s.split(';').filter_map(|entry| {
        let parts: Vec<&str> = entry.split(',').collect();
        if parts.len() >= 6 {
            let node_id = cluster::hex_to_node_id(parts[0]).ok()?;
            let ip = parts[1].to_owned();
            let port: u16 = parts[2].parse().ok()?;
            let bus_port: u16 = parts[3].parse().ok()?;
            let flags: u16 = parts[4].parse().ok()?;
            let config_epoch: u64 = parts[5].parse().ok()?;
            Some(GossipEntry { node_id, ip, port, bus_port, flags, config_epoch })
        } else {
            None
        }
    }).collect()
}

/// Write a RESP array of bulk strings.
fn write_resp_array(buf: &mut Vec<u8>, parts: &[&str]) {
    buf.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for part in parts {
        buf.extend_from_slice(format!("${}\r\n{}\r\n", part.len(), part).as_bytes());
    }
}

/// Start the cluster bus listener on the given port.
/// Returns a handle to the listener thread and a stop flag.
pub fn start_cluster_bus(
    bus_port: u16,
    db: Arc<ShardedDatabase>,
    logger: Logger,
) -> (thread::JoinHandle<()>, Arc<AtomicBool>) {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = stop.clone();

    let handle = thread::spawn(move || {
        let addr = format!("0.0.0.0:{}", bus_port);
        let listener = match TcpListener::bind(&addr) {
            Ok(l) => {
                log!(logger, Notice, "Cluster bus listening on port {}", bus_port);
                l
            }
            Err(e) => {
                log!(logger, Warning, "Failed to bind cluster bus on port {}: {}", bus_port, e);
                return;
            }
        };
        listener.set_nonblocking(false).ok();

        // Accept loop
        while !stop.load(Ordering::Relaxed) {
            listener.set_nonblocking(true).ok();
            match listener.accept() {
                Ok((mut stream, addr)) => {
                    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
                    stream.set_write_timeout(Some(Duration::from_secs(5))).ok();
                    let db = db.clone();
                    let logger = logger.clone();
                    thread::spawn(move || {
                        handle_bus_connection(&mut stream, &db, &logger);
                    });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(100));
                }
                Err(e) => {
                    if !stop.load(Ordering::Relaxed) {
                        log!(logger, Warning, "Cluster bus accept error: {}", e);
                    }
                }
            }
        }
    });

    (handle, stop_clone)
}

/// Handle a single incoming cluster bus connection.
fn handle_bus_connection(stream: &mut TcpStream, db: &ShardedDatabase, logger: &Logger) {
    match read_msg(stream) {
        Ok(parts) => {
            match decode_msg(&parts) {
                Ok(msg) => {
                    // Process the message on shard 0's database
                    if let Ok(mut shard) = db.shard_write(0, 0) {
                        let is_ping = matches!(&msg, ClusterMsg::Ping { .. } | ClusterMsg::Meet { .. });
                        process_msg(msg, &mut shard.db.cluster);

                        // Reply with PONG for PING/MEET
                        if is_ping {
                            let pong = build_ping_msg(&shard.db.cluster, true);
                            let data = encode_msg(&pong);
                            let _ = stream.write_all(&data);
                            let _ = stream.flush();
                        }
                    }
                }
                Err(e) => {
                    log!(logger, Verbose, "Failed to decode cluster bus message: {}", e);
                }
            }
        }
        Err(_) => {
            // Ignore read errors (timeouts, etc.)
        }
    }
}

/// Start the gossip timer thread that periodically sends PINGs to random nodes.
pub fn start_gossip_timer(
    db: Arc<ShardedDatabase>,
    logger: Logger,
    interval_ms: u64,
) -> (thread::JoinHandle<()>, Arc<AtomicBool>) {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = stop.clone();

    let handle = thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(interval_ms));

            if let Ok(mut shard) = db.shard_write(0, 0) {
                if !shard.db.cluster.enabled {
                    continue;
                }

                // Pick a random node to PING
                let candidates: Vec<(String, u16)> = shard.db.cluster.nodes.values()
                    .filter(|n| n.id != shard.db.cluster.my_id && n.link_established)
                    .map(|n| (format!("{}:{}", n.ip, n.bus_port), n.port))
                    .collect();

                if candidates.is_empty() {
                    continue;
                }

                let idx = rand::random::<usize>() % candidates.len();
                let (addr, _) = candidates[idx].clone();

                let ping = build_ping_msg(&shard.db.cluster, false);
                let data = encode_msg(&ping);

                // Send in a separate thread to avoid blocking
                let logger = logger.clone();
                thread::spawn(move || {
                    if let Ok(mut stream) = TcpStream::connect(&addr) {
                        stream.set_write_timeout(Some(Duration::from_secs(2))).ok();
                        if let Err(e) = stream.write_all(&data) {
                            log!(logger, Verbose, "Gossip PING failed to {}: {}", addr, e);
                        }
                    }
                });

                // Update cluster health
                shard.db.cluster.check_health();
            }
        }
    });

    (handle, stop_clone)
}

#[cfg(test)]
mod test_cluster_bus {
    use super::*;

    #[test]
    fn test_encode_decode_ping() {
        let mut slots = vec![false; CLUSTER_SLOTS];
        slots[0] = true;
        slots[1] = true;
        let msg = ClusterMsg::Ping {
            sender_id: [0xAB; NODE_ID_LEN],
            ip: "127.0.0.1".to_owned(),
            port: 6379,
            bus_port: 16379,
            config_epoch: 1,
            slots,
            gossip: vec![],
        };
        let encoded = encode_msg(&msg);
        let encoded_str = String::from_utf8_lossy(&encoded);
        assert!(encoded_str.starts_with("*8\r\n"));
        assert!(encoded_str.contains("PING"));
    }

    #[test]
    fn test_encode_decode_meet() {
        let msg = ClusterMsg::Meet {
            sender_id: [0xCD; NODE_ID_LEN],
            ip: "192.168.1.1".to_owned(),
            port: 6380,
            bus_port: 16380,
        };
        let encoded = encode_msg(&msg);
        let parts = try_parse_resp(&encoded).unwrap();
        let decoded = decode_msg(&parts).unwrap();
        match decoded {
            ClusterMsg::Meet { sender_id, ip, port, bus_port } => {
                assert_eq!(sender_id, [0xCD; NODE_ID_LEN]);
                assert_eq!(ip, "192.168.1.1");
                assert_eq!(port, 6380);
                assert_eq!(bus_port, 16380);
            }
            _ => panic!("expected Meet"),
        }
    }

    #[test]
    fn test_encode_decode_fail() {
        let msg = ClusterMsg::Fail {
            target_id: [0xEF; NODE_ID_LEN],
        };
        let encoded = encode_msg(&msg);
        let parts = try_parse_resp(&encoded).unwrap();
        let decoded = decode_msg(&parts).unwrap();
        match decoded {
            ClusterMsg::Fail { target_id } => {
                assert_eq!(target_id, [0xEF; NODE_ID_LEN]);
            }
            _ => panic!("expected Fail"),
        }
    }

    #[test]
    fn test_slots_compact_roundtrip() {
        let mut slots = vec![false; CLUSTER_SLOTS];
        slots[0] = true;
        slots[100] = true;
        slots[5460] = true;
        slots[16383] = true;
        let encoded = encode_slots_compact(&slots);
        let decoded = decode_slots_compact(&encoded);
        assert_eq!(decoded[0], true);
        assert_eq!(decoded[100], true);
        assert_eq!(decoded[5460], true);
        assert_eq!(decoded[16383], true);
        assert_eq!(decoded[1], false);
        assert_eq!(decoded[5461], false);
    }

    #[test]
    fn test_gossip_roundtrip() {
        let entries = vec![
            GossipEntry {
                node_id: [0x11; NODE_ID_LEN],
                ip: "10.0.0.1".to_owned(),
                port: 6379,
                bus_port: 16379,
                flags: 4,
                config_epoch: 2,
            },
            GossipEntry {
                node_id: [0x22; NODE_ID_LEN],
                ip: "10.0.0.2".to_owned(),
                port: 6380,
                bus_port: 16380,
                flags: 0,
                config_epoch: 1,
            },
        ];
        let encoded = encode_gossip(&entries);
        let decoded = decode_gossip(&encoded);
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].ip, "10.0.0.1");
        assert_eq!(decoded[1].ip, "10.0.0.2");
    }

    #[test]
    fn test_process_ping() {
        let mut state = ClusterState::new(true, "127.0.0.1".to_owned(), 6379, "nodes.conf".to_owned(), 15000);
        state.register_self();

        let sender_id = cluster::generate_node_id();
        let mut slots = vec![false; CLUSTER_SLOTS];
        slots[100] = true;

        let msg = ClusterMsg::Ping {
            sender_id,
            ip: "10.0.0.1".to_owned(),
            port: 6380,
            bus_port: 16380,
            config_epoch: 1,
            slots: slots.clone(),
            gossip: vec![],
        };
        process_msg(msg, &mut state);

        // Node should be added
        assert!(state.nodes.contains_key(&sender_id));
        let node = state.nodes.get(&sender_id).unwrap();
        assert_eq!(node.ip, "10.0.0.1");
        assert_eq!(node.port, 6380);
        assert!(node.link_established);
    }

    #[test]
    fn test_build_ping_msg() {
        let mut state = ClusterState::new(true, "127.0.0.1".to_owned(), 6379, "nodes.conf".to_owned(), 15000);
        state.register_self();
        state.add_slot(0).unwrap();

        let ping = build_ping_msg(&state, false);
        match ping {
            ClusterMsg::Ping { sender_id, ip, port, .. } => {
                assert_eq!(sender_id, state.my_id);
                assert_eq!(ip, "127.0.0.1");
                assert_eq!(port, 6379);
            }
            _ => panic!("expected Ping"),
        }
    }
}
