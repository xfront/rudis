//! AOF rewrite: regenerate AOF files from the current in-memory state.
//!
//! Iterates all keys in each shard and writes the equivalent RESP commands
//! to a new AOF file, then atomically replaces the old one.

use std::fs;
use std::io::{self, Write};

use crate::shard::ShardedDatabase;
use crate::Value;

/// Write a RESP bulk string (e.g. `$5\r\nhello\r\n`).
fn write_bulk_string(buf: &mut Vec<u8>, data: &[u8]) {
    write!(buf, "${}\r\n", data.len()).unwrap();
    buf.extend_from_slice(data);
    buf.extend_from_slice(b"\r\n");
}

/// Write a RESP array of bulk strings as a complete command.
fn write_command(buf: &mut Vec<u8>, args: &[&[u8]]) {
    write!(buf, "*{}\r\n", args.len()).unwrap();
    for arg in args {
        write_bulk_string(buf, arg);
    }
}

/// Convert a single Value into the RESP commands needed to recreate it.
/// Returns None for types that cannot be represented as commands (probabilistic
/// structures like BloomFilter, CuckooFilter, TDigest, TopK).
fn value_to_resp_commands(key: &[u8], value: &Value) -> Vec<Vec<u8>> {
    let mut cmds = Vec::new();

    match value {
        Value::Nil => {}

        Value::String(s) => {
            let val = s.to_vec();
            let mut buf = Vec::new();
            write_command(&mut buf, &[b"SET", key, &val]);
            cmds.push(buf);
        }

        Value::List(list) => {
            let members = list.lrange(0, -1);
            if members.is_empty() {
                return cmds;
            }
            let mut args: Vec<&[u8]> = vec![b"RPUSH", key];
            for m in &members {
                args.push(m);
            }
            let mut buf = Vec::new();
            write_command(&mut buf, &args);
            cmds.push(buf);
        }

        Value::Set(set) => {
            let members = set.smembers();
            if members.is_empty() {
                return cmds;
            }
            let mut args: Vec<&[u8]> = vec![b"SADD", key];
            for m in &members {
                args.push(m);
            }
            let mut buf = Vec::new();
            write_command(&mut buf, &args);
            cmds.push(buf);
        }

        Value::Hash(hash) => {
            let fields = hash.hgetall();
            if fields.is_empty() {
                return cmds;
            }
            let mut args: Vec<&[u8]> = vec![b"HSET", key];
            for (f, v) in &fields {
                args.push(f);
                args.push(v);
            }
            let mut buf = Vec::new();
            write_command(&mut buf, &args);
            cmds.push(buf);
        }

        Value::SortedSet(zset) => {
            // Use the HashMap (member -> score) for iteration.
            match zset {
                crate::zset::ValueSortedSet::Data(_, hmap) => {
                    if hmap.is_empty() {
                        return cmds;
                    }
                    let mut args: Vec<&[u8]> = vec![b"ZADD", key];
                    // We need to collect and sort for deterministic output.
                    let mut items: Vec<(&Vec<u8>, &f64)> = hmap.iter().collect();
                    items.sort_by(|a, b| a.0.cmp(b.0));
                    let score_strings: Vec<(Vec<u8>,)> = items
                        .iter()
                        .map(|(_, &score)| (format_score(score),))
                        .collect();
                    for (i, (member, _)) in items.iter().enumerate() {
                        args.push(&score_strings[i].0);
                        args.push(member);
                    }
                    let mut buf = Vec::new();
                    write_command(&mut buf, &args);
                    cmds.push(buf);
                }
            }
        }

        Value::Stream(stream) => {
            // Emit XADD for each entry.
            for entry in stream.all_entries() {
                let id_str = entry.id.to_bytes();
                let mut args: Vec<&[u8]> = vec![b"XADD", key, &id_str];
                for (f, v) in &entry.fields {
                    args.push(f);
                    args.push(v);
                }
                let mut buf = Vec::new();
                write_command(&mut buf, &args);
                cmds.push(buf);
            }
            // Emit XGROUP CREATE for each consumer group.
            for (name, group) in stream.all_groups() {
                let last_id_str = group.last_id.to_bytes();
                let mut buf = Vec::new();
                write_command(
                    &mut buf,
                    &[b"XGROUP", b"CREATE", key, name, &last_id_str],
                );
                cmds.push(buf);
            }
        }

        Value::Json(json) => {
            let json_str = serde_json::to_string(&json.value).unwrap_or_default();
            let mut buf = Vec::new();
            write_command(
                &mut buf,
                &[b"JSON.SET", key, b".", json_str.as_bytes()],
            );
            cmds.push(buf);
        }

        Value::TimeSeries(ts) => {
            // TS.CREATE with retention and labels - build directly into buffer
            // to avoid lifetime issues with local String temporaries.
            {
                let mut parts: Vec<Vec<u8>> = vec![b"TS.CREATE".to_vec(), key.to_vec()];
                if ts.retention_ms > 0 {
                    parts.push(b"RETENTION".to_vec());
                    parts.push(ts.retention_ms.to_string().into_bytes());
                }
                if !ts.labels.is_empty() {
                    parts.push(b"LABELS".to_vec());
                    for (k, v) in &ts.labels {
                        parts.push(k.as_bytes().to_vec());
                        parts.push(v.as_bytes().to_vec());
                    }
                }
                let mut buf = Vec::new();
                write!(buf, "*{}\r\n", parts.len()).unwrap();
                for p in &parts {
                    write!(buf, "${}\r\n", p.len()).unwrap();
                    buf.extend_from_slice(p);
                    buf.extend_from_slice(b"\r\n");
                }
                cmds.push(buf);
            }
            // TS.ADD for each sample.
            for (&timestamp, &value) in &ts.samples {
                let ts_str = timestamp.to_string().into_bytes();
                let val_str = format_float(value);
                let mut buf = Vec::new();
                write_command(
                    &mut buf,
                    &[b"TS.ADD", key, &ts_str, &val_str],
                );
                cmds.push(buf);
            }
        }

        // Probabilistic structures: cannot be perfectly reconstructed from
        // commands. Skip with a warning logged by the caller.
        Value::BloomFilter(_)
        | Value::CuckooFilter(_)
        | Value::TDigest(_)
        | Value::TopK(_) => {}
    }

    cmds
}

/// Format a score for ZADD (matching Redis conventions).
fn format_score(score: f64) -> Vec<u8> {
    if score == f64::INFINITY {
        b"inf".to_vec()
    } else if score == f64::NEG_INFINITY {
        b"-inf".to_vec()
    } else if score == score.floor() && score.abs() < 1e15 {
        format!("{}", score as i64).into_bytes()
    } else {
        format!("{}", score).into_bytes()
    }
}

/// Format a float for TS.ADD (matching Redis conventions).
fn format_float(value: f64) -> Vec<u8> {
    if value == value.floor() && value.abs() < 1e15 {
        format!("{}", value as i64).into_bytes()
    } else {
        format!("{}", value).into_bytes()
    }
}

/// Rewrite the AOF file for a single shard.
///
/// Collects all non-expired keys from the shard's database, generates the
/// equivalent RESP commands, writes them to a temporary file, and atomically
/// replaces the old AOF file.
///
/// Returns the number of keys written.
pub fn rewrite_aof_for_shard(
    db: &ShardedDatabase,
    shard_idx: usize,
    aof_path: &str,
) -> io::Result<usize> {
    let mut all_cmds: Vec<Vec<u8>> = Vec::new();
    let mut key_count = 0usize;
    let mut skipped = 0usize;

    // Lock only the specific shard.
    let shard = db.shard_write(0, shard_idx).map_err(|e| {
        io::Error::new(io::ErrorKind::Other, format!("shard lock failed: {:?}", e))
    })?;
    let database = &shard.db;
    let db_idx = 0;

    for key in database.data_keys(db_idx) {
        if let Some(value) = database.get(db_idx, key) {
            let cmds = value_to_resp_commands(key, value);
            if cmds.is_empty() && !value.is_nil() {
                skipped += 1;
            }
            if !cmds.is_empty() {
                key_count += 1;
                all_cmds.extend(cmds);
            }

            // If the key has an expiration, emit PEXPIRE.
            if let Some(&exp_ms) = database.get_msexpiration(db_idx, key) {
                let ttl = exp_ms - util::mstime();
                if ttl > 0 {
                    let ttl_str = ttl.to_string();
                    let mut buf = Vec::new();
                    write_command(
                        &mut buf,
                        &[b"PEXPIRE", key, ttl_str.as_bytes()],
                    );
                    all_cmds.push(buf);
                }
            }
        }
    }
    drop(shard); // release lock before file I/O

    if skipped > 0 {
        eprintln!(
            "AOF rewrite: skipped {} probabilistic keys (BF/CF/TDIGEST/TOPK) on shard {}",
            skipped, shard_idx
        );
    }

    // Write to temp file then atomically rename.
    let tmp_path = format!("{}.tmp", aof_path);
    let mut fp = fs::File::create(&tmp_path)?;
    for cmd in &all_cmds {
        fp.write_all(cmd)?;
    }
    fp.flush()?;
    fs::rename(&tmp_path, aof_path)?;

    Ok(key_count)
}

/// Rewrite AOF files for all shards.
///
/// Returns a vector of (shard_index, keys_written) pairs.
pub fn rewrite_all_aofs(
    db: &ShardedDatabase,
    base_path: &str,
    num_shards: usize,
) -> io::Result<Vec<(usize, usize)>> {
    let mut results = Vec::new();

    for shard_idx in 0..num_shards {
        let aof_path = if num_shards > 1 {
            format!("{}.{}", base_path, shard_idx)
        } else {
            base_path.to_owned()
        };
        match rewrite_aof_for_shard(db, shard_idx, &aof_path) {
            Ok(n) => results.push((shard_idx, n)),
            Err(e) => {
                eprintln!(
                    "AOF rewrite failed for shard {}: {}",
                    shard_idx, e
                );
                return Err(e);
            }
        }
    }

    Ok(results)
}
