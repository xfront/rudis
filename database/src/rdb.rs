//! RDB snapshot persistence using bincode serialization.
//!
//! Saves and loads the complete database state (all keys, values, and
//! expirations) to/from a single file.

use std::fs;
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::Mutex;

use crate::shard::ShardedDatabase;
use crate::Value;

/// Magic bytes to identify a rudis RDB file.
const RDB_MAGIC: &[u8; 4] = b"RDB1";

/// Serializable representation of one key-value pair with optional expiration.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct RdbEntry {
    /// Logical database index (0..databases-1)
    pub db_idx: u8,
    pub key: Vec<u8>,
    pub value: Value,
    /// Expiration in milliseconds since Unix epoch, or None for no expiration.
    pub expire_at_ms: Option<i64>,
}

/// Serializable representation of the entire database state.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct RdbSnapshot {
    pub entries: Vec<RdbEntry>,
}

/// Save all data from a ShardedDatabase to an RDB file.
///
/// Iterates all shards, collects every non-expired key-value pair, and
/// writes them as a single bincode-encoded blob.
pub fn save_to_file(db: &ShardedDatabase, path: &str) -> io::Result<usize> {
    let entries = Mutex::new(Vec::new());

    // Get the number of logical databases from the first shard's config
    let num_logical_dbs = {
        let shard = db.shard_write(0, 0).unwrap();
        shard.db.config.databases as usize
    };

    // Iterate all logical databases (0..num_logical_dbs)
    for db_idx in 0..num_logical_dbs {
        // For each logical database, iterate all shards
        db.with_all_shards(0, |shard| {
            let database = &shard.db;
            let mut local_entries = Vec::new();
            // Get keys from this logical database inside the shard
            for key in database.data_keys(db_idx) {
                if let Some(value) = database.get(db_idx, key) {
                    let expire_at_ms = database
                        .get_msexpiration(db_idx, key)
                        .copied();
                    local_entries.push(RdbEntry {
                        db_idx: db_idx as u8,
                        key: key.clone(),
                        value: value.clone(),
                        expire_at_ms,
                    });
                }
            }
            entries.lock().unwrap().extend(local_entries);
        });
    }

    let entries = entries.into_inner().unwrap();
    let snapshot = RdbSnapshot { entries };
    let encoded = bincode::serialize(&snapshot)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("bincode encode: {}", e)))?;

    let total_keys = snapshot.entries.len();

    // Write atomically: write to temp file then rename.
    let tmp_path = format!("{}.tmp", path);
    let mut fp = fs::File::create(&tmp_path)?;
    fp.write_all(RDB_MAGIC)?;
    // Write length prefix (u64) for the encoded blob.
    let len = encoded.len() as u64;
    fp.write_all(&len.to_le_bytes())?;
    fp.write_all(&encoded)?;
    fp.flush()?;
    fs::rename(&tmp_path, path)?;

    Ok(total_keys)
}

/// Load an RDB file and return the snapshot data.
pub fn load_from_file(path: &str) -> io::Result<RdbSnapshot> {
    let mut fp = fs::File::open(path)?;
    let mut magic = [0u8; 4];
    fp.read_exact(&mut magic)?;
    if &magic != RDB_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "not a valid rudis RDB file",
        ));
    }
    let mut len_buf = [0u8; 8];
    fp.read_exact(&mut len_buf)?;
    let len = u64::from_le_bytes(len_buf) as usize;
    let mut encoded = vec![0u8; len];
    fp.read_exact(&mut encoded)?;
    let snapshot: RdbSnapshot = bincode::deserialize(&encoded)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("bincode decode: {}", e)))?;
    Ok(snapshot)
}

/// Apply a loaded snapshot to a ShardedDatabase, routing each key to its
/// correct shard and logical database.
pub fn apply_snapshot(db: &ShardedDatabase, snapshot: RdbSnapshot) {
    let now_ms = util::mstime();
    let mut loaded = 0usize;

    for entry in snapshot.entries {
        // Skip expired entries.
        if let Some(exp) = entry.expire_at_ms {
            if exp <= now_ms {
                continue;
            }
        }

        // Route key to the correct shard using CRC64.
        let shard_idx = ShardedDatabase::shard_for_key(&entry.key) % db.num_shards();
        // Use the logical database index from the entry.
        let db_idx = entry.db_idx as usize;

        if let Ok(mut shard) = db.shard_write(0, shard_idx) {
            *shard.db.get_or_create(db_idx, &entry.key) = entry.value;
            if let Some(exp) = entry.expire_at_ms {
                shard.db.set_msexpiration(db_idx, entry.key.clone(), exp);
            }
            shard.db.key_updated(db_idx, &entry.key);
            loaded += 1;
        }
    }

    eprintln!("RDB: loaded {} keys from snapshot", loaded);
}

/// Check if an RDB file exists at the given path.
pub fn rdb_exists(path: &str) -> bool {
    Path::new(path).exists()
}
