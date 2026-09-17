//! Dragonfly-inspired sharded database infrastructure.
//!
//! The database is partitioned into multiple shards, each with its own lock.
//! Keys are mapped to shards using CRC64 hashing, enabling concurrent access
//! to different shards (shared-nothing architecture).

use std::sync::{Arc, Mutex};

use crc64::crc64;

use crate::{Database, Stats, Value};

/// Default number of shards per database index.
/// Dragonfly uses one shard per thread; we use a fixed count that can be
/// tuned based on the expected concurrency level.
pub const DEFAULT_SHARDS_PER_DB: usize = 16;

/// Per-database-index shard data.
/// Each shard holds a subset of the key space for one database index.
pub struct Shard {
    /// The database instance for this shard.
    /// Each shard has its own independent data, expiration, watches, etc.
    pub db: Database,
}

impl Shard {
    /// Creates a new empty shard with the given config.
    pub fn new(config: &config::Config) -> Self {
        Shard {
            db: Database::new_shard(config),
        }
    }

    /// Creates a new empty shard sharing the given statistics block with
    /// the other shards of a ShardedDatabase.
    pub fn with_stats(config: &config::Config, stats: Arc<Stats>) -> Self {
        Shard {
            db: Database::new_shard_with_stats(config, stats),
        }
    }
}

/// A sharded database that partitions keys across multiple shards.
///
/// Inspired by Dragonfly's shared-nothing architecture, this allows
/// concurrent access to different shards via per-shard Mutex.
///
/// # Architecture
/// ```text
/// ShardedDatabase
/// ├── shards[0]: Arc<Mutex<Shard>>  ← keys where hash(key) % num_shards == 0
/// ├── shards[1]: Arc<Mutex<Shard>>  ← keys where hash(key) % num_shards == 1
/// ├── ...
/// └── shards[N]: Arc<Mutex<Shard>>  ← keys where hash(key) % num_shards == N
/// ```
pub struct ShardedDatabase {
    /// Per-database-index, per-key-shard data.
    /// Outer vec: database indices (e.g., db0..db15).
    /// Inner vec: shards within each database index.
    shards: Vec<Vec<Arc<Mutex<Shard>>>>,

    /// Number of database indices (from config).
    num_databases: usize,

    /// Number of shards per database index.
    num_shards: usize,

    /// Server-wide statistics shared by every shard (read by INFO).
    pub stats: Arc<Stats>,
}

impl ShardedDatabase {
    /// Links every shard's Database back to this ShardedDatabase (weakly,
    /// with its own shard index) so admin commands (INFO/DBSIZE) can
    /// aggregate keyspace data across shards. Call once after the sharded
    /// database has been wrapped in an Arc.
    pub fn init_shard_links(self: &Arc<Self>) {
        for db_shards in &self.shards {
            for (shard_idx, shard) in db_shards.iter().enumerate() {
                let mut shard = shard.lock().unwrap();
                shard.db.sharded = Some((Arc::downgrade(self), shard_idx));
            }
        }
    }

    /// Aggregates `(keys, expires, ttl_sum, ttl_keys)` for one database index
    /// across all shards, optionally skipping one shard (the one whose lock is
    /// already held by the caller, which passes its own data instead).
    ///
    /// Logical database `db_index` lives in each shard's Database data maps,
    /// so this iterates the single shard layer and reads data map `db_index`.
    pub fn keyspace_stats_except(
        &self,
        db_index: usize,
        except: Option<usize>,
    ) -> (usize, usize, i64, usize) {
        let mut keys = 0usize;
        let mut expires = 0usize;
        let mut ttl_sum = 0i64;
        let mut ttl_keys = 0usize;
        for (shard_idx, shard) in self.shards[0].iter().enumerate() {
            if Some(shard_idx) == except {
                continue;
            }
            let shard = shard.lock().unwrap();
            let (k, e, s, c) = shard.db.keyspace_summary(db_index);
            keys += k;
            expires += e;
            ttl_sum += s;
            ttl_keys += c;
        }
        (keys, expires, ttl_sum, ttl_keys)
    }

    /// Clears every logical database on all shards except one (whose lock is
    /// already held by the caller, which clears itself afterwards).
    pub fn clearall_except(&self, except: Option<usize>) {
        for (shard_idx, shard) in self.shards[0].iter().enumerate() {
            if Some(shard_idx) == except {
                continue;
            }
            let mut shard = shard.lock().unwrap();
            shard.db.clearall();
        }
    }

    /// Clears one logical database on all shards except one (whose lock is
    /// already held by the caller, which clears itself afterwards).
    pub fn clear_db_except(&self, db_index: usize, except: Option<usize>) {
        for (shard_idx, shard) in self.shards[0].iter().enumerate() {
            if Some(shard_idx) == except {
                continue;
            }
            let mut shard = shard.lock().unwrap();
            shard.db.clear(db_index);
        }
    }

    /// Collects keys matching `pattern` in logical database `db_index` across
    /// all shards except one (the caller's own shard, whose lock is already
    /// held and which appends its own matches afterwards).
    pub fn keys_except(&self, db_index: usize, pattern: &[u8], except: Option<usize>) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        for (shard_idx, shard) in self.shards[0].iter().enumerate() {
            if Some(shard_idx) == except {
                continue;
            }
            let shard = shard.lock().unwrap();
            keys.extend(shard.db.keys(db_index, pattern));
        }
        keys
    }

    /// Snapshot of all keys in logical database `db_index` on every shard
    /// except one (the caller's own shard, whose lock is already held and
    /// which appends its own keys afterwards). Used by SCAN and RANDOMKEY.
    pub fn data_keys_except(&self, db_index: usize, except: Option<usize>) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        for (shard_idx, shard) in self.shards[0].iter().enumerate() {
            if Some(shard_idx) == except {
                continue;
            }
            let shard = shard.lock().unwrap();
            keys.extend(shard.db.data_keys(db_index).into_iter().cloned());
        }
        keys
    }

    /// Locks the shard hosting `key` and reads its value plus expiration.
    /// Used by cross-shard key commands (RENAME/COPY/...); the caller must
    /// not invoke this for a key hosted by its own shard (its lock is already
    /// held and the read would deadlock): check with `shard_of` first.
    pub fn read_with_ttl(&self, db_index: usize, key: &[u8]) -> Option<(Value, Option<i64>)> {
        let idx = Self::shard_for_key(key) % self.num_shards;
        let mut shard = self.shards[0][idx].lock().unwrap();
        let value = shard.db.get(db_index, key).cloned()?;
        let expiration = shard.db.get_msexpiration(db_index, key).copied();
        Some((value, expiration))
    }

    /// Returns `true` if `key` is hosted by `my_shard`.
    pub fn is_own_shard(&self, key: &[u8], my_shard: usize) -> bool {
        Self::shard_for_key(key) % self.num_shards == my_shard
    }

    /// Buckets `keys` by their hosting shard (ascending, the caller's own
    /// `my_shard` excluded) and, for each bucket, locks the shard and calls
    /// `f` with it and the positions (into `keys`) of the keys it hosts.
    ///
    /// Callers route multi-key commands to the *minimum* shard of their keys
    /// (see networking), so every locked shard here has a higher index than
    /// the one already held: locks are always taken in ascending order and
    /// no deadlock is possible. Buckets are processed one at a time; each
    /// lock is released before the next is taken.
    pub fn with_key_shards<F, R>(
        &self,
        db_index: usize,
        my_shard: Option<usize>,
        keys: &[&[u8]],
        mut f: F,
    ) -> Vec<(usize, R)>
    where
        F: FnMut(&mut Shard, &[usize]) -> R,
    {
        let mut buckets: Vec<(usize, Vec<usize>)> = Vec::new();
        for (pos, key) in keys.iter().enumerate() {
            let idx = Self::shard_for_key(key) % self.num_shards;
            if Some(idx) == my_shard {
                continue;
            }
            match buckets.iter_mut().find(|(i, _)| *i == idx) {
                Some((_, positions)) => positions.push(pos),
                None => buckets.push((idx, vec![pos])),
            }
        }
        buckets.sort_by_key(|(idx, _)| *idx);

        let mut results = Vec::with_capacity(buckets.len());
        for (idx, positions) in &buckets {
            let mut shard = self.shards[0][*idx].lock().unwrap();
            results.push((*idx, f(&mut shard, positions)));
        }
        results
    }

    /// Reads owned clones of the values under `keys` from every shard except
    /// `my_shard` (whose keys the caller reads through its own `Database`).
    /// Returns one entry per key position; entries for keys hosted by
    /// `my_shard` stay `None`.
    pub fn read_values_except(
        &self,
        db_index: usize,
        my_shard: Option<usize>,
        keys: &[&[u8]],
    ) -> Vec<Option<Value>> {
        let mut out: Vec<Option<Value>> = vec![None; keys.len()];
        self.with_key_shards(db_index, my_shard, keys, |shard, positions| {
            for &p in positions {
                out[p] = shard.db.get(db_index, keys[p]).cloned();
            }
        });
        out
    }

    /// Writes `value` (with an optional expiration) under `key`, on whichever
    /// shard hosts it. Returns `false` (and writes nothing) when the key is
    /// hosted by `my_shard`: the caller must write those through its own
    /// already-locked `Database`.
    pub fn write_value(
        &self,
        db_index: usize,
        my_shard: Option<usize>,
        key: &[u8],
        value: Value,
        msexpiration: Option<i64>,
    ) -> bool {
        if let Some(my) = my_shard {
            if self.is_own_shard(key, my) {
                return false;
            }
        }
        let mut value = Some(value);
        self.with_key_shards(db_index, my_shard, &[key], |shard, _| {
            shard.db.remove_msexpiration(db_index, key);
            *shard.db.get_or_create(db_index, key) = value.take().unwrap();
            if let Some(exp) = msexpiration {
                shard.db.set_msexpiration(db_index, key.to_vec(), exp);
            }
            shard.db.key_updated(db_index, key);
            true
        });
        true
    }

    /// Removes `key` (and its expiration) from whichever shard hosts it.
    /// Returns `false` when the key is hosted by `my_shard` (the caller
    /// removes those locally); otherwise whether the key existed.
    pub fn remove_key(
        &self,
        db_index: usize,
        my_shard: Option<usize>,
        key: &[u8],
    ) -> bool {
        if let Some(my) = my_shard {
            if self.is_own_shard(key, my) {
                return false;
            }
        }
        let mut existed = false;
        self.with_key_shards(db_index, my_shard, &[key], |shard, _| {
            existed = shard.db.remove(db_index, key).is_some();
            shard.db.remove_msexpiration(db_index, key);
            if existed {
                shard.db.key_updated(db_index, key);
            }
            true
        });
        existed
    }

    /// Creates a new ShardedDatabase with the default number of shards.
    pub fn new(config: &config::Config) -> Self {
        Self::with_shards(config, DEFAULT_SHARDS_PER_DB)
    }

    /// Creates a new ShardedDatabase with a specified number of shards.
    pub fn with_shards(config: &config::Config, num_shards: usize) -> Self {
        // The outer dimension is the database index, but every command goes
        // through shard 0 of this layer: logical databases are carried by the
        // per-shard Database's own data maps (one per configured database).
        // Keep a single layer here to avoid hosting unused Database instances.
        let num_databases = 1;
        let mut shards = Vec::with_capacity(num_databases);
        let stats = Stats::new();

        for _db_index in 0..num_databases {
            let db_shards = (0..num_shards)
                .map(|_| Arc::new(Mutex::new(Shard::with_stats(config, stats.clone()))))
                .collect();
            shards.push(db_shards);
        }

        ShardedDatabase {
            shards,
            num_databases,
            num_shards,
            stats,
        }
    }

    /// Computes the shard index for a given key using CRC64 hashing.
    #[inline]
    pub fn shard_for_key(key: &[u8]) -> usize {
        // Will be adjusted modulo num_shards by the caller
        crc64(0, key) as usize
    }

    /// Gets the shard index for a key within a specific database's shard count.
    #[inline]
    pub fn shard_index(&self, key: &[u8]) -> usize {
        Self::shard_for_key(key) % self.num_shards
    }

    /// Returns the number of shards.
    #[inline]
    pub fn num_shards(&self) -> usize {
        self.num_shards
    }

    /// Returns the number of database indices.
    #[inline]
    pub fn num_databases(&self) -> usize {
        self.num_databases
    }

    /// Gets a reference to a shard's Arc<Mutex<Shard>>.
    #[inline]
    pub fn get_shard(&self, db_index: usize, key: &[u8]) -> &Arc<Mutex<Shard>> {
        let shard_idx = self.shard_index(key);
        &self.shards[db_index][shard_idx]
    }

    /// Gets a reference to a shard's Arc by database and shard index.
    #[inline]
    pub fn get_shard_by_index(&self, db_index: usize, shard_idx: usize) -> &Arc<Mutex<Shard>> {
        &self.shards[db_index][shard_idx]
    }

    /// Gets write access to a specific shard by database index and shard index.
    /// Returns a MutexGuard for direct access.
    pub fn shard_write(
        &self,
        db_index: usize,
        shard_idx: usize,
    ) -> Result<std::sync::MutexGuard<'_, Shard>, std::sync::PoisonError<std::sync::MutexGuard<'_, Shard>>> {
        self.shards[db_index][shard_idx].lock()
    }

    /// Executes a closure with access to the appropriate shard.
    pub fn with_shard<F, R>(&self, db_index: usize, key: &[u8], f: F) -> R
    where
        F: FnOnce(&mut Shard) -> R,
    {
        let shard = self.get_shard(db_index, key);
        let mut guard = shard.lock().unwrap();
        f(&mut *guard)
    }

    /// Executes a closure with access to a specific shard by index.
    pub fn with_shard_by_index<F, R>(&self, db_index: usize, shard_idx: usize, f: F) -> R
    where
        F: FnOnce(&mut Shard) -> R,
    {
        let shard = &self.shards[db_index][shard_idx];
        let mut guard = shard.lock().unwrap();
        f(&mut *guard)
    }

    /// Executes a closure with access to all shards of a database index.
    /// Useful for global operations like KEYS, DBSIZE, etc.
    pub fn with_all_shards<F, R>(&self, db_index: usize, f: F) -> Vec<R>
    where
        F: Fn(&mut Shard) -> R,
    {
        let mut results = Vec::with_capacity(self.shards[db_index].len());
        for shard in &self.shards[db_index] {
            let mut guard = shard.lock().unwrap();
            results.push(f(&mut *guard));
        }
        results
    }

    /// Executes a closure with access to all shards across all databases.
    /// Useful for FLUSHALL.
    pub fn with_all_shards_all_dbs<F>(&self, f: F)
    where
        F: Fn(&mut Shard),
    {
        for db_shards in &self.shards {
            for shard in db_shards {
                let mut guard = shard.lock().unwrap();
                f(&mut *guard);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use config::Config;
    use logger::{Level, Logger};

    fn test_config() -> Config {
        Config::new(Logger::new(Level::Warning))
    }

    #[test]
    fn shard_creation() {
        let config = test_config();
        let sharded = ShardedDatabase::with_shards(&config, 4);
        assert_eq!(sharded.num_shards(), 4);
        // One shard layer: logical databases live inside each shard's Database.
        assert_eq!(sharded.num_databases(), 1);
    }

    #[test]
    fn shard_routing_consistency() {
        let config = test_config();
        let sharded = ShardedDatabase::with_shards(&config, 8);
        let key = b"test_key";

        // Same key should always map to the same shard
        let shard1 = sharded.shard_index(key);
        let shard2 = sharded.shard_index(key);
        assert_eq!(shard1, shard2);
        assert!(shard1 < 8);
    }

    #[test]
    fn shard_routing_distribution() {
        let config = test_config();
        let sharded = ShardedDatabase::with_shards(&config, 4);

        // Different keys should (usually) map to different shards
        let mut shard_counts = vec![0usize; 4];
        for i in 0..100u32 {
            let key = format!("key_{}", i);
            let shard = sharded.shard_index(key.as_bytes());
            shard_counts[shard] += 1;
        }

        // Each shard should have at least some keys (statistical distribution)
        for count in &shard_counts {
            assert!(*count > 0, "Expected some keys in each shard");
        }
    }

    #[test]
    fn shard_read_write() {
        let config = test_config();
        let sharded = ShardedDatabase::with_shards(&config, 4);
        let key = b"hello".to_vec();
        let val = b"world".to_vec();

        // Write to shard
        sharded.with_shard(0, &key, |shard| {
            shard.db.get_or_create(0, &key).set(val.clone()).unwrap();
        });

        // Read from shard
        let result = sharded.with_shard(0, &key, |shard| shard.db.dbsize(0));

        assert_eq!(result, 1);
    }
}
