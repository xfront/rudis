//! Dragonfly-inspired sharded database infrastructure.
//!
//! The database is partitioned into multiple shards, each with its own lock.
//! Keys are mapped to shards using CRC64 hashing, enabling concurrent access
//! to different shards (shared-nothing architecture).

use std::sync::{Arc, Mutex};

use crc64::crc64;

use crate::{Database, Stats};

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
    pub fn keyspace_stats_except(
        &self,
        db_index: usize,
        except: Option<usize>,
    ) -> (usize, usize, i64, usize) {
        let mut keys = 0usize;
        let mut expires = 0usize;
        let mut ttl_sum = 0i64;
        let mut ttl_keys = 0usize;
        for (shard_idx, shard) in self.shards[db_index].iter().enumerate() {
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

    /// Creates a new ShardedDatabase with the default number of shards.
    pub fn new(config: &config::Config) -> Self {
        Self::with_shards(config, DEFAULT_SHARDS_PER_DB)
    }

    /// Creates a new ShardedDatabase with a specified number of shards.
    pub fn with_shards(config: &config::Config, num_shards: usize) -> Self {
        let num_databases = config.databases as usize;
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
        assert_eq!(sharded.num_databases(), 16);
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
