//! Dragonfly-inspired sharded database infrastructure.
//!
//! The database is partitioned into multiple shards, each with its own lock.
//! Keys are mapped to shards using CRC64 hashing, enabling concurrent access
//! to different shards (shared-nothing architecture).

use std::sync::{Arc, Mutex};

use crc64::crc64;

use crate::Database;

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
}

impl ShardedDatabase {
    /// Creates a new ShardedDatabase with the default number of shards.
    pub fn new(config: &config::Config) -> Self {
        Self::with_shards(config, DEFAULT_SHARDS_PER_DB)
    }

    /// Creates a new ShardedDatabase with a specified number of shards.
    pub fn with_shards(config: &config::Config, num_shards: usize) -> Self {
        let num_databases = config.databases as usize;
        let mut shards = Vec::with_capacity(num_databases);

        for _db_index in 0..num_databases {
            let db_shards = (0..num_shards)
                .map(|_| Arc::new(Mutex::new(Shard::new(config))))
                .collect();
            shards.push(db_shards);
        }

        ShardedDatabase {
            shards,
            num_databases,
            num_shards,
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
