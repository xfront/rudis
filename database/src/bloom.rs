//! Bloom Filter, Cuckoo Filter, t-digest, and Top-K implementations.
//!
//! These are used by the RedisBloom-compatible commands.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

// ============================================================================
// Bloom Filter
// ============================================================================

/// A Bloom filter implementation.
#[derive(Debug, Clone, PartialEq)]
pub struct BloomFilter {
    /// The bit vector.
    pub bits: Vec<u8>,
    /// Number of bits.
    pub capacity: usize,
    /// Number of hash functions.
    pub num_hashes: usize,
    /// Number of items added.
    pub size: usize,
    /// Expansion rate (for scalable bloom filters).
    pub expansion: usize,
    /// Current number of filters.
    pub num_filters: usize,
}

impl BloomFilter {
    /// Create a new bloom filter with given error rate and capacity.
    pub fn new(capacity: usize, error_rate: f64) -> Self {
        let capacity = capacity.max(1);
        let error_rate = error_rate.clamp(0.0001, 0.99);
        // Calculate optimal number of bits: m = -n*ln(p) / (ln(2))^2
        let ln2_sq = std::f64::consts::LN_2 * std::f64::consts::LN_2;
        let num_bits = (-(capacity as f64) * error_rate.ln() / ln2_sq).ceil() as usize;
        let num_bits = num_bits.max(8);
        // Calculate optimal number of hashes: k = (m/n) * ln(2)
        let num_hashes = ((num_bits as f64 / capacity as f64) * std::f64::consts::LN_2).ceil() as usize;
        let num_hashes = num_hashes.max(1).min(20);

        let bytes = (num_bits + 7) / 8;
        BloomFilter {
            bits: vec![0u8; bytes],
            capacity: num_bits,
            num_hashes,
            size: 0,
            expansion: 0,
            num_filters: 1,
        }
    }

    /// Create from explicit parameters (for BF.RESERVE).
    pub fn with_params(capacity_bits: usize, num_hashes: usize) -> Self {
        let bytes = (capacity_bits + 7) / 8;
        BloomFilter {
            bits: vec![0u8; bytes],
            capacity: capacity_bits,
            num_hashes: num_hashes.max(1),
            size: 0,
            expansion: 0,
            num_filters: 1,
        }
    }

    fn get_hashes(&self, item: &[u8]) -> Vec<usize> {
        let mut hashes = Vec::with_capacity(self.num_hashes);
        for i in 0..self.num_hashes {
            let mut hasher = DefaultHasher::new();
            i.hash(&mut hasher);
            item.hash(&mut hasher);
            let h = hasher.finish() as usize;
            hashes.push(h % self.capacity);
        }
        hashes
    }

    /// Add an item. Returns true if the item was definitely not present before.
    pub fn add(&mut self, item: &[u8]) -> bool {
        let hashes = self.get_hashes(item);
        let mut new_item = true;
        for &bit in &hashes {
            let byte_idx = bit / 8;
            let bit_idx = bit % 8;
            if self.bits[byte_idx] & (1 << bit_idx) == 0 {
                new_item = true;
            }
            self.bits[byte_idx] |= 1 << bit_idx;
        }
        if new_item {
            self.size += 1;
        }
        new_item
    }

    /// Check if an item might exist. Returns true if possibly present, false if definitely not.
    pub fn exists(&self, item: &[u8]) -> bool {
        let hashes = self.get_hashes(item);
        for &bit in &hashes {
            let byte_idx = bit / 8;
            let bit_idx = bit % 8;
            if self.bits[byte_idx] & (1 << bit_idx) == 0 {
                return false;
            }
        }
        true
    }

    /// Get info about the filter.
    pub fn info(&self) -> BloomFilterInfo {
        BloomFilterInfo {
            capacity: self.capacity,
            num_hashes: self.num_hashes,
            num_filters: self.num_filters,
            size: self.size,
            expansion: self.expansion,
            bits: self.bits.len() * 8,
        }
    }
}

/// Information about a bloom filter.
pub struct BloomFilterInfo {
    pub capacity: usize,
    pub num_hashes: usize,
    pub num_filters: usize,
    pub size: usize,
    pub expansion: usize,
    pub bits: usize,
}

// ============================================================================
// Cuckoo Filter
// ============================================================================

/// A Cuckoo filter implementation.
#[derive(Debug, Clone, PartialEq)]
pub struct CuckooFilter {
    /// Buckets, each containing up to `bucket_size` fingerprints.
    pub buckets: Vec<Vec<u16>>,
    /// Number of buckets.
    pub num_buckets: usize,
    /// Size of each bucket (max fingerprints per bucket).
    pub bucket_size: usize,
    /// Fingerprint size in bits.
    pub fingerprint_bits: usize,
    /// Maximum number of kicks before declaring full.
    pub max_kicks: usize,
    /// Number of items stored.
    pub size: usize,
    /// Number of items deleted.
    pub num_deletes: usize,
}

impl CuckooFilter {
    /// Create a new cuckoo filter with given capacity.
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        let bucket_size = 4; // Standard bucket size
        let num_buckets = (capacity + bucket_size - 1) / bucket_size;
        let num_buckets = num_buckets.next_power_of_two().max(16);
        CuckooFilter {
            buckets: vec![Vec::new(); num_buckets],
            num_buckets,
            bucket_size,
            fingerprint_bits: 16,
            max_kicks: 500,
            size: 0,
            num_deletes: 0,
        }
    }

    fn fingerprint(&self, item: &[u8]) -> u16 {
        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        let h = hasher.finish() as u16;
        // Ensure non-zero fingerprint
        if h == 0 { 1 } else { h }
    }

    fn bucket_index(&self, item: &[u8]) -> usize {
        let mut hasher = DefaultHasher::new();
        b"bucket".hash(&mut hasher);
        item.hash(&mut hasher);
        hasher.finish() as usize % self.num_buckets
    }

    fn alt_index(&self, index: usize, fp: u16) -> usize {
        let mut hasher = DefaultHasher::new();
        fp.hash(&mut hasher);
        (index ^ (hasher.finish() as usize)) % self.num_buckets
    }

    /// Add an item. Returns true if added successfully.
    pub fn add(&mut self, item: &[u8]) -> bool {
        let fp = self.fingerprint(item);
        let i1 = self.bucket_index(item);
        let i2 = self.alt_index(i1, fp);

        if self.buckets[i1].len() < self.bucket_size {
            self.buckets[i1].push(fp);
            self.size += 1;
            return true;
        }
        if self.buckets[i2].len() < self.bucket_size {
            self.buckets[i2].push(fp);
            self.size += 1;
            return true;
        }

        // Kick existing items
        let mut idx = if rand::random::<bool>() { i1 } else { i2 };
        for _ in 0..self.max_kicks {
            let kick_pos = rand::random::<usize>() % self.buckets[idx].len();
            let old_fp = self.buckets[idx][kick_pos];
            self.buckets[idx][kick_pos] = fp;
            idx = self.alt_index(idx, old_fp);
            if self.buckets[idx].len() < self.bucket_size {
                self.buckets[idx].push(old_fp);
                self.size += 1;
                return true;
            }
        }
        false // Filter is full
    }

    /// Add an item only if it doesn't exist. Returns true if added.
    pub fn addnx(&mut self, item: &[u8]) -> bool {
        if self.exists(item) {
            return false;
        }
        self.add(item)
    }

    /// Check if an item might exist.
    pub fn exists(&self, item: &[u8]) -> bool {
        let fp = self.fingerprint(item);
        let i1 = self.bucket_index(item);
        let i2 = self.alt_index(i1, fp);
        self.buckets[i1].contains(&fp) || self.buckets[i2].contains(&fp)
    }

    /// Count how many times an item appears (approximate).
    pub fn count(&self, item: &[u8]) -> usize {
        let fp = self.fingerprint(item);
        let i1 = self.bucket_index(item);
        let i2 = self.alt_index(i1, fp);
        let c1 = self.buckets[i1].iter().filter(|&&f| f == fp).count();
        let c2 = self.buckets[i2].iter().filter(|&&f| f == fp).count();
        c1 + c2
    }

    /// Delete an item. Returns true if deleted.
    pub fn delete(&mut self, item: &[u8]) -> bool {
        let fp = self.fingerprint(item);
        let i1 = self.bucket_index(item);
        let i2 = self.alt_index(i1, fp);

        if let Some(pos) = self.buckets[i1].iter().position(|&f| f == fp) {
            self.buckets[i1].remove(pos);
            self.size -= 1;
            self.num_deletes += 1;
            return true;
        }
        if let Some(pos) = self.buckets[i2].iter().position(|&f| f == fp) {
            self.buckets[i2].remove(pos);
            self.size -= 1;
            self.num_deletes += 1;
            return true;
        }
        false
    }

    /// Get info about the filter.
    pub fn info(&self) -> CuckooFilterInfo {
        CuckooFilterInfo {
            size: self.size,
            num_buckets: self.num_buckets,
            bucket_size: self.bucket_size,
            num_kicks: self.max_kicks,
            num_deletes: self.num_deletes,
            fingerprint_bits: self.fingerprint_bits,
        }
    }
}

pub struct CuckooFilterInfo {
    pub size: usize,
    pub num_buckets: usize,
    pub bucket_size: usize,
    pub num_kicks: usize,
    pub num_deletes: usize,
    pub fingerprint_bits: usize,
}

// ============================================================================
// t-digest (simplified)
// ============================================================================

/// A centroid in the t-digest.
#[derive(Debug, Clone, PartialEq)]
struct Centroid {
    mean: f64,
    count: f64,
}

/// A simplified t-digest for quantile estimation.
#[derive(Debug, Clone, PartialEq)]
pub struct TDigest {
    centroids: Vec<Centroid>,
    compression: f64,
    count: u64,
    min: f64,
    max: f64,
}

impl TDigest {
    pub fn new(compression: f64) -> Self {
        TDigest {
            centroids: Vec::new(),
            compression: compression.max(10.0),
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    /// Add a value to the digest.
    pub fn add(&mut self, value: f64) {
        if value < self.min { self.min = value; }
        if value > self.max { self.max = value; }
        self.centroids.push(Centroid { mean: value, count: 1.0 });
        self.count += 1;
        // Compress if too many centroids
        if self.centroids.len() > (self.compression as usize * 10) {
            self.compress();
        }
    }

    fn compress(&mut self) {
        if self.centroids.len() <= 1 { return; }
        self.centroids.sort_by(|a, b| a.mean.partial_cmp(&b.mean).unwrap());
        let mut new_centroids = vec![self.centroids[0].clone()];
        let total = self.count as f64;
        for c in &self.centroids[1..] {
            let last = new_centroids.last_mut().unwrap();
            let combined_count = last.count + c.count;
            let q = last.count / total;
            let max_count = 4.0 * total * q * (1.0 - q) / self.compression;
            if combined_count <= max_count.max(1.0) {
                last.mean = (last.mean * last.count + c.mean * c.count) / combined_count;
                last.count = combined_count;
            } else {
                new_centroids.push(c.clone());
            }
        }
        self.centroids = new_centroids;
    }

    /// Estimate a quantile (0.0 to 1.0).
    pub fn quantile(&self, q: f64) -> f64 {
        if self.centroids.is_empty() { return 0.0; }
        if q <= 0.0 { return self.min; }
        if q >= 1.0 { return self.max; }
        let mut sorted = self.centroids.clone();
        sorted.sort_by(|a, b| a.mean.partial_cmp(&b.mean).unwrap());
        let total: f64 = sorted.iter().map(|c| c.count).sum();
        let target = q * total;
        let mut cumulative = 0.0;
        for c in &sorted {
            cumulative += c.count;
            if cumulative >= target {
                return c.mean;
            }
        }
        sorted.last().map(|c| c.mean).unwrap_or(0.0)
    }

    /// Get the CDF (cumulative distribution function) value.
    pub fn cdf(&self, value: f64) -> f64 {
        if self.centroids.is_empty() { return 0.0; }
        let mut sorted = self.centroids.clone();
        sorted.sort_by(|a, b| a.mean.partial_cmp(&b.mean).unwrap());
        let total: f64 = sorted.iter().map(|c| c.count).sum();
        let mut cumulative = 0.0;
        for c in &sorted {
            if c.mean > value { break; }
            cumulative += c.count;
        }
        cumulative / total
    }

    pub fn min(&self) -> f64 { self.min }
    pub fn max(&self) -> f64 { self.max }
    pub fn count(&self) -> u64 { self.count }

    pub fn reset(&mut self) {
        self.centroids.clear();
        self.count = 0;
        self.min = f64::INFINITY;
        self.max = f64::NEG_INFINITY;
    }

    pub fn merge(&mut self, other: &TDigest) {
        for c in &other.centroids {
            self.centroids.push(c.clone());
        }
        self.count += other.count;
        if other.min < self.min { self.min = other.min; }
        if other.max > self.max { self.max = other.max; }
        self.compress();
    }

    /// Estimates the 0-based rank of `value`: the number of observations
    /// smaller than `value` plus half the number of observations equal to
    /// it. Returns -1 when the value is below the smallest observation, n
    /// when above the largest, and -2 for every input when empty.
    pub fn rank(&self, value: f64) -> i64 {
        if self.centroids.is_empty() { return -2; }
        if value < self.min { return -1; }
        let mut sorted = self.centroids.clone();
        sorted.sort_by(|a, b| a.mean.partial_cmp(&b.mean).unwrap());
        let mut smaller = 0.0;
        let mut equal = 0.0;
        for c in &sorted {
            if c.mean < value {
                smaller += c.count;
            } else if c.mean == value {
                equal += c.count;
            } else {
                break;
            }
        }
        (smaller + equal * 0.5).floor() as i64
    }

    /// Reverse rank: the number of observations greater than `value` plus
    /// half the observations equal to it. Returns -1 above the largest
    /// observation, n below the smallest, and -2 for every input when
    /// empty.
    pub fn revrank(&self, value: f64) -> i64 {
        if self.centroids.is_empty() { return -2; }
        if value > self.max { return -1; }
        if value < self.min { return self.count as i64; }
        let mut sorted = self.centroids.clone();
        sorted.sort_by(|a, b| a.mean.partial_cmp(&b.mean).unwrap());
        let mut greater = 0.0;
        let mut equal = 0.0;
        for c in &sorted {
            if c.mean > value {
                greater += c.count;
            } else if c.mean == value {
                equal += c.count;
            }
        }
        (greater + equal * 0.5).floor() as i64
    }

    /// Estimates the value occupying `rank` (0-based ascending). Ranks 0
    /// and n-1 are answered exactly with min/max; ranks >= n map to +inf,
    /// negative ranks to -inf, and an empty sketch answers nan.
    pub fn value_at_rank(&self, rank: f64) -> f64 {
        if self.centroids.is_empty() { return f64::NAN; }
        let n = self.count as i64;
        let r = rank.round() as i64;
        if r < 0 { return f64::NEG_INFINITY; }
        if r >= n { return f64::INFINITY; }
        if r == 0 { return self.min; }
        if r == n - 1 { return self.max; }
        let target = r as f64;
        let mut sorted = self.centroids.clone();
        sorted.sort_by(|a, b| a.mean.partial_cmp(&b.mean).unwrap());
        let mut cumulative = 0.0;
        for c in &sorted {
            if cumulative + c.count > target {
                return c.mean;
            }
            cumulative += c.count;
        }
        self.max
    }

    /// Estimates the value occupying `revrank` (0-based descending):
    /// revrank 0 is the largest observation and n-1 the smallest;
    /// revranks >= n map to -inf, negative to +inf.
    pub fn value_at_revrank(&self, revrank: f64) -> f64 {
        if self.centroids.is_empty() { return f64::NAN; }
        let n = self.count as i64;
        let r = revrank.round() as i64;
        if r < 0 { return f64::INFINITY; }
        if r >= n { return f64::NEG_INFINITY; }
        self.value_at_rank((n - 1 - r) as f64)
    }

    /// Mean of the observations inside the quantile window: observations
    /// below the `low` cutoff and at or above the `high` cutoff are
    /// excluded (cutoffs 0 / 1 disable the respective cut). nan when the
    /// sketch is empty or nothing remains after cutting.
    pub fn trimmed_mean(&self, low: f64, high: f64) -> f64 {
        if self.centroids.is_empty() { return f64::NAN; }
        let n = self.count as f64;
        let v_low = if low <= 0.0 { f64::NEG_INFINITY } else { self.value_at_rank(low * n) };
        let v_high = if high >= 1.0 { f64::INFINITY } else { self.value_at_rank(high * n) };
        let (mut sum, mut weight) = (0.0, 0.0);
        for c in &self.centroids {
            if c.mean < v_low || c.mean >= v_high { continue; }
            sum += c.mean * c.count;
            weight += c.count;
        }
        if weight == 0.0 { return f64::NAN; }
        sum / weight
    }
}

// ============================================================================
// Top-K
// ============================================================================

/// A Top-K data structure for tracking the K most frequent items.
#[derive(Debug, Clone, PartialEq)]
pub struct TopK {
    k: usize,
    width: usize,
    depth: usize,
    /// Counters: depth x width
    counters: Vec<Vec<(Vec<u8>, u64)>>,
    /// The current top-k items and their counts.
    heap: Vec<(Vec<u8>, u64)>,
    total: u64,
}

impl TopK {
    pub fn new(k: usize, width: usize, depth: usize) -> Self {
        let width = width.max(8);
        let depth = depth.max(3);
        TopK {
            k,
            width,
            depth,
            counters: vec![vec![(Vec::new(), 0); width]; depth],
            heap: Vec::with_capacity(k),
            total: 0,
        }
    }

    /// Add an item with optional increment.
    pub fn add(&mut self, item: &[u8], increment: u64) {
        self.total += increment;
        // Update counters at each depth
        for d in 0..self.depth {
            let idx = self.hash_index(item, d);
            let entry = &mut self.counters[d][idx];
            if entry.0 == item {
                entry.1 += increment;
            } else if entry.0.is_empty() || entry.0 == item {
                entry.0 = item.to_vec();
                entry.1 = increment;
            } else {
                // Eviction: decrease existing, maybe replace
                if entry.1 <= increment {
                    entry.0 = item.to_vec();
                    entry.1 = increment;
                } else {
                    entry.1 -= 1;
                }
            }
        }
        // Update heap
        let min_count = self.min_counter(item);
        if let Some(pos) = self.heap.iter().position(|(k, _)| k == item) {
            self.heap[pos].1 = min_count;
        } else if self.heap.len() < self.k {
            self.heap.push((item.to_vec(), min_count));
        } else if let Some(min_pos) = self.heap.iter().enumerate().min_by_key(|(_, (_, c))| *c).map(|(i, _)| i) {
            if min_count > self.heap[min_pos].1 {
                self.heap[min_pos] = (item.to_vec(), min_count);
            }
        }
    }

    fn hash_index(&self, item: &[u8], depth: usize) -> usize {
        let mut hasher = DefaultHasher::new();
        depth.hash(&mut hasher);
        item.hash(&mut hasher);
        hasher.finish() as usize % self.width
    }

    fn min_counter(&self, item: &[u8]) -> u64 {
        let mut min = u64::MAX;
        for d in 0..self.depth {
            let idx = self.hash_index(item, d);
            if self.counters[d][idx].0 == item {
                min = min.min(self.counters[d][idx].1);
            }
        }
        if min == u64::MAX { 0 } else { min }
    }

    /// Query if an item is in the top-k. Returns true if it is.
    pub fn query(&self, item: &[u8]) -> bool {
        self.heap.iter().any(|(k, _)| k == item)
    }

    /// Get the count estimate for an item.
    pub fn count(&self, item: &[u8]) -> u64 {
        self.min_counter(item)
    }

    /// List the current top-k items.
    pub fn list(&self) -> Vec<(Vec<u8>, u64)> {
        let mut result = self.heap.clone();
        result.sort_by(|a, b| b.1.cmp(&a.1));
        result
    }

    pub fn info(&self) -> TopKInfo {
        TopKInfo {
            k: self.k,
            width: self.width,
            depth: self.depth,
            total: self.total,
        }
    }
}

pub struct TopKInfo {
    pub k: usize,
    pub width: usize,
    pub depth: usize,
    pub total: u64,
}

#[cfg(test)]
mod test_bloom {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let mut bf = BloomFilter::new(1000, 0.01);
        assert!(!bf.exists(b"hello"));
        bf.add(b"hello");
        assert!(bf.exists(b"hello"));
        assert!(!bf.exists(b"world")); // Very likely false
    }

    #[test]
    fn test_bloom_filter_multiple() {
        let mut bf = BloomFilter::new(1000, 0.01);
        bf.add(b"apple");
        bf.add(b"banana");
        bf.add(b"cherry");
        assert!(bf.exists(b"apple"));
        assert!(bf.exists(b"banana"));
        assert!(bf.exists(b"cherry"));
    }

    #[test]
    fn test_cuckoo_filter_basic() {
        let mut cf = CuckooFilter::new(1000);
        assert!(!cf.exists(b"hello"));
        cf.add(b"hello");
        assert!(cf.exists(b"hello"));
    }

    #[test]
    fn test_cuckoo_filter_delete() {
        let mut cf = CuckooFilter::new(1000);
        cf.add(b"hello");
        assert!(cf.exists(b"hello"));
        cf.delete(b"hello");
        assert!(!cf.exists(b"hello"));
    }

    #[test]
    fn test_tdigest_basic() {
        let mut td = TDigest::new(100.0);
        for i in 1..=100 {
            td.add(i as f64);
        }
        assert_eq!(td.count(), 100);
        assert_eq!(td.min(), 1.0);
        assert_eq!(td.max(), 100.0);
        let median = td.quantile(0.5);
        assert!(median > 40.0 && median < 60.0, "median = {}", median);
    }

    #[test]
    fn test_topk_basic() {
        let mut topk = TopK::new(3, 32, 5);
        for _ in 0..10 { topk.add(b"a", 1); }
        for _ in 0..5 { topk.add(b"b", 1); }
        for _ in 0..3 { topk.add(b"c", 1); }
        topk.add(b"d", 1);
        assert!(topk.query(b"a"));
    }

    // Official example: TDIGEST.ADD s 10 20 30 40 50 60
    #[test]
    fn test_tdigest_rank_unique() {
        let mut td = TDigest::new(1000.0);
        for v in [10.0, 20.0, 30.0, 40.0, 50.0, 60.0] {
            td.add(v);
        }
        let expected = [-1i64, 0, 1, 2, 3, 4, 5, 6];
        for (i, v) in [0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0].iter().enumerate() {
            assert_eq!(td.rank(*v), expected[i], "rank({})", v);
        }
        let expected_rev = [6i64, 5, 4, 3, 2, 1, 0, -1];
        for (i, v) in [0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0].iter().enumerate() {
            assert_eq!(td.revrank(*v), expected_rev[i], "revrank({})", v);
        }
    }

    // Official example: TDIGEST.ADD s 10 10 10 10 20 20
    #[test]
    fn test_tdigest_rank_duplicates() {
        let mut td = TDigest::new(1000.0);
        for v in [10.0f64, 10.0, 10.0, 10.0, 20.0, 20.0] {
            td.add(v);
        }
        assert_eq!(td.rank(10.0), 2);
        assert_eq!(td.rank(20.0), 5);
        assert_eq!(td.revrank(10.0), 4);
        assert_eq!(td.revrank(20.0), 1);
    }

    // Official example: TDIGEST.ADD t 1 2 2 3 3 3 4 4 4 4 5 5 5 5 5
    #[test]
    fn test_tdigest_byrank() {
        let mut td = TDigest::new(1000.0);
        for v in [1.0f64, 2.0, 2.0, 3.0, 3.0, 3.0, 4.0, 4.0, 4.0, 4.0, 5.0, 5.0, 5.0, 5.0, 5.0] {
            td.add(v);
        }
        let expected = [1.0f64, 2.0, 2.0, 3.0, 3.0, 3.0, 4.0, 4.0, 4.0, 4.0, 5.0, 5.0, 5.0, 5.0, 5.0];
        for (i, r) in expected.iter().enumerate() {
            assert_eq!(td.value_at_rank(i as f64), *r, "byrank({})", i);
        }
        assert_eq!(td.value_at_rank(15.0), f64::INFINITY);
        assert_eq!(td.value_at_rank(-1.0), f64::NEG_INFINITY);
        assert_eq!(td.value_at_revrank(0.0), 5.0);
        assert_eq!(td.value_at_revrank(14.0), 1.0);
        assert_eq!(td.value_at_revrank(15.0), f64::NEG_INFINITY);
        assert_eq!(td.value_at_revrank(-1.0), f64::INFINITY);
        let empty = TDigest::new(1000.0);
        assert!(empty.value_at_rank(3.0).is_nan());
        assert_eq!(empty.rank(1.0), -2);
        assert_eq!(empty.revrank(1.0), -2);
    }

    // Official example: TDIGEST.ADD t 1 2 3 4 5 6 7 8 9 10
    #[test]
    fn test_tdigest_trimmed_mean() {
        let mut td = TDigest::new(1000.0);
        for v in 1..=10 {
            td.add(v as f64);
        }
        assert_eq!(td.trimmed_mean(0.1, 0.6), 4.0);
        assert_eq!(td.trimmed_mean(0.3, 0.9), 6.5);
        assert_eq!(td.trimmed_mean(0.0, 1.0), 5.5);
        let empty = TDigest::new(1000.0);
        assert!(empty.trimmed_mean(0.0, 1.0).is_nan());
    }
}
