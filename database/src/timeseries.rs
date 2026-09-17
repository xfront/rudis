//! RedisTimeSeries-compatible data structure.
//!
//! Stores time-value pairs with labels and supports range queries.

use std::collections::BTreeMap;

/// A single sample in the time series.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Sample {
    pub timestamp: i64,
    pub value: f64,
}

/// Duplicate policy for handling same-timestamp inserts.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum DuplicatePolicy {
    Block,
    First,
    Last,
    Min,
    Max,
    Sum,
}

/// A time series data structure.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TimeSeries {
    /// Samples stored by timestamp.
    pub samples: BTreeMap<i64, f64>,
    /// Retention period in milliseconds (0 = no retention).
    pub retention_ms: i64,
    /// Labels attached to this time series.
    pub labels: Vec<(String, String)>,
    /// Chunk size (for compatibility, not used in this implementation).
    pub chunk_size: usize,
    /// Duplicate policy.
    pub duplicate_policy: DuplicatePolicy,
    /// Encoding type (for compatibility).
    pub encoding: String,
}

impl TimeSeries {
    pub fn new(retention_ms: i64, labels: Vec<(String, String)>, duplicate_policy: DuplicatePolicy) -> Self {
        TimeSeries {
            samples: BTreeMap::new(),
            retention_ms,
            labels,
            chunk_size: 4096,
            duplicate_policy,
            encoding: "compressed".to_owned(),
        }
    }

    /// Add a sample. Returns the timestamp of the added sample.
    pub fn add(&mut self, timestamp: i64, value: f64) -> Result<i64, String> {
        // Handle auto-timestamp
        let ts = if timestamp == -1 {
            // Use current time in milliseconds
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0)
        } else {
            timestamp
        };

        // Handle duplicate policy
        if let Some(existing) = self.samples.get(&ts) {
            match self.duplicate_policy {
                DuplicatePolicy::Block => return Err(format!("ERR TSDB: Error at TSDB.ADD, timestamp {} already exists", ts)),
                DuplicatePolicy::First => return Ok(ts),
                DuplicatePolicy::Last => { self.samples.insert(ts, value); }
                DuplicatePolicy::Min => { if value < *existing { self.samples.insert(ts, value); } }
                DuplicatePolicy::Max => { if value > *existing { self.samples.insert(ts, value); } }
                DuplicatePolicy::Sum => { let sum = existing + value; self.samples.insert(ts, sum); }
            }
        } else {
            self.samples.insert(ts, value);
        }

        // Apply retention
        self.apply_retention();

        Ok(ts)
    }

    /// Add multiple samples.
    pub fn madd(&mut self, samples: &[(i64, f64)]) -> Result<Vec<i64>, String> {
        let mut timestamps = Vec::new();
        for (ts, val) in samples {
            timestamps.push(self.add(*ts, *val)?);
        }
        Ok(timestamps)
    }

    /// Increment the value at a timestamp.
    pub fn incrby(&mut self, timestamp: i64, value: f64) -> Result<i64, String> {
        let ts = if timestamp == -1 {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0)
        } else {
            timestamp
        };
        let current = self.samples.get(&ts).copied().unwrap_or(0.0);
        self.samples.insert(ts, current + value);
        self.apply_retention();
        Ok(ts)
    }

    /// Decrement the value at a timestamp.
    pub fn decrby(&mut self, timestamp: i64, value: f64) -> Result<i64, String> {
        self.incrby(timestamp, -value)
    }

    /// Delete samples in a time range.
    pub fn del(&mut self, from: i64, to: i64) -> usize {
        let keys: Vec<i64> = self.samples.range(from..=to).map(|(&k, _)| k).collect();
        let count = keys.len();
        for k in keys {
            self.samples.remove(&k);
        }
        count
    }

    /// Get the last sample.
    pub fn get(&self) -> Option<Sample> {
        self.samples.iter().next_back().map(|(&ts, &val)| Sample { timestamp: ts, value: val })
    }

    /// Query samples in a time range.
    pub fn range(&self, from: i64, to: i64, count: Option<usize>) -> Vec<Sample> {
        let iter = self.samples.range(from..=to);
        let samples: Vec<Sample> = iter.map(|(&ts, &val)| Sample { timestamp: ts, value: val }).collect();
        match count {
            Some(n) => samples.into_iter().take(n).collect(),
            None => samples,
        }
    }

    /// Query samples in reverse order.
    pub fn revrange(&self, from: i64, to: i64, count: Option<usize>) -> Vec<Sample> {
        let mut samples = self.range(from, to, None);
        samples.reverse();
        match count {
            Some(n) => samples.into_iter().take(n).collect(),
            None => samples,
        }
    }

    fn apply_retention(&mut self) {
        if self.retention_ms <= 0 {
            return;
        }
        if let Some((&max_ts, _)) = self.samples.iter().next_back() {
            let cutoff = max_ts - self.retention_ms;
            let keys: Vec<i64> = self.samples.range(..cutoff).map(|(&k, _)| k).collect();
            for k in keys {
                self.samples.remove(&k);
            }
        }
    }

    pub fn info(&self) -> TimeSeriesInfo {
        let (min_ts, max_ts) = match (self.samples.keys().next(), self.samples.keys().next_back()) {
            (Some(&min), Some(&max)) => (min, max),
            _ => (0, 0),
        };
        TimeSeriesInfo {
            total_samples: self.samples.len(),
            retention_ms: self.retention_ms,
            chunk_count: 1,
            chunk_size: self.chunk_size,
            labels: self.labels.clone(),
            min_timestamp: min_ts,
            max_timestamp: max_ts,
            encoding: self.encoding.clone(),
            duplicate_policy: format!("{:?}", self.duplicate_policy),
        }
    }
}

/// Information about a time series.
pub struct TimeSeriesInfo {
    pub total_samples: usize,
    pub retention_ms: i64,
    pub chunk_count: usize,
    pub chunk_size: usize,
    pub labels: Vec<(String, String)>,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub encoding: String,
    pub duplicate_policy: String,
}

#[cfg(test)]
mod test_timeseries {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut ts = TimeSeries::new(0, vec![], DuplicatePolicy::Last);
        ts.add(1000, 1.0).unwrap();
        ts.add(2000, 2.0).unwrap();
        ts.add(3000, 3.0).unwrap();
        assert_eq!(ts.samples.len(), 3);
        assert_eq!(ts.get().unwrap().value, 3.0);
    }

    #[test]
    fn test_range() {
        let mut ts = TimeSeries::new(0, vec![], DuplicatePolicy::Last);
        for i in 1..=10 {
            ts.add(i * 1000, i as f64).unwrap();
        }
        let result = ts.range(3000, 7000, None);
        assert_eq!(result.len(), 5);
        assert_eq!(result[0].value, 3.0);
        assert_eq!(result[4].value, 7.0);
    }

    #[test]
    fn test_retention() {
        let mut ts = TimeSeries::new(5000, vec![], DuplicatePolicy::Last);
        ts.add(1000, 1.0).unwrap();
        ts.add(10000, 2.0).unwrap(); // This should evict ts=1000
        assert_eq!(ts.samples.len(), 1);
        assert!(ts.samples.contains_key(&10000));
    }

    #[test]
    fn test_duplicate_policy() {
        let mut ts = TimeSeries::new(0, vec![], DuplicatePolicy::Last);
        ts.add(1000, 1.0).unwrap();
        ts.add(1000, 2.0).unwrap();
        assert_eq!(*ts.samples.get(&1000).unwrap(), 2.0);

        let mut ts = TimeSeries::new(0, vec![], DuplicatePolicy::Max);
        ts.add(1000, 5.0).unwrap();
        ts.add(1000, 3.0).unwrap();
        assert_eq!(*ts.samples.get(&1000).unwrap(), 5.0);
    }

    #[test]
    fn test_del() {
        let mut ts = TimeSeries::new(0, vec![], DuplicatePolicy::Last);
        for i in 1..=5 {
            ts.add(i * 1000, i as f64).unwrap();
        }
        let deleted = ts.del(2000, 4000);
        assert_eq!(deleted, 3);
        assert_eq!(ts.samples.len(), 2);
    }
}
