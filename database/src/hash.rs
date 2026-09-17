use std::collections::HashMap;
use std::io;
use std::io::Write;
use std::str;

use error::OperationError;
use rdbutil::constants::*;
use rdbutil::{encode_len, encode_slice_u8};

/// A hash value stored in the database.
/// Internally uses a HashMap<Vec<u8>, Vec<u8>> mapping field names to values.
/// Per-field expiration is tracked via field_expiration_ms.
#[derive(PartialEq, Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ValueHash {
    data: HashMap<Vec<u8>, Vec<u8>>,
    /// Per-field expiration times in milliseconds (absolute timestamps).
    field_expiration_ms: HashMap<Vec<u8>, i64>,
}

impl Default for ValueHash {
    fn default() -> Self {
        Self::new()
    }
}

impl ValueHash {
    /// Creates a new empty hash.
    pub fn new() -> ValueHash {
        ValueHash {
            data: HashMap::new(),
            field_expiration_ms: HashMap::new(),
        }
    }

    /// Sets a field to a value. Returns true if the field is new, false if it
    /// was updated.
    pub fn hset(&mut self, field: Vec<u8>, value: Vec<u8>) -> bool {
        self.data.insert(field, value).is_none()
    }

    /// Sets a field to a value only if the field does not already exist.
    /// Returns true if the field was set, false if it already existed.
    pub fn hsetnx(&mut self, field: Vec<u8>, value: Vec<u8>) -> bool {
        if self.data.contains_key(&field) {
            false
        } else {
            self.data.insert(field, value);
            true
        }
    }

    /// Gets the value of a field. Returns None if the field does not exist.
    pub fn hget(&self, field: &[u8]) -> Option<&Vec<u8>> {
        self.data.get(field)
    }

    /// Gets the values of multiple fields. For each field, returns the value
    /// or None if the field does not exist.
    pub fn hmget(&self, fields: &[Vec<u8>]) -> Vec<Option<&Vec<u8>>> {
        fields.iter().map(|f| self.data.get(f.as_slice())).collect()
    }

    /// Sets multiple field-value pairs. Returns the number of new fields added.
    pub fn hmset(&mut self, field_values: Vec<(Vec<u8>, Vec<u8>)>) -> usize {
        let mut count = 0;
        for (field, value) in field_values {
            if self.data.insert(field, value).is_none() {
                count += 1;
            }
        }
        count
    }

    /// Deletes one or more fields. Returns the number of fields removed.
    pub fn hdel(&mut self, fields: &[Vec<u8>]) -> usize {
        let mut count = 0;
        for field in fields {
            if self.data.remove(field).is_some() {
                count += 1;
            }
        }
        count
    }

    /// Returns the number of fields in the hash.
    pub fn hlen(&self) -> usize {
        self.data.len()
    }

    /// Returns the length of the value associated with the field, or 0 if
    /// the field does not exist.
    pub fn hstrlen(&self, field: &[u8]) -> usize {
        match self.data.get(field) {
            Some(v) => v.len(),
            None => 0,
        }
    }

    /// Checks if a field exists in the hash.
    pub fn hexists(&self, field: &[u8]) -> bool {
        self.data.contains_key(field)
    }

    /// Returns all field names in the hash.
    pub fn hkeys(&self) -> Vec<Vec<u8>> {
        self.data.keys().cloned().collect()
    }

    /// Returns all values in the hash.
    pub fn hvals(&self) -> Vec<Vec<u8>> {
        self.data.values().cloned().collect()
    }

    /// Returns all field-value pairs in the hash.
    pub fn hgetall(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.data
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    // --- Per-field expiration methods ---

    /// Check if a field is expired and remove it if so.
    fn remove_if_expired(&mut self, field: &[u8], now_ms: i64) -> bool {
        if let Some(&exp) = self.field_expiration_ms.get(field) {
            if exp <= now_ms {
                self.data.remove(field);
                self.field_expiration_ms.remove(field);
                return true;
            }
        }
        false
    }

    /// Sets expiration for specific fields. Returns results for each field:
    /// 1 = expiration set, 0 = field not found, -1 = field already expired or no change.
    /// `nx`: only set if field has no expiration
    /// `xx`: only set if field already has expiration
    /// `gt`: only set if new expiration > current
    /// `lt`: only set if new expiration < current
    pub fn hexpire_fields(
        &mut self,
        fields: &[Vec<u8>],
        msexpiration: i64,
        now_ms: i64,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
    ) -> Vec<i64> {
        let mut results = Vec::with_capacity(fields.len());
        for field in fields {
            if !self.data.contains_key(field.as_slice()) {
                results.push(0); // field not found
                continue;
            }
            // Clean expired first
            self.remove_if_expired(field, now_ms);
            if !self.data.contains_key(field.as_slice()) {
                results.push(0);
                continue;
            }
            let current_exp = self.field_expiration_ms.get(field).copied();
            let ok = match (nx, xx, gt, lt) {
                (true, _, _, _) if current_exp.is_some() => false,
                (_, true, _, _) if current_exp.is_none() => false,
                (_, _, true, _) if current_exp.is_some() && msexpiration <= current_exp.unwrap() => false,
                (_, _, _, true) if current_exp.is_some() && msexpiration >= current_exp.unwrap() => false,
                _ => true,
            };
            if ok {
                self.field_expiration_ms.insert(field.clone(), msexpiration);
                results.push(1);
            } else {
                results.push(-1);
            }
        }
        results
    }

    /// Returns TTL in milliseconds for specified fields.
    /// Returns: -1 = no expiration, -2 = field not found, >=0 = TTL in ms
    pub fn httl_fields(&self, fields: &[Vec<u8>], now_ms: i64) -> Vec<i64> {
        fields.iter().map(|field| {
            if !self.data.contains_key(field.as_slice()) {
                return -2;
            }
            match self.field_expiration_ms.get(field.as_slice()) {
                Some(&exp) => {
                    let ttl = exp - now_ms;
                    if ttl <= 0 { -2 } else { ttl }
                }
                None => -1,
            }
        }).collect()
    }

    /// Removes expiration from specified fields.
    /// Returns: 1 = persist succeeded, 0 = field not found, -1 = field had no expiration
    pub fn hpersist_fields(&mut self, fields: &[Vec<u8>]) -> Vec<i64> {
        fields.iter().map(|field| {
            if !self.data.contains_key(field.as_slice()) {
                0
            } else if self.field_expiration_ms.remove(field.as_slice()).is_some() {
                1
            } else {
                -1
            }
        }).collect()
    }

    /// Returns the number of non-expired fields.
    pub fn hlen_active(&self, now_ms: i64) -> usize {
        self.data.iter().filter(|(k, _)| {
            match self.field_expiration_ms.get(k.as_slice()) {
                Some(&exp) => exp > now_ms,
                None => true,
            }
        }).count()
    }

    /// Increments the integer value of a field by the given amount.
    /// Creates the field with value 0 if it doesn't exist.
    /// Returns the new value.
    pub fn hincrby(&mut self, field: Vec<u8>, increment: i64) -> Result<i64, OperationError> {
        let current = match self.data.get(&field) {
            Some(val) => {
                let s = str::from_utf8(val)?;
                if s.starts_with('0') && s.len() > 1 {
                    return Err(OperationError::ValueError(
                        "ERR value is not a valid integer".to_owned(),
                    ));
                }
                s.parse::<i64>()
                    .map_err(|_| OperationError::ValueError("ERR value is not a valid integer".to_owned()))?
            }
            None => 0,
        };
        let newval = current
            .checked_add(increment)
            .ok_or(OperationError::OverflowError)?;
        self.data.insert(field, newval.to_string().into_bytes());
        Ok(newval)
    }

    /// Increments the float value of a field by the given amount.
    /// Creates the field with value 0 if it doesn't exist.
    /// Returns the new value.
    pub fn hincrbyfloat(&mut self, field: Vec<u8>, increment: f64) -> Result<f64, OperationError> {
        let current = match self.data.get(&field) {
            Some(val) => {
                let s = str::from_utf8(val)?;
                s.parse::<f64>()
                    .map_err(|_| OperationError::ValueError("ERR value is not a valid float".to_owned()))?
            }
            None => 0.0,
        };
        let newval = current + increment;
        self.data
            .insert(field, format!("{}", newval).into_bytes());
        Ok(newval)
    }

    /// Serializes the hash for the DUMP command.
    pub fn dump<T: Write>(&self, writer: &mut T) -> io::Result<usize> {
        let mut v = vec![];
        encode_len(self.data.len(), &mut v).map_err(|e| io::Error::from(e))?;
        for (field, value) in &self.data {
            encode_slice_u8(field, &mut v, true).map_err(|e| io::Error::from(e))?;
            encode_slice_u8(value, &mut v, true).map_err(|e| io::Error::from(e))?;
        }
        let data = [
            vec![TYPE_HASH],
            v,
            vec![(VERSION & 0xff) as u8],
            vec![((VERSION >> 8) & 0xff) as u8],
        ]
        .concat();
        writer.write(&*data)
    }

    /// Returns a debug description of the hash.
    pub fn debug_object(&self) -> String {
        let mut serialized_data = vec![];
        let serialized = self.dump(&mut serialized_data).unwrap();
        format!(
            "Value at:0x0000000000 refcount:1 encoding:hashtable serializedlength:{} lru:0 \
             lru_seconds_idle:0",
            serialized
        )
    }
}

#[cfg(test)]
mod test_hash {
    use super::ValueHash;

    #[test]
    fn hset_hget() {
        let mut hash = ValueHash::new();
        assert!(hash.hset(b"field1".to_vec(), b"value1".to_vec()));
        assert!(!hash.hset(b"field1".to_vec(), b"value2".to_vec()));
        assert_eq!(hash.hget(b"field1"), Some(&b"value2".to_vec()));
        assert_eq!(hash.hget(b"field2"), None);
    }

    #[test]
    fn hsetnx() {
        let mut hash = ValueHash::new();
        assert!(hash.hsetnx(b"field1".to_vec(), b"value1".to_vec()));
        assert!(!hash.hsetnx(b"field1".to_vec(), b"value2".to_vec()));
        assert_eq!(hash.hget(b"field1"), Some(&b"value1".to_vec()));
    }

    #[test]
    fn hdel() {
        let mut hash = ValueHash::new();
        hash.hset(b"field1".to_vec(), b"value1".to_vec());
        hash.hset(b"field2".to_vec(), b"value2".to_vec());
        assert_eq!(hash.hdel(&[b"field1".to_vec(), b"field3".to_vec()]), 1);
        assert_eq!(hash.hlen(), 1);
    }

    #[test]
    fn hlen() {
        let mut hash = ValueHash::new();
        assert_eq!(hash.hlen(), 0);
        hash.hset(b"field1".to_vec(), b"value1".to_vec());
        assert_eq!(hash.hlen(), 1);
        hash.hset(b"field2".to_vec(), b"value2".to_vec());
        assert_eq!(hash.hlen(), 2);
    }

    #[test]
    fn hexists() {
        let mut hash = ValueHash::new();
        hash.hset(b"field1".to_vec(), b"value1".to_vec());
        assert!(hash.hexists(b"field1"));
        assert!(!hash.hexists(b"field2"));
    }

    #[test]
    fn hkeys_hvals() {
        let mut hash = ValueHash::new();
        hash.hset(b"field1".to_vec(), b"value1".to_vec());
        hash.hset(b"field2".to_vec(), b"value2".to_vec());
        let mut keys = hash.hkeys();
        keys.sort();
        assert_eq!(keys, vec![b"field1".to_vec(), b"field2".to_vec()]);
        let mut vals = hash.hvals();
        vals.sort();
        assert_eq!(vals, vec![b"value1".to_vec(), b"value2".to_vec()]);
    }

    #[test]
    fn hgetall() {
        let mut hash = ValueHash::new();
        hash.hset(b"field1".to_vec(), b"value1".to_vec());
        hash.hset(b"field2".to_vec(), b"value2".to_vec());
        let mut pairs = hash.hgetall();
        pairs.sort();
        assert_eq!(
            pairs,
            vec![
                (b"field1".to_vec(), b"value1".to_vec()),
                (b"field2".to_vec(), b"value2".to_vec()),
            ]
        );
    }

    #[test]
    fn hincrby() {
        let mut hash = ValueHash::new();
        assert_eq!(hash.hincrby(b"field1".to_vec(), 5).unwrap(), 5);
        assert_eq!(hash.hincrby(b"field1".to_vec(), 3).unwrap(), 8);
        assert_eq!(hash.hincrby(b"field1".to_vec(), -2).unwrap(), 6);
    }

    #[test]
    fn hincrbyfloat() {
        let mut hash = ValueHash::new();
        assert_eq!(hash.hincrbyfloat(b"field1".to_vec(), 1.5).unwrap(), 1.5);
        assert_eq!(hash.hincrbyfloat(b"field1".to_vec(), 2.5).unwrap(), 4.0);
    }

    #[test]
    fn hmset_hmget() {
        let mut hash = ValueHash::new();
        hash.hmset(vec![
            (b"field1".to_vec(), b"value1".to_vec()),
            (b"field2".to_vec(), b"value2".to_vec()),
        ]);
        let results = hash.hmget(&[b"field1".to_vec(), b"field3".to_vec()]);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], Some(&b"value1".to_vec()));
        assert_eq!(results[1], None);
    }

    #[test]
    fn hstrlen() {
        let mut hash = ValueHash::new();
        hash.hset(b"field1".to_vec(), b"value1".to_vec());
        assert_eq!(hash.hstrlen(b"field1"), 6);
        assert_eq!(hash.hstrlen(b"field2"), 0);
    }
}
