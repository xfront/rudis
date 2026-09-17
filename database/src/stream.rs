use std::collections::{HashMap, VecDeque};

use error::OperationError;

/// A Stream ID, consisting of milliseconds timestamp and a sequence number.
#[derive(PartialEq, Eq, Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct StreamID {
    pub ms: u64,
    pub seq: u64,
}

impl StreamID {
    pub fn new(ms: u64, seq: u64) -> Self {
        StreamID { ms, seq }
    }

    /// Zero ID (0-0)
    pub fn zero() -> Self {
        StreamID { ms: 0, seq: 0 }
    }

    /// Parse a stream ID from "ms-seq" format.
    /// Also supports "*" for auto-generation (returns None to signal caller).
    pub fn parse(s: &[u8]) -> Result<Option<StreamID>, OperationError> {
        let s = std::str::from_utf8(s)
            .map_err(|_| OperationError::ValueError("ERR invalid stream ID".to_owned()))?;
        if s == "*" {
            return Ok(None);
        }
        if let Some((ms_str, seq_str)) = s.split_once('-') {
            let ms: u64 = ms_str.parse()
                .map_err(|_| OperationError::ValueError("ERR invalid stream ID".to_owned()))?;
            let seq: u64 = seq_str.parse()
                .map_err(|_| OperationError::ValueError("ERR invalid stream ID".to_owned()))?;
            Ok(Some(StreamID::new(ms, seq)))
        } else {
            // Just a number means ms with seq=0
            let ms: u64 = s.parse()
                .map_err(|_| OperationError::ValueError("ERR invalid stream ID".to_owned()))?;
            Ok(Some(StreamID::new(ms, 0)))
        }
    }

    /// Format as "ms-seq"
    pub fn to_string(&self) -> String {
        format!("{}-{}", self.ms, self.seq)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }
}

impl PartialOrd for StreamID {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StreamID {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.ms.cmp(&other.ms) {
            std::cmp::Ordering::Equal => self.seq.cmp(&other.seq),
            other => other,
        }
    }
}

/// A single entry in a Stream.
#[derive(PartialEq, Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StreamEntry {
    pub id: StreamID,
    pub fields: Vec<(Vec<u8>, Vec<u8>)>,
}

/// A pending entry tracked by a consumer group.
#[derive(PartialEq, Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PendingEntry {
    pub id: StreamID,
    pub consumer: Vec<u8>,
    pub last_delivery_time: i64,
    pub delivery_count: u64,
}

/// A consumer within a consumer group.
#[derive(PartialEq, Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Consumer {
    pub name: Vec<u8>,
    pub last_seen: i64,
}

/// A consumer group for a Stream.
#[derive(PartialEq, Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConsumerGroup {
    /// Last delivered ID for this group.
    pub last_id: StreamID,
    /// Number of entries added to the stream since group creation.
    pub entries_added: u64,
    /// Consumers in this group.
    pub consumers: HashMap<Vec<u8>, Consumer>,
    /// Pending entries list (PEL).
    pub pending: VecDeque<PendingEntry>,
}

impl ConsumerGroup {
    pub fn new(last_id: StreamID) -> Self {
        ConsumerGroup {
            last_id,
            entries_added: 0,
            consumers: HashMap::new(),
            pending: VecDeque::new(),
        }
    }
}

/// A Stream value stored in the database.
#[derive(PartialEq, Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ValueStream {
    /// Entries in chronological order (oldest first).
    entries: VecDeque<StreamEntry>,
    /// The last (highest) ID added to this stream.
    last_id: StreamID,
    /// Consumer groups.
    groups: HashMap<Vec<u8>, ConsumerGroup>,
    /// Total number of entries added since creation (never decreases on delete).
    entries_added: u64,
}

impl Default for ValueStream {
    fn default() -> Self {
        Self::new()
    }
}

impl ValueStream {
    /// Creates a new empty stream.
    pub fn new() -> ValueStream {
        ValueStream {
            entries: VecDeque::new(),
            last_id: StreamID::zero(),
            groups: HashMap::new(),
            entries_added: 0,
        }
    }

    /// Returns the number of entries in the stream.
    pub fn xlen(&self) -> usize {
        self.entries.len()
    }

    /// Returns the last ID of the stream.
    pub fn last_id(&self) -> StreamID {
        self.last_id
    }

    /// Returns total entries added (never decreases).
    pub fn entries_added(&self) -> u64 {
        self.entries_added
    }

    /// Generates the next auto-increment ID.
    fn next_id(&mut self, now_ms: u64) -> StreamID {
        if self.last_id.ms < now_ms {
            StreamID::new(now_ms, 0)
        } else {
            StreamID::new(self.last_id.ms, self.last_id.seq + 1)
        }
    }

    /// Adds an entry to the stream. Returns the assigned StreamID.
    /// `id` = None means auto-generate (*).
    /// `fields` must have an even number of elements (field-value pairs).
    pub fn xadd(
        &mut self,
        id: Option<StreamID>,
        fields: Vec<(Vec<u8>, Vec<u8>)>,
        now_ms: u64,
    ) -> Result<StreamID, OperationError> {
        let assigned_id = match id {
            None => self.next_id(now_ms),
            Some(given) => {
                // Must be greater than the current last_id
                if given <= self.last_id {
                    return Err(OperationError::ValueError(
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item".to_owned(),
                    ));
                }
                given
            }
        };

        let entry = StreamEntry {
            id: assigned_id,
            fields,
        };
        self.entries.push_back(entry);
        self.last_id = assigned_id;
        self.entries_added += 1;

        // Update consumer groups' entries_added
        for group in self.groups.values_mut() {
            group.entries_added += 1;
        }

        Ok(assigned_id)
    }

    /// Returns entries in the given ID range [start, end].
    /// Use StreamID::zero() for the minimum and StreamID::new(u64::MAX, u64::MAX) for the maximum.
    /// `count` limits the number of returned entries (None = all).
    pub fn xrange(
        &self,
        start: StreamID,
        end: StreamID,
        count: Option<usize>,
    ) -> Vec<&StreamEntry> {
        let mut result = Vec::new();
        for entry in &self.entries {
            if entry.id >= start && entry.id <= end {
                result.push(entry);
                if let Some(c) = count {
                    if result.len() >= c {
                        break;
                    }
                }
            }
        }
        result
    }

    /// Returns entries in reverse order within the given ID range [end, start].
    pub fn xrevrange(
        &self,
        end: StreamID,
        start: StreamID,
        count: Option<usize>,
    ) -> Vec<&StreamEntry> {
        let mut result = Vec::new();
        for entry in self.entries.iter().rev() {
            if entry.id <= end && entry.id >= start {
                result.push(entry);
                if let Some(c) = count {
                    if result.len() >= c {
                        break;
                    }
                }
            }
        }
        result
    }

    /// Deletes entries by their IDs. Returns the number of entries actually deleted.
    pub fn xdel(&mut self, ids: &[StreamID]) -> usize {
        let mut count = 0;
        for id in ids {
            if let Some(pos) = self.entries.iter().position(|e| e.id == *id) {
                self.entries.remove(pos);
                count += 1;
            }
        }
        count
    }

    /// Trims the stream by MAXLEN or MINID.
    /// `maxlen`: Some(n) keeps at most n entries.
    /// `minid`: Some(id) removes entries with ID < id.
    /// `approx`: if true, may trim approximately (for now we do exact).
    /// Returns the number of entries deleted.
    pub fn xtrim(
        &mut self,
        maxlen: Option<usize>,
        minid: Option<StreamID>,
        _approx: bool,
        _limit: Option<usize>,
    ) -> usize {
        let mut deleted = 0;

        if let Some(ml) = maxlen {
            while self.entries.len() > ml {
                self.entries.pop_front();
                deleted += 1;
            }
        }

        if let Some(min_id) = minid {
            while let Some(front) = self.entries.front() {
                if front.id < min_id {
                    self.entries.pop_front();
                    deleted += 1;
                } else {
                    break;
                }
            }
        }

        deleted
    }

    /// Reads entries from the stream starting after the given ID.
    /// Returns entries with ID > `start_id`.
    pub fn xread(&self, start_id: StreamID, count: Option<usize>) -> Vec<&StreamEntry> {
        let mut result = Vec::new();
        for entry in &self.entries {
            if entry.id > start_id {
                result.push(entry);
                if let Some(c) = count {
                    if result.len() >= c {
                        break;
                    }
                }
            }
        }
        result
    }

    /// Creates a consumer group. Returns error if group already exists.
    /// `last_id` = "$" means use the stream's last_id.
    pub fn xgroup_create(
        &mut self,
        group_name: Vec<u8>,
        last_id: Option<StreamID>,
    ) -> Result<(), OperationError> {
        if self.groups.contains_key(&group_name) {
            return Err(OperationError::ValueError(
                "ERR BUSYGROUP Consumer Group name already exists".to_owned(),
            ));
        }
        let id = last_id.unwrap_or(self.last_id);
        self.groups.insert(group_name, ConsumerGroup::new(id));
        Ok(())
    }

    /// Destroys a consumer group. Returns true if it existed.
    pub fn xgroup_destroy(&mut self, group_name: &[u8]) -> bool {
        self.groups.remove(group_name).is_some()
    }

    /// Sets the consumer group's last delivered ID.
    pub fn xgroup_setid(&mut self, group_name: &[u8], id: StreamID) -> Result<(), OperationError> {
        match self.groups.get_mut(group_name) {
            Some(group) => {
                group.last_id = id;
                Ok(())
            }
            None => Err(OperationError::ValueError(
                "ERR NOGROUP No such consumer group".to_owned(),
            )),
        }
    }

    /// Creates a consumer within a group. Returns true if newly created.
    pub fn xgroup_createconsumer(
        &mut self,
        group_name: &[u8],
        consumer_name: Vec<u8>,
        now_ms: i64,
    ) -> Result<bool, OperationError> {
        match self.groups.get_mut(group_name) {
            Some(group) => {
                if group.consumers.contains_key(&consumer_name) {
                    return Ok(false);
                }
                group.consumers.insert(
                    consumer_name.clone(),
                    Consumer {
                        name: consumer_name,
                        last_seen: now_ms,
                    },
                );
                Ok(true)
            }
            None => Err(OperationError::ValueError(
                "ERR NOGROUP No such consumer group".to_owned(),
            )),
        }
    }

    /// Deletes a consumer from a group. Returns true if it existed.
    pub fn xgroup_delconsumer(
        &mut self,
        group_name: &[u8],
        consumer_name: &[u8],
    ) -> Result<usize, OperationError> {
        match self.groups.get_mut(group_name) {
            Some(group) => {
                // Remove all pending entries for this consumer
                let before = group.pending.len();
                group.pending.retain(|p| p.consumer != consumer_name);
                let removed_pending = before - group.pending.len();
                let consumer_existed = group.consumers.remove(consumer_name).is_some();
                Ok(if consumer_existed {
                    removed_pending
                } else {
                    0
                })
            }
            None => Err(OperationError::ValueError(
                "ERR NOGROUP No such consumer group".to_owned(),
            )),
        }
    }

    /// Reads entries for a consumer group (XREADGROUP).
    /// Returns entries and updates the PEL.
    pub fn xreadgroup(
        &mut self,
        group_name: &[u8],
        consumer_name: &[u8],
        count: Option<usize>,
        noack: bool,
        now_ms: i64,
    ) -> Result<Vec<&StreamEntry>, OperationError> {
        let group = self.groups.get_mut(group_name).ok_or_else(|| {
            OperationError::ValueError("ERR NOGROUP No such consumer group".to_owned())
        })?;

        // Ensure consumer exists
        if !group.consumers.contains_key(consumer_name) {
            group.consumers.insert(
                consumer_name.to_vec(),
                Consumer {
                    name: consumer_name.to_vec(),
                    last_seen: now_ms,
                },
            );
        }

        // Update consumer last_seen
        if let Some(consumer) = group.consumers.get_mut(consumer_name) {
            consumer.last_seen = now_ms;
        }

        let last_id = group.last_id;
        let mut result = Vec::new();

        for entry in &self.entries {
            if entry.id > last_id {
                result.push(entry);
                if let Some(c) = count {
                    if result.len() >= c {
                        break;
                    }
                }
            }
        }

        // Update group's last_id and PEL
        if let Some(last_entry) = result.last() {
            let new_last_id = last_entry.id;
            let group = self.groups.get_mut(group_name).unwrap();
            group.last_id = new_last_id;

            if !noack {
                for entry in &result {
                    group.pending.push_back(PendingEntry {
                        id: entry.id,
                        consumer: consumer_name.to_vec(),
                        last_delivery_time: now_ms,
                        delivery_count: 1,
                    });
                }
            }
        }

        Ok(result)
    }

    /// Acknowledges entries in a consumer group. Returns the count of ACKed entries.
    pub fn xack(&mut self, group_name: &[u8], ids: &[StreamID]) -> usize {
        match self.groups.get_mut(group_name) {
            Some(group) => {
                let mut count = 0;
                for id in ids {
                    if let Some(pos) = group.pending.iter().position(|p| p.id == *id) {
                        group.pending.remove(pos);
                        count += 1;
                    }
                }
                count
            }
            None => 0,
        }
    }

    /// Returns pending entries info for a group.
    pub fn xpending(
        &self,
        group_name: &[u8],
        start: Option<StreamID>,
        end: Option<StreamID>,
        count: Option<usize>,
        consumer_filter: Option<&[u8]>,
    ) -> Result<Vec<&PendingEntry>, OperationError> {
        let group = self.groups.get(group_name).ok_or_else(|| {
            OperationError::ValueError("ERR NOGROUP No such consumer group".to_owned())
        })?;

        let mut result: Vec<&PendingEntry> = group
            .pending
            .iter()
            .filter(|p| {
                let mut ok = true;
                if let Some(s) = start {
                    ok = ok && p.id >= s;
                }
                if let Some(e) = end {
                    ok = ok && p.id <= e;
                }
                if let Some(c) = consumer_filter {
                    ok = ok && p.consumer == c;
                }
                ok
            })
            .collect();

        if let Some(c) = count {
            result.truncate(c);
        }

        Ok(result)
    }

    /// Returns summary info for xpending (without range).
    pub fn xpending_summary(
        &self,
        group_name: &[u8],
    ) -> Result<(usize, Option<StreamID>, Option<StreamID>, Vec<(Vec<u8>, usize)>), OperationError>
    {
        let group = self.groups.get(group_name).ok_or_else(|| {
            OperationError::ValueError("ERR NOGROUP No such consumer group".to_owned())
        })?;

        let total = group.pending.len();
        let min_id = group.pending.front().map(|p| p.id);
        let max_id = group.pending.back().map(|p| p.id);

        // Count per consumer
        let mut consumer_counts: HashMap<Vec<u8>, usize> = HashMap::new();
        for p in &group.pending {
            *consumer_counts.entry(p.consumer.clone()).or_insert(0) += 1;
        }
        let consumer_list: Vec<(Vec<u8>, usize)> = consumer_counts.into_iter().collect();

        Ok((total, min_id, max_id, consumer_list))
    }

    /// Returns info about the stream for XINFO STREAM.
    pub fn xinfo(&self) -> StreamInfo {
        StreamInfo {
            length: self.entries.len(),
            last_id: self.last_id,
            entries_added: self.entries_added,
            first_id: self.entries.front().map(|e| e.id),
            groups: self.groups.len(),
        }
    }

    /// Returns all consumer groups info.
    pub fn xinfo_groups(&self) -> Vec<GroupInfo> {
        self.groups
            .iter()
            .map(|(name, group)| GroupInfo {
                name: name.clone(),
                consumers: group.consumers.len(),
                pending: group.pending.len(),
                last_id: group.last_id,
                entries_added: group.entries_added,
            })
            .collect()
    }

    /// Returns consumers info for a group.
    pub fn xinfo_consumers(
        &self,
        group_name: &[u8],
    ) -> Result<Vec<&Consumer>, OperationError> {
        let group = self.groups.get(group_name).ok_or_else(|| {
            OperationError::ValueError("ERR NOGROUP No such consumer group".to_owned())
        })?;
        Ok(group.consumers.values().collect())
    }

    /// Sets the stream's last_id (XSETID command).
    pub fn xsetid(&mut self, id: StreamID, entries_added: Option<u64>) {
        self.last_id = id;
        if let Some(ea) = entries_added {
            self.entries_added = ea;
        }
    }

    /// Claims pending entries from a consumer group (XCLAIM).
    pub fn xclaim(
        &mut self,
        group_name: &[u8],
        consumer_name: &[u8],
        min_idle_time: i64,
        ids: &[StreamID],
        now_ms: i64,
    ) -> Result<Vec<&StreamEntry>, OperationError> {
        let group = self.groups.get_mut(group_name).ok_or_else(|| {
            OperationError::ValueError("ERR NOGROUP No such consumer group".to_owned())
        })?;

        // Ensure consumer exists
        if !group.consumers.contains_key(consumer_name) {
            group.consumers.insert(
                consumer_name.to_vec(),
                Consumer {
                    name: consumer_name.to_vec(),
                    last_seen: now_ms,
                },
            );
        }

        let mut result_ids = Vec::new();
        for id in ids {
            if let Some(pending) = group.pending.iter_mut().find(|p| p.id == *id) {
                let idle_time = now_ms - pending.last_delivery_time;
                if idle_time >= min_idle_time {
                    pending.consumer = consumer_name.to_vec();
                    pending.last_delivery_time = now_ms;
                    pending.delivery_count += 1;
                    result_ids.push(*id);
                }
            }
        }

        // Find the actual entries
        let result: Vec<&StreamEntry> = self
            .entries
            .iter()
            .filter(|e| result_ids.contains(&e.id))
            .collect();

        Ok(result)
    }

    /// Auto-claims pending entries (XAUTOCLAIM).
    pub fn xautoclaim(
        &mut self,
        group_name: &[u8],
        consumer_name: &[u8],
        min_idle_time: i64,
        start: StreamID,
        count: usize,
        now_ms: i64,
    ) -> Result<(Vec<Vec<u8>>, StreamID), OperationError> {
        let group = self.groups.get_mut(group_name).ok_or_else(|| {
            OperationError::ValueError("ERR NOGROUP No such consumer group".to_owned())
        })?;

        // Ensure consumer exists
        if !group.consumers.contains_key(consumer_name) {
            group.consumers.insert(
                consumer_name.to_vec(),
                Consumer {
                    name: consumer_name.to_vec(),
                    last_seen: now_ms,
                },
            );
        }

        let mut claimed_ids = Vec::new();
        let mut cursor = StreamID::zero();
        let mut scanned = 0;

        for pending in group.pending.iter_mut() {
            if scanned >= count {
                break;
            }
            if pending.id >= start {
                let idle_time = now_ms - pending.last_delivery_time;
                if idle_time >= min_idle_time {
                    pending.consumer = consumer_name.to_vec();
                    pending.last_delivery_time = now_ms;
                    pending.delivery_count += 1;
                    claimed_ids.push(pending.id);
                }
                cursor = pending.id;
                scanned += 1;
            }
        }

        // Format claimed entries as [id, fields...] arrays
        let mut result = Vec::new();
        for id in &claimed_ids {
            if let Some(entry) = self.entries.iter().find(|e| e.id == *id) {
                let mut entry_resp = vec![id.to_bytes()];
                for (f, v) in &entry.fields {
                    entry_resp.push(f.clone());
                    entry_resp.push(v.clone());
                }
                result.push(entry_resp);
            }
        }

        // Next cursor: the ID after the last scanned
        let next_cursor = if scanned >= count {
            // Return the next ID to scan
            StreamID::new(cursor.ms, cursor.seq + 1)
        } else {
            StreamID::new(0, 0) // Signal completion
        };

        Ok((result.into_iter().flatten().collect(), next_cursor))
    }

    /// Returns all entries in the stream (for AOF rewrite).
    pub fn all_entries(&self) -> &VecDeque<StreamEntry> {
        &self.entries
    }

    /// Returns all consumer groups as a reference (for AOF rewrite).
    pub fn all_groups(&self) -> &HashMap<Vec<u8>, ConsumerGroup> {
        &self.groups
    }
}

/// Summary info for XINFO STREAM.
pub struct StreamInfo {
    pub length: usize,
    pub last_id: StreamID,
    pub entries_added: u64,
    pub first_id: Option<StreamID>,
    pub groups: usize,
}

/// Info about a consumer group for XINFO GROUPS.
pub struct GroupInfo {
    pub name: Vec<u8>,
    pub consumers: usize,
    pub pending: usize,
    pub last_id: StreamID,
    pub entries_added: u64,
}

#[cfg(test)]
mod test_stream {
    use super::*;

    #[test]
    fn stream_id_parse() {
        let id = StreamID::parse(b"1-1").unwrap().unwrap();
        assert_eq!(id.ms, 1);
        assert_eq!(id.seq, 1);

        let id = StreamID::parse(b"100").unwrap().unwrap();
        assert_eq!(id.ms, 100);
        assert_eq!(id.seq, 0);

        assert!(StreamID::parse(b"*").unwrap().is_none());
    }

    #[test]
    fn stream_id_ordering() {
        let a = StreamID::new(1, 0);
        let b = StreamID::new(1, 1);
        let c = StreamID::new(2, 0);
        assert!(a < b);
        assert!(b < c);
        assert!(a < c);
    }

    #[test]
    fn xadd_xlen() {
        let mut stream = ValueStream::new();
        let id1 = stream
            .xadd(None, vec![(b"f1".to_vec(), b"v1".to_vec())], 1000)
            .unwrap();
        assert_eq!(id1.ms, 1000);
        assert_eq!(id1.seq, 0);
        assert_eq!(stream.xlen(), 1);

        let id2 = stream
            .xadd(None, vec![(b"f2".to_vec(), b"v2".to_vec())], 1000)
            .unwrap();
        assert_eq!(id2.ms, 1000);
        assert_eq!(id2.seq, 1);
        assert_eq!(stream.xlen(), 2);
    }

    #[test]
    fn xadd_explicit_id() {
        let mut stream = ValueStream::new();
        let id = stream
            .xadd(
                Some(StreamID::new(5, 0)),
                vec![(b"f1".to_vec(), b"v1".to_vec())],
                1000,
            )
            .unwrap();
        assert_eq!(id, StreamID::new(5, 0));

        // Can't add smaller ID
        let result = stream.xadd(
            Some(StreamID::new(4, 0)),
            vec![(b"f2".to_vec(), b"v2".to_vec())],
            1000,
        );
        assert!(result.is_err());
    }

    #[test]
    fn xrange_test() {
        let mut stream = ValueStream::new();
        for i in 1..=5 {
            stream
                .xadd(
                    Some(StreamID::new(i as u64, 0)),
                    vec![(b"f".to_vec(), format!("v{}", i).into_bytes())],
                    1000,
                )
                .unwrap();
        }

        let entries = stream.xrange(StreamID::new(2, 0), StreamID::new(4, 0), None);
        assert_eq!(entries.len(), 3);

        let entries = stream.xrange(StreamID::new(2, 0), StreamID::new(4, 0), Some(2));
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn xrevrange_test() {
        let mut stream = ValueStream::new();
        for i in 1..=5 {
            stream
                .xadd(
                    Some(StreamID::new(i as u64, 0)),
                    vec![(b"f".to_vec(), format!("v{}", i).into_bytes())],
                    1000,
                )
                .unwrap();
        }

        let entries = stream.xrevrange(StreamID::new(4, 0), StreamID::new(2, 0), None);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].id, StreamID::new(4, 0));
        assert_eq!(entries[2].id, StreamID::new(2, 0));
    }

    #[test]
    fn xdel_test() {
        let mut stream = ValueStream::new();
        for i in 1..=3 {
            stream
                .xadd(
                    Some(StreamID::new(i as u64, 0)),
                    vec![(b"f".to_vec(), b"v".to_vec())],
                    1000,
                )
                .unwrap();
        }
        assert_eq!(stream.xlen(), 3);

        let deleted = stream.xdel(&[StreamID::new(2, 0)]);
        assert_eq!(deleted, 1);
        assert_eq!(stream.xlen(), 2);
    }

    #[test]
    fn xtrim_maxlen() {
        let mut stream = ValueStream::new();
        for i in 1..=5 {
            stream
                .xadd(
                    Some(StreamID::new(i as u64, 0)),
                    vec![(b"f".to_vec(), b"v".to_vec())],
                    1000,
                )
                .unwrap();
        }

        let deleted = stream.xtrim(Some(3), None, false, None);
        assert_eq!(deleted, 2);
        assert_eq!(stream.xlen(), 3);
    }

    #[test]
    fn xread_test() {
        let mut stream = ValueStream::new();
        for i in 1..=5 {
            stream
                .xadd(
                    Some(StreamID::new(i as u64, 0)),
                    vec![(b"f".to_vec(), b"v".to_vec())],
                    1000,
                )
                .unwrap();
        }

        let entries = stream.xread(StreamID::new(3, 0), None);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].id, StreamID::new(4, 0));
    }

    #[test]
    fn consumer_group_basic() {
        let mut stream = ValueStream::new();
        for i in 1..=3 {
            stream
                .xadd(
                    Some(StreamID::new(i as u64, 0)),
                    vec![(b"f".to_vec(), b"v".to_vec())],
                    1000,
                )
                .unwrap();
        }

        // Create group
        stream.xgroup_create(b"mygroup".to_vec(), Some(StreamID::zero())).unwrap();

        // Duplicate group should fail
        assert!(stream.xgroup_create(b"mygroup".to_vec(), None).is_err());

        // Destroy group
        assert!(stream.xgroup_destroy(b"mygroup"));
        assert!(!stream.xgroup_destroy(b"mygroup"));
    }

    #[test]
    fn xack_test() {
        let mut stream = ValueStream::new();
        for i in 1..=3 {
            stream
                .xadd(
                    Some(StreamID::new(i as u64, 0)),
                    vec![(b"f".to_vec(), b"v".to_vec())],
                    1000,
                )
                .unwrap();
        }

        stream.xgroup_create(b"grp".to_vec(), Some(StreamID::zero())).unwrap();
        let entries = stream
            .xreadgroup(b"grp", b"consumer1", None, false, 5000)
            .unwrap();
        assert_eq!(entries.len(), 3);

        let acked = stream.xack(b"grp", &[StreamID::new(2, 0)]);
        assert_eq!(acked, 1);
    }
}
