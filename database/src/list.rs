use std::collections::LinkedList;
use std::io;
use std::io::Write;

use dbutil::normalize_position;
use error::OperationError;
use rdbutil::constants::*;
use rdbutil::{encode_len, encode_slice_u8};

#[derive(PartialEq, Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ValueList {
    Data(LinkedList<Vec<u8>>),
}

impl Default for ValueList {
    fn default() -> Self {
        Self::new()
    }
}

impl ValueList {
    pub fn new() -> Self {
        ValueList::Data(LinkedList::new())
    }

    pub fn push(&mut self, el: Vec<u8>, right: bool) {
        match *self {
            ValueList::Data(ref mut list) => {
                if right {
                    list.push_back(el);
                } else {
                    list.push_front(el);
                }
            }
        };
    }

    pub fn pop(&mut self, right: bool) -> Option<Vec<u8>> {
        match *self {
            ValueList::Data(ref mut list) => {
                if right {
                    list.pop_back()
                } else {
                    list.pop_front()
                }
            }
        }
    }

    pub fn lindex(&self, _index: i64) -> Option<&[u8]> {
        match *self {
            ValueList::Data(ref list) => {
                let index = match normalize_position(_index, list.len()) {
                    Ok(i) => i,
                    Err(_) => return None,
                };
                list.iter().nth(index as usize).map(|a| &a[..])
            }
        }
    }

    pub fn linsert(&mut self, before: bool, pivot: Vec<u8>, newvalue: Vec<u8>) -> Option<usize> {
        match *self {
            ValueList::Data(ref mut list) => match list.iter().position(|x| x == &pivot) {
                Some(_pos) => {
                    let pos = if before { _pos } else { _pos + 1 };
                    let mut right = list.split_off(pos);
                    list.push_back(newvalue);
                    list.append(&mut right);
                    Some(list.len())
                }
                None => None,
            },
        }
    }

    pub fn llen(&self) -> usize {
        match *self {
            ValueList::Data(ref list) => list.len(),
        }
    }

    pub fn lrange(&self, _start: i64, _stop: i64) -> Vec<&[u8]> {
        match *self {
            ValueList::Data(ref list) => {
                let len = list.len();
                let start = match normalize_position(_start, len) {
                    Ok(i) => i,
                    Err(g) => {
                        if !g {
                            0
                        } else {
                            return Vec::new();
                        }
                    }
                };
                let stop = match normalize_position(_stop, len) {
                    Ok(i) => i,
                    Err(g) => {
                        if !g {
                            return Vec::new();
                        } else {
                            len
                        }
                    }
                };
                list.iter()
                    .skip(start as usize)
                    .take(stop as usize - start as usize + 1)
                    .map(|a| &a[..])
                    .collect::<Vec<_>>()
            }
        }
    }

    pub fn lrem(&mut self, left: bool, limit: usize, newvalue: Vec<u8>) -> usize {
        let mut count = 0;
        let mut newlist = LinkedList::new();
        match *self {
            ValueList::Data(ref mut list) => {
                if left {
                    while limit == 0 || count < limit {
                        match list.pop_front() {
                            None => break,
                            Some(el) => {
                                if el != newvalue {
                                    newlist.push_back(el);
                                } else {
                                    count += 1;
                                }
                            }
                        }
                    }
                    newlist.append(list);
                } else {
                    while limit == 0 || count < limit {
                        match list.pop_back() {
                            None => break,
                            Some(el) => {
                                if el != newvalue {
                                    newlist.push_front(el);
                                } else {
                                    count += 1;
                                }
                            }
                        }
                    }
                    // omg, ugly code, let me explain
                    // append will merge right two lists and clear the parameter
                    // newlist is the one that will survive after lrem
                    // but list needs to be at the beginning, so we are merging
                    // first to list and then to newlist
                    list.append(&mut newlist);
                    newlist.append(list);
                }
            }
        };
        *self = ValueList::Data(newlist);
        count
    }

    pub fn lset(&mut self, index: i64, newvalue: Vec<u8>) -> Result<(), OperationError> {
        match *self {
            ValueList::Data(ref mut list) => {
                let i = match normalize_position(index, list.len()) {
                    Ok(i) => i,
                    Err(_) => return Err(OperationError::OutOfBoundsError),
                };
                // this unwrap is safe because `i` is already validated to be inside the list
                let el = list.iter_mut().nth(i).unwrap();
                *el = newvalue;
                Ok(())
            }
        }
    }

    pub fn ltrim(&mut self, _start: i64, _stop: i64) -> Result<(), OperationError> {
        let list = match *self {
            ValueList::Data(ref mut list) => {
                let len = list.len();
                let start = match normalize_position(_start, len) {
                    Ok(i) => i,
                    Err(g) => {
                        if !g {
                            0
                        } else {
                            list.clear();
                            return Ok(());
                        }
                    }
                };
                let stop = match normalize_position(_stop, len) {
                    Ok(i) => i,
                    Err(g) => {
                        if !g {
                            list.clear();
                            return Ok(());
                        } else {
                            len - 1
                        }
                    }
                };
                list.split_off(stop + 1);
                list.split_off(start)
            }
        };
        *self = ValueList::Data(list);
        Ok(())
    }

    pub fn dump<T: Write>(&self, writer: &mut T) -> io::Result<usize> {
        let mut v = vec![];
        match *self {
            ValueList::Data(ref list) => {
                encode_len(list.len(), &mut v).unwrap();
                for ref item in list {
                    encode_slice_u8(&*item, &mut v, true)?;
                }
            }
        };
        let data = [
            vec![TYPE_LIST],
            v,
            vec![(VERSION & 0xff) as u8],
            vec![((VERSION >> 8) & 0xff) as u8],
        ]
        .concat();
        writer.write(&*data)
    }

    pub fn debug_object(&self) -> String {
        let mut serialized_data = vec![];
        let serialized = self.dump(&mut serialized_data).unwrap();
        let encoding = match *self {
            ValueList::Data(_) => "linkedlist",
        };

        format!(
            "Value at:0x0000000000 refcount:1 encoding:{} serializedlength:{} lru:0 \
             lru_seconds_idle:0",
            encoding, serialized
        )
    }

    /// LPOS: Find the index of the first (or nth) occurrence of an element.
    /// `rank`: 0 = first match, 1 = second match (from left), -1 = first from right, etc.
    /// `count`: max number of matches (0 = all)
    /// `maxlen`: max elements to scan (0 = all)
    /// Returns a Vec of matching indices.
    pub fn lpos(&self, element: &[u8], rank: i64, count: usize, maxlen: usize) -> Vec<i64> {
        let mut results = Vec::new();
        match *self {
            ValueList::Data(ref list) => {
                let len = list.len();
                let forward = rank >= 0;
                let mut rank_counter = if forward { rank as usize } else { (-rank) as usize };
                let mut scanned = 0usize;

                if forward {
                    for (i, item) in list.iter().enumerate() {
                        if maxlen > 0 && scanned >= maxlen { break; }
                        scanned += 1;
                        if item.as_slice() == element {
                            rank_counter -= 1;
                            if rank_counter == 0 {
                                results.push(i as i64);
                                if count > 0 && results.len() >= count { break; }
                                // Reset for next match with rank=1 semantics
                                rank_counter = 1;
                            }
                        }
                    }
                } else {
                    let items: Vec<_> = list.iter().collect();
                    for idx in (0..len).rev() {
                        if maxlen > 0 && scanned >= maxlen { break; }
                        scanned += 1;
                        if items[idx].as_slice() == element {
                            rank_counter -= 1;
                            if rank_counter == 0 {
                                results.push(idx as i64);
                                if count > 0 && results.len() >= count { break; }
                                rank_counter = 1;
                            }
                        }
                    }
                }
            }
        }
        results
    }

    /// Pop from left or right and push to another list's left or right.
    /// Returns the moved element, or None if source is empty.
    pub fn pop_push(&mut self, src_right: bool, dst: &mut ValueList, dst_right: bool) -> Option<Vec<u8>> {
        let el = self.pop(src_right)?;
        dst.push(el.clone(), dst_right);
        Some(el)
    }
}

#[cfg(test)]
mod test_rdb {
    use super::ValueList;

    #[test]
    fn dump_string_list() {
        let mut v = vec![];
        let mut list = ValueList::new();
        for item in [b"a", b"b", b"c", b"d", b"e"].iter() {
            list.push(item.to_vec(), true);
        }
        list.dump(&mut v).unwrap();
        assert_eq!(v, b"\x01\x05\x01a\x01b\x01c\x01d\x01e\x07\x00".to_vec());
    }
}
