//! JSON data structure for RedisJSON compatibility.
//!
//! Wraps serde_json::Value and provides JSONPath-like access.

use serde_json;

/// A JSON value stored in the database.
#[derive(Debug, Clone, PartialEq)]
pub struct ValueJson {
    pub value: serde_json::Value,
}

impl ValueJson {
    pub fn new(value: serde_json::Value) -> Self {
        ValueJson { value }
    }

    pub fn from_str(s: &str) -> Result<Self, String> {
        let value = serde_json::from_str(s).map_err(|e| format!("ERR invalid JSON: {}", e))?;
        Ok(ValueJson { value })
    }

    /// Get the JSON type name.
    pub fn json_type(&self) -> &str {
        match &self.value {
            serde_json::Value::Null => "null",
            serde_json::Value::Bool(_) => "boolean",
            serde_json::Value::Number(_) => "number",  // could be "integer" or "number"
            serde_json::Value::String(_) => "string",
            serde_json::Value::Array(_) => "array",
            serde_json::Value::Object(_) => "object",
        }
    }

    /// Get the string length.
    pub fn strlen(&self) -> Result<usize, String> {
        match &self.value {
            serde_json::Value::String(s) => Ok(s.len()),
            _ => Err("ERR wrong type".to_owned()),
        }
    }

    /// Get the number of elements (array length or object keys count).
    pub fn objlen(&self) -> Result<usize, String> {
        match &self.value {
            serde_json::Value::Object(o) => Ok(o.len()),
            serde_json::Value::Array(a) => Ok(a.len()),
            _ => Err("ERR wrong type".to_owned()),
        }
    }

    /// Get object keys.
    pub fn objkeys(&self) -> Result<Vec<String>, String> {
        match &self.value {
            serde_json::Value::Object(o) => Ok(o.keys().cloned().collect()),
            _ => Err("ERR wrong type".to_owned()),
        }
    }
}

/// Parse a JSON path into components.
/// Supports: ".", "$", ".field", "[index]", ".field.subfield"
pub fn parse_json_path(path: &str) -> Vec<PathComponent> {
    let mut components = Vec::new();
    if path.is_empty() || path == "$" || path == "." {
        return components;
    }

    let path = if path.starts_with('$') { &path[1..] } else { path };
    let path = if path.starts_with('.') { &path[1..] } else { path };

    if path.is_empty() {
        return components;
    }

    let mut chars = path.chars().peekable();
    let mut current_key = String::new();

    while let Some(&ch) = chars.peek() {
        match ch {
            '.' => {
                if !current_key.is_empty() {
                    components.push(PathComponent::Key(current_key.clone()));
                    current_key.clear();
                }
                chars.next();
            }
            '[' => {
                if !current_key.is_empty() {
                    components.push(PathComponent::Key(current_key.clone()));
                    current_key.clear();
                }
                chars.next(); // consume '['
                let mut index_str = String::new();
                while let Some(&c) = chars.peek() {
                    if c == ']' {
                        chars.next();
                        break;
                    }
                    index_str.push(c);
                    chars.next();
                }
                if let Ok(idx) = index_str.parse::<i64>() {
                    components.push(PathComponent::Index(idx));
                } else {
                    // It's a quoted key like ["field"]
                    let key = index_str.trim_matches('"').trim_matches('\'').to_owned();
                    components.push(PathComponent::Key(key));
                }
            }
            _ => {
                current_key.push(ch);
                chars.next();
            }
        }
    }
    if !current_key.is_empty() {
        components.push(PathComponent::Key(current_key));
    }

    components
}

/// A component of a JSON path.
#[derive(Debug, Clone)]
pub enum PathComponent {
    Key(String),
    Index(i64),
}

/// Navigate to a value by path, returning a reference.
pub fn json_get<'a>(root: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let components = parse_json_path(path);
    let mut current = root;
    for comp in &components {
        match comp {
            PathComponent::Key(k) => {
                match current {
                    serde_json::Value::Object(o) => {
                        current = o.get(k)?;
                    }
                    _ => return None,
                }
            }
            PathComponent::Index(idx) => {
                match current {
                    serde_json::Value::Array(a) => {
                        let i = if *idx < 0 {
                            (a.len() as i64 + idx) as usize
                        } else {
                            *idx as usize
                        };
                        current = a.get(i)?;
                    }
                    _ => return None,
                }
            }
        }
    }
    Some(current)
}

/// Navigate to a mutable value by path.
pub fn json_get_mut<'a>(root: &'a mut serde_json::Value, path: &str) -> Option<&'a mut serde_json::Value> {
    let components = parse_json_path(path);
    let mut current = root;
    for comp in &components {
        match comp {
            PathComponent::Key(k) => {
                match current {
                    serde_json::Value::Object(o) => {
                        current = o.get_mut(k)?;
                    }
                    _ => return None,
                }
            }
            PathComponent::Index(idx) => {
                match current {
                    serde_json::Value::Array(a) => {
                        let i = if *idx < 0 {
                            (a.len() as i64 + idx) as usize
                        } else {
                            *idx as usize
                        };
                        current = a.get_mut(i)?;
                    }
                    _ => return None,
                }
            }
        }
    }
    Some(current)
}

/// Set a value at the given path, creating intermediate objects/arrays as needed.
pub fn json_set(root: &mut serde_json::Value, path: &str, new_value: serde_json::Value) -> bool {
    let components = parse_json_path(path);
    if components.is_empty() {
        *root = new_value;
        return true;
    }

    let mut current = root;
    for (i, comp) in components.iter().enumerate() {
        if i == components.len() - 1 {
            // Last component: set the value
            match comp {
                PathComponent::Key(k) => {
                    if let serde_json::Value::Object(o) = current {
                        o.insert(k.clone(), new_value);
                        return true;
                    }
                    return false;
                }
                PathComponent::Index(idx) => {
                    if let serde_json::Value::Array(a) = current {
                        let i = if *idx < 0 {
                            (a.len() as i64 + idx) as usize
                        } else {
                            *idx as usize
                        };
                        if i < a.len() {
                            a[i] = new_value;
                            return true;
                        }
                    }
                    return false;
                }
            }
        } else {
            // Navigate deeper
            match comp {
                PathComponent::Key(k) => {
                    if let serde_json::Value::Object(o) = current {
                        if !o.contains_key(k.as_str()) {
                            // Create intermediate object
                            o.insert(k.clone(), serde_json::Value::Object(serde_json::Map::new()));
                        }
                        current = o.get_mut(k).unwrap();
                    } else {
                        return false;
                    }
                }
                PathComponent::Index(idx) => {
                    if let serde_json::Value::Array(a) = current {
                        let i = if *idx < 0 {
                            (a.len() as i64 + idx) as usize
                        } else {
                            *idx as usize
                        };
                        if i < a.len() {
                            current = &mut a[i];
                        } else {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
            }
        }
    }
    false
}

/// Delete a value at the given path. Returns true if deleted.
pub fn json_del(root: &mut serde_json::Value, path: &str) -> bool {
    let components = parse_json_path(path);
    if components.is_empty() {
        *root = serde_json::Value::Null;
        return true;
    }

    let mut current = root;
    for (i, comp) in components.iter().enumerate() {
        if i == components.len() - 1 {
            match comp {
                PathComponent::Key(k) => {
                    if let serde_json::Value::Object(o) = current {
                        return o.remove(k).is_some();
                    }
                    return false;
                }
                PathComponent::Index(idx) => {
                    if let serde_json::Value::Array(a) = current {
                        let i = if *idx < 0 {
                            (a.len() as i64 + idx) as usize
                        } else {
                            *idx as usize
                        };
                        if i < a.len() {
                            a.remove(i);
                            return true;
                        }
                    }
                    return false;
                }
            }
        } else {
            match comp {
                PathComponent::Key(k) => {
                    if let serde_json::Value::Object(o) = current {
                        if let Some(next) = o.get_mut(k) {
                            current = next;
                        } else {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                PathComponent::Index(idx) => {
                    if let serde_json::Value::Array(a) = current {
                        let i = if *idx < 0 {
                            (a.len() as i64 + idx) as usize
                        } else {
                            *idx as usize
                        };
                        if i < a.len() {
                            current = &mut a[i];
                        } else {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
            }
        }
    }
    false
}

/// Increment a number at the given path.
pub fn json_numincrby(root: &mut serde_json::Value, path: &str, increment: f64) -> Result<f64, String> {
    match json_get_mut(root, path) {
        Some(val) => {
            match val {
                serde_json::Value::Number(n) => {
                    let current = n.as_f64().ok_or("ERR not a number")?;
                    let new_val = current + increment;
                    *val = serde_json::json!(new_val);
                    Ok(new_val)
                }
                _ => Err("ERR wrong type".to_owned()),
            }
        }
        None => Err("ERR path not found".to_owned()),
    }
}

/// Multiply a number at the given path.
pub fn json_nummultby(root: &mut serde_json::Value, path: &str, multiplier: f64) -> Result<f64, String> {
    match json_get_mut(root, path) {
        Some(val) => {
            match val {
                serde_json::Value::Number(n) => {
                    let current = n.as_f64().ok_or("ERR not a number")?;
                    let new_val = current * multiplier;
                    *val = serde_json::json!(new_val);
                    Ok(new_val)
                }
                _ => Err("ERR wrong type".to_owned()),
            }
        }
        None => Err("ERR path not found".to_owned()),
    }
}

/// Append a string to a string value at the given path.
pub fn json_strappend(root: &mut serde_json::Value, path: &str, append: &str) -> Result<usize, String> {
    match json_get_mut(root, path) {
        Some(val) => {
            match val {
                serde_json::Value::String(s) => {
                    s.push_str(append);
                    Ok(s.len())
                }
                _ => Err("ERR wrong type".to_owned()),
            }
        }
        None => Err("ERR path not found".to_owned()),
    }
}

/// Append to a JSON array at the given path. Returns new length.
pub fn json_arrappend(root: &mut serde_json::Value, path: &str, values: Vec<serde_json::Value>) -> Result<usize, String> {
    match json_get_mut(root, path) {
        Some(val) => {
            match val {
                serde_json::Value::Array(a) => {
                    for v in values {
                        a.push(v);
                    }
                    Ok(a.len())
                }
                _ => Err("ERR wrong type".to_owned()),
            }
        }
        None => Err("ERR path not found".to_owned()),
    }
}

/// Get the length of a JSON array at the given path.
pub fn json_arrlen(root: &serde_json::Value, path: &str) -> Result<usize, String> {
    match json_get(root, path) {
        Some(val) => {
            match val {
                serde_json::Value::Array(a) => Ok(a.len()),
                _ => Err("ERR wrong type".to_owned()),
            }
        }
        None => Err("ERR path not found".to_owned()),
    }
}

/// Pop from a JSON array at the given path.
pub fn json_arrpop(root: &mut serde_json::Value, path: &str, index: i64) -> Result<serde_json::Value, String> {
    match json_get_mut(root, path) {
        Some(val) => {
            match val {
                serde_json::Value::Array(a) => {
                    if a.is_empty() {
                        return Err("ERR array is empty".to_owned());
                    }
                    let i = if index < 0 {
                        (a.len() as i64 + index).max(0) as usize
                    } else {
                        (index as usize).min(a.len() - 1)
                    };
                    Ok(a.remove(i))
                }
                _ => Err("ERR wrong type".to_owned()),
            }
        }
        None => Err("ERR path not found".to_owned()),
    }
}

/// Insert into a JSON array at the given path.
pub fn json_arrinsert(root: &mut serde_json::Value, path: &str, index: i64, values: Vec<serde_json::Value>) -> Result<usize, String> {
    match json_get_mut(root, path) {
        Some(val) => {
            match val {
                serde_json::Value::Array(a) => {
                    let i = if index < 0 {
                        (a.len() as i64 + index).max(0) as usize
                    } else {
                        (index as usize).min(a.len())
                    };
                    for (offset, v) in values.into_iter().enumerate() {
                        a.insert(i + offset, v);
                    }
                    Ok(a.len())
                }
                _ => Err("ERR wrong type".to_owned()),
            }
        }
        None => Err("ERR path not found".to_owned()),
    }
}

/// Find the index of a value in a JSON array.
pub fn json_arrindex(root: &serde_json::Value, path: &str, search: &serde_json::Value) -> Result<i64, String> {
    match json_get(root, path) {
        Some(val) => {
            match val {
                serde_json::Value::Array(a) => {
                    for (i, item) in a.iter().enumerate() {
                        if item == search {
                            return Ok(i as i64);
                        }
                    }
                    Ok(-1)
                }
                _ => Err("ERR wrong type".to_owned()),
            }
        }
        None => Err("ERR path not found".to_owned()),
    }
}

/// Trim a JSON array.
pub fn json_arrtrim(root: &mut serde_json::Value, path: &str, start: i64, stop: i64) -> Result<usize, String> {
    match json_get_mut(root, path) {
        Some(val) => {
            match val {
                serde_json::Value::Array(a) => {
                    let len = a.len() as i64;
                    let s = if start < 0 { (len + start).max(0) as usize } else { (start as usize).min(a.len()) };
                    let e = if stop < 0 { (len + stop).max(0) as usize } else { (stop as usize).min(a.len()) };
                    if s >= e || s >= a.len() {
                        a.clear();
                        return Ok(0);
                    }
                    let trimmed: Vec<_> = a.drain(s..e).collect();
                    a.clear();
                    a.extend(trimmed);
                    Ok(a.len())
                }
                _ => Err("ERR wrong type".to_owned()),
            }
        }
        None => Err("ERR path not found".to_owned()),
    }
}

/// Merge JSON at a path (RFC 7396 JSON Merge Patch).
pub fn json_merge(root: &mut serde_json::Value, path: &str, patch: serde_json::Value) -> bool {
    match json_get_mut(root, path) {
        Some(val) => {
            merge_json(val, patch);
            true
        }
        None => false,
    }
}

fn merge_json(target: &mut serde_json::Value, patch: serde_json::Value) {
    match (target.is_object(), patch.is_object()) {
        (true, true) => {
            if let serde_json::Value::Object(patch_obj) = patch {
                if let serde_json::Value::Object(target_obj) = target {
                    for (key, value) in patch_obj {
                        if value.is_null() {
                            target_obj.remove(&key);
                        } else {
                            let entry = target_obj.entry(key).or_insert(serde_json::Value::Null);
                            merge_json(entry, value);
                        }
                    }
                }
            }
        }
        _ => {
            *target = patch;
        }
    }
}

#[cfg(test)]
mod test_json {
    use super::*;

    #[test]
    fn test_parse_path() {
        let components = parse_json_path("$.foo.bar");
        assert_eq!(components.len(), 2);
        assert!(matches!(&components[0], PathComponent::Key(k) if k == "foo"));
        assert!(matches!(&components[1], PathComponent::Key(k) if k == "bar"));
    }

    #[test]
    fn test_parse_path_array() {
        let components = parse_json_path("$.arr[0]");
        assert_eq!(components.len(), 2);
        assert!(matches!(&components[0], PathComponent::Key(k) if k == "arr"));
        assert!(matches!(&components[1], PathComponent::Index(0)));
    }

    #[test]
    fn test_json_get_set() {
        let mut root = serde_json::json!({"foo": {"bar": 42}});
        let val = json_get(&root, "$.foo.bar").unwrap();
        assert_eq!(val, &serde_json::json!(42));

        json_set(&mut root, "$.foo.bar", serde_json::json!(100));
        let val = json_get(&root, "$.foo.bar").unwrap();
        assert_eq!(val, &serde_json::json!(100));
    }

    #[test]
    fn test_json_del() {
        let mut root = serde_json::json!({"foo": {"bar": 42, "baz": 43}});
        assert!(json_del(&mut root, "$.foo.bar"));
        assert_eq!(root, serde_json::json!({"foo": {"baz": 43}}));
    }

    #[test]
    fn test_json_numincrby() {
        let mut root = serde_json::json!({"a": 10});
        let result = json_numincrby(&mut root, "$.a", 5.0).unwrap();
        assert_eq!(result, 15.0);
    }

    #[test]
    fn test_json_arrappend() {
        let mut root = serde_json::json!({"arr": [1, 2]});
        let len = json_arrappend(&mut root, "$.arr", vec![serde_json::json!(3)]).unwrap();
        assert_eq!(len, 3);
    }
}
