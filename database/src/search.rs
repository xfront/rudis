//! RediSearch-compatible full-text search module.
//!
//! Provides index management, document indexing, and basic text search.

use std::collections::HashMap;
use std::collections::HashSet;

/// Field types supported by search indexes.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldType {
    Text,
    Tag,
    Numeric,
    Geo,
}

/// A field definition in a search index schema.
#[derive(Debug, Clone)]
pub struct SchemaField {
    pub name: String,
    pub alias: Option<String>,
    pub field_type: FieldType,
    pub sortable: bool,
    pub no_index: bool,
}

/// A search index.
#[derive(Debug, Clone)]
pub struct SearchIndex {
    pub name: String,
    pub prefix: Vec<String>,
    pub schema: Vec<SchemaField>,
    /// Maps document key to indexed field values.
    pub documents: HashMap<String, HashMap<String, String>>,
    /// Inverted index: term -> set of document keys.
    pub inverted_index: HashMap<String, HashSet<String>>,
    /// Alias for this index.
    pub aliases: Vec<String>,
}

impl SearchIndex {
    pub fn new(name: &str, prefix: Vec<String>, schema: Vec<SchemaField>) -> Self {
        SearchIndex {
            name: name.to_owned(),
            prefix,
            schema,
            documents: HashMap::new(),
            inverted_index: HashMap::new(),
            aliases: Vec::new(),
        }
    }

    /// Index a document (key-value pairs).
    pub fn index_document(&mut self, key: &str, fields: HashMap<String, String>) {
        // Remove old entry if exists
        self.remove_document(key);

        // Index each field
        for schema_field in &self.schema {
            if schema_field.no_index {
                continue;
            }
            if let Some(value) = fields.get(&schema_field.name) {
                match schema_field.field_type {
                    FieldType::Text => {
                        // Tokenize and index
                        for term in tokenize(value) {
                            self.inverted_index
                                .entry(term)
                                .or_insert_with(HashSet::new)
                                .insert(key.to_owned());
                        }
                    }
                    FieldType::Tag => {
                        // Tags are separated by commas
                        for tag in value.split(',') {
                            let tag = tag.trim().to_lowercase();
                            if !tag.is_empty() {
                                self.inverted_index
                                    .entry(format!("tag:{}", tag))
                                    .or_insert_with(HashSet::new)
                                    .insert(key.to_owned());
                            }
                        }
                    }
                    FieldType::Numeric => {
                        // Store numeric value for range queries
                        self.inverted_index
                            .entry(format!("num:{}={}", schema_field.name, value))
                            .or_insert_with(HashSet::new)
                            .insert(key.to_owned());
                    }
                    FieldType::Geo => {
                        // Store geo value
                        self.inverted_index
                            .entry(format!("geo:{}={}", schema_field.name, value))
                            .or_insert_with(HashSet::new)
                            .insert(key.to_owned());
                    }
                }
            }
        }

        self.documents.insert(key.to_owned(), fields);
    }

    /// Remove a document from the index.
    pub fn remove_document(&mut self, key: &str) {
        if let Some(old_fields) = self.documents.remove(key) {
            // Remove from inverted index
            for (_field, terms) in &self.inverted_index {
                // We need to rebuild - just remove the key from all term sets
            }
            // Rebuild inverted index without this key
            let keys_to_remove: Vec<String> = self.inverted_index.iter()
                .filter(|(_, v)| v.contains(key))
                .map(|(k, _)| k.clone())
                .collect();
            for term in keys_to_remove {
                if let Some(set) = self.inverted_index.get_mut(&term) {
                    set.remove(key);
                    if set.is_empty() {
                        self.inverted_index.remove(&term);
                    }
                }
            }
        }
    }

    /// Search for documents matching the query.
    pub fn search(&self, query: &str, offset: usize, limit: usize) -> SearchResult {
        let query = query.trim();
        let matching_keys: HashSet<String> = if query == "*" {
            self.documents.keys().cloned().collect()
        } else {
            // Parse query terms and intersect
            let terms: Vec<String> = tokenize(query);
            if terms.is_empty() {
                return SearchResult { total: 0, documents: vec![] };
            }
            let mut result: Option<HashSet<String>> = None;
            for term in &terms {
                let term_lower = term.to_lowercase();
                let matches = self.inverted_index.get(&term_lower)
                    .cloned()
                    .unwrap_or_default();
                result = Some(match result {
                    Some(existing) => existing.intersection(&matches).cloned().collect(),
                    None => matches,
                });
            }
            result.unwrap_or_default()
        };

        let total = matching_keys.len();
        let documents: Vec<SearchDocument> = matching_keys.into_iter()
            .skip(offset)
            .take(limit)
            .map(|key| {
                let fields = self.documents.get(&key).cloned().unwrap_or_default();
                SearchDocument { key, fields }
            })
            .collect();

        SearchResult { total, documents }
    }

    pub fn info(&self) -> IndexInfo {
        IndexInfo {
            name: self.name.clone(),
            num_docs: self.documents.len(),
            num_terms: self.inverted_index.len(),
            num_fields: self.schema.len(),
            fields: self.schema.iter().map(|f| FieldInfo {
                name: f.name.clone(),
                field_type: format!("{:?}", f.field_type),
            }).collect(),
        }
    }
}

/// Tokenize text into lowercase terms.
fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_lowercase())
        .collect()
}

/// Result of a search query.
pub struct SearchResult {
    pub total: usize,
    pub documents: Vec<SearchDocument>,
}

/// A document in search results.
pub struct SearchDocument {
    pub key: String,
    pub fields: HashMap<String, String>,
}

/// Information about a search index.
pub struct IndexInfo {
    pub name: String,
    pub num_docs: usize,
    pub num_terms: usize,
    pub num_fields: usize,
    pub fields: Vec<FieldInfo>,
}

pub struct FieldInfo {
    pub name: String,
    pub field_type: String,
}

/// Manages all search indexes.
#[derive(Debug, Clone, Default)]
pub struct SearchEngine {
    pub indexes: HashMap<String, SearchIndex>,
    pub aliases: HashMap<String, String>,
}

impl SearchEngine {
    pub fn new() -> Self {
        SearchEngine {
            indexes: HashMap::new(),
            aliases: HashMap::new(),
        }
    }

    pub fn create_index(&mut self, name: &str, prefix: Vec<String>, schema: Vec<SchemaField>) -> Result<(), String> {
        if self.indexes.contains_key(name) {
            return Err(format!("Index already exists: {}", name));
        }
        let index = SearchIndex::new(name, prefix, schema);
        self.indexes.insert(name.to_owned(), index);
        Ok(())
    }

    pub fn drop_index(&mut self, name: &str, delete_docs: bool) -> Result<(), String> {
        if self.indexes.remove(name).is_none() {
            return Err(format!("Index not found: {}", name));
        }
        // Remove aliases
        let aliases_to_remove: Vec<String> = self.aliases.iter()
            .filter(|(_, v)| *v == name)
            .map(|(k, _)| k.clone())
            .collect();
        for alias in aliases_to_remove {
            self.aliases.remove(&alias);
        }
        Ok(())
    }

    pub fn get_index(&self, name: &str) -> Option<&SearchIndex> {
        self.indexes.get(name).or_else(|| {
            self.aliases.get(name).and_then(|real_name| self.indexes.get(real_name))
        })
    }

    pub fn get_index_mut(&mut self, name: &str) -> Option<&mut SearchIndex> {
        if self.indexes.contains_key(name) {
            self.indexes.get_mut(name)
        } else if let Some(real_name) = self.aliases.get(name).cloned() {
            self.indexes.get_mut(&real_name)
        } else {
            None
        }
    }

    pub fn add_alias(&mut self, alias: &str, index_name: &str) -> Result<(), String> {
        if !self.indexes.contains_key(index_name) {
            return Err(format!("Index not found: {}", index_name));
        }
        self.aliases.insert(alias.to_owned(), index_name.to_owned());
        Ok(())
    }

    pub fn del_alias(&mut self, alias: &str) -> Result<(), String> {
        if self.aliases.remove(alias).is_none() {
            return Err(format!("Alias not found: {}", alias));
        }
        Ok(())
    }
}

#[cfg(test)]
mod test_search {
    use super::*;

    #[test]
    fn test_create_index() {
        let mut engine = SearchEngine::new();
        let schema = vec![
            SchemaField { name: "title".to_owned(), alias: None, field_type: FieldType::Text, sortable: false, no_index: false },
        ];
        assert!(engine.create_index("idx", vec!["doc:".to_owned()], schema).is_ok());
        assert!(engine.get_index("idx").is_some());
    }

    #[test]
    fn test_index_and_search() {
        let mut engine = SearchEngine::new();
        let schema = vec![
            SchemaField { name: "title".to_owned(), alias: None, field_type: FieldType::Text, sortable: false, no_index: false },
        ];
        engine.create_index("idx", vec!["doc:".to_owned()], schema).unwrap();

        let mut fields = HashMap::new();
        fields.insert("title".to_owned(), "Hello World".to_owned());
        engine.get_index_mut("idx").unwrap().index_document("doc:1", fields);

        let mut fields2 = HashMap::new();
        fields2.insert("title".to_owned(), "Hello Rust".to_owned());
        engine.get_index_mut("idx").unwrap().index_document("doc:2", fields2);

        let result = engine.get_index("idx").unwrap().search("hello", 0, 10);
        assert_eq!(result.total, 2);

        let result = engine.get_index("idx").unwrap().search("world", 0, 10);
        assert_eq!(result.total, 1);

        let result = engine.get_index("idx").unwrap().search("*", 0, 10);
        assert_eq!(result.total, 2);
    }

    #[test]
    fn test_alias() {
        let mut engine = SearchEngine::new();
        let schema = vec![];
        engine.create_index("idx", vec![], schema).unwrap();
        assert!(engine.add_alias("myalias", "idx").is_ok());
        assert!(engine.get_index("myalias").is_some());
        assert!(engine.del_alias("myalias").is_ok());
        assert!(engine.get_index("myalias").is_none());
    }
}
