use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Result;

#[derive(Debug, Serialize, Deserialize)]
pub struct SSTable {
    pub data: HashMap<String, String>,
}

impl SSTable {
    pub fn new(data: HashMap<String, String>) -> Self {
        SSTable { data }
    }

    pub fn read(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    pub fn contains(&self, key: &str) -> bool {
        self.data.contains_key(key)
    }

    pub fn merge(&mut self, other_sstable: &SSTable) -> Result<()> {
        for entry in other_sstable.data.iter() {
            self.data.insert(entry.key().clone(), entry.value().clone());
        }
        Ok(())
    }
}
