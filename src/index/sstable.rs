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

    pub fn scan() {
        // TODO: retrieves all key/val pairs in the specified key range
    }

    pub fn read(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    pub fn read_block(&self, key: &str, offset: u64, length: usize) {
        // TODO: Reads a specific block of data using an offset.
    }

    pub fn write_block(&mut self, key: &str, data: String) {
        // TODO: Writes a block of data to the SSTable.
    }

    // TODO: optimize using bloom filter
    pub fn contains(&self, key: &str) -> bool {
        self.data.contains_key(key)
    }

    pub fn get_range() {
        // TODO: returns min/max key
    }

    pub fn merge(&mut self, other_sstable: &SSTable) -> Result<()> {
        for entry in other_sstable.data.iter() {
            self.data.insert(entry.key().clone(), entry.value().clone());
        }
        Ok(())
    }

    pub fn create_index() {
        // TODO: builds an on-disk index
    }
}
