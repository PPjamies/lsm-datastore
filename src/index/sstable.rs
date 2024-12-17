extern crate bloom;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Result;

#[derive(Debug, Serialize, Deserialize)]
pub struct SSTable {
    pub data: HashMap<u64, String>,
}

impl SSTable {
    pub fn new(data: HashMap<u64, String>) -> Self {
        SSTable { data }
    }

    pub fn scan(&self, start: &u64, end: &u64) -> Result<Option<Vec<(u64, String)>>> {
        if (!self.contains(start) || !self.contains(end)) {
            return Ok(None);
        }

        let mut result = Vec::new();
        for (key, value) in self.data.iter() {
            if key >= start && key <= end {
                result.push((key.clone(), value.clone()));
            }
        }
        Ok(Some(result))
    }

    pub fn read(&self, key: &u64) -> Option<&String> {
        self.data.get(key)
    }

    pub fn read_block(&self, key: &str, offset: u64, length: usize) {
        // TODO: Reads a specific block of data using an offset.
    }

    pub fn write_block(&mut self, key: &str, data: String) {
        // TODO: Writes a block of data to the SSTable.
    }

    pub fn contains(&self, key: &u64) -> bool {
        self.data.contains_key(key)
    }

    pub fn get_key_range(&self) -> Result<(u64, u64)> {
        let min_key = self.data.keys().min().unwrap().clone();
        let max_key = self.data.keys().max().unwrap().clone();
        Ok((min_key, max_key))
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
