use crate::file::{flush, serialized_size};
use chrono::Utc;
use skiplist::SkipMap;
use std::io::Result;
use std::ops::Bound;

#[derive(Debug)]
pub struct Memtable {
    pub data: SkipMap<String, String>,
}

impl Memtable {
    pub fn new() -> Self {
        Memtable {
            data: SkipMap::new(),
        }
    }

    pub fn put(&mut self, key: String, value: String) {
        self.data.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    pub fn delete(&mut self, key: &str) {
        self.data.insert(key.to_string(), String::from("TOMBSTONE"));
    }

    pub fn size(&self) -> Result<u64> {
        Ok(serialized_size(self.data.iter().collect()))?
    }

    pub fn flush(&mut self) -> Result<(String, String, String, Vec<(String, String)>, u64, i64)> {
        let data: Vec<(String, String)> = self.data.iter().collect();

        let size: u64 = self.size()?;

        // create file path for new SSTable
        let Some((min_key, _)) = self.data.lower_bound(Bound::Included(&"a".to_string()));
        let Some((max_key, _)) = self.data.upper_bound(Bound::Included(&"z".to_string()));
        let timestamp: i64 = Utc::now().timestamp_millis();
        let path: String = format!("sstable_{}_{}_{}", min_key, max_key, timestamp);

        // save memtable to disk
        flush(&path, &data, false)?;

        // reset the Memtable
        self.data.clear();

        Ok((
            path,
            min_key.clone(),
            max_key.clone(),
            data,
            size,
            timestamp,
        ))
    }

    pub fn snapshot(&self) {
        //TODO: Creates a snapshot of the current state for consistent reads during flush operations.
    }
}
