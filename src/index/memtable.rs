use crate::file::{flush, serialized_size};
use chrono::Utc;
use skiplist::SkipMap;
use std::io::Result;
use std::ops::Bound;

#[derive(Debug)]
pub struct Memtable {
    pub data: SkipMap<u64, String>,
}

impl Memtable {
    pub fn new() -> Self {
        Memtable {
            data: SkipMap::new(),
        }
    }

    pub fn put(&mut self, key: u64, value: String) {
        self.data.insert(key, value);
    }

    pub fn get(&self, key: &u64) -> Option<&String> {
        self.data.get(key)
    }

    pub fn delete(&mut self, key: &u64) {
        self.data.insert(key.clone(), String::from("TOMBSTONE"));
    }

    pub fn contains(&self, key: &u64) -> bool {
        self.data.contains_key(key)
    }

    pub fn size(&self) -> Result<u64> {
        Ok(serialized_size(self.data.iter().collect()))?
    }

    pub fn flush(&mut self) -> Result<(String, u64, u64, Vec<(u64, String)>, u64, i64)> {
        let data: Vec<(u64, String)> = self.data.iter().collect();

        let size: u64 = self.size()?;

        // create file path for new SSTable
        let Some((min_key, _)) = self.data.lower_bound(Bound::Included(&u64::MIN));
        let Some((max_key, _)) = self.data.upper_bound(Bound::Included(&u64::MAX));
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
}
