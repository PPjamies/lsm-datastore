extern crate bloom;
use crate::{flush, serialized_size};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::{Bound, HashMap, HashSet};
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

    pub fn contains(&self, key: &u64) -> bool {
        self.data.contains_key(key)
    }

    pub fn get_key_range(&self) -> Result<(u64, u64)> {
        let min_key = self.data.keys().min().unwrap().clone();
        let max_key = self.data.keys().max().unwrap().clone();
        Ok((min_key, max_key))
    }

    pub fn size(&self) -> Result<u64> {
        Ok(serialized_size(self.data.iter().collect()))?
    }

    pub fn merge(&mut self, other_sstable: &SSTable) -> Result<()> {
        for entry in other_sstable.data.iter() {
            if entry.value() == "TOMBSTONE" {
                self.data.remove(&entry.key());
                continue;
            }
            self.data.insert(entry.key().clone(), entry.value().clone());
        }
        Ok(())
    }

    pub fn split(&mut self, threshold: u64) -> Result<(SSTable)> {
        let mut map: HashMap<u64, String> = HashMap::new();

        let mut curr_size: u64 = 0;
        for (key, val) in self.data.iter() {
            let size: u64 = serialized_size(&key)? + serialized_size(&val)?;

            if curr_size + size > threshold {
                map.insert(key.clone(), val.clone());
                self.data.remove(key);
            }
        }
        Ok(SSTable::new(map))
    }

    pub fn flush(&mut self) -> Result<(String, u64, u64, u64, i64)> {
        let data: Vec<(u64, String)> = self.data.iter().collect();
        let size: u64 = self.size()?;

        // create file path for new SSTable
        let (min_key, max_key) = self.get_key_range()?;
        let timestamp: i64 = Utc::now().timestamp_millis();
        let path: String = format!("sstable_{}_{}_{}", min_key, max_key, timestamp);

        // save sstable
        flush(&path, &data, false)?;

        Ok((path, min_key.clone(), max_key.clone(), size, timestamp))
    }
}
