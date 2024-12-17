use crate::file::{flush, Metadata};
use serde::{Deserialize, Serialize};
use skiplist::SkipMap;
use std::io::Result;
use std::ops::Bound::Included;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableMetadata {
    pub path: String,
    pub min_key: String,
    pub max_key: String,
    pub size: u64,
    pub timestamp: i64,
    pub is_compacted: bool,
}

impl SSTableMetadata {
    pub fn new(path: String, min_key: String, max_key: String, size: u64, timestamp: i64) -> Self {
        SSTableMetadata {
            path,
            min_key,
            max_key,
            size,
            timestamp,
            is_compacted: false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SSTable {
    pub metadata: SSTableMetadata,
    pub data: Vec<(String, String)>,
    pub map: SkipMap<String, String>,
}

impl SSTable {
    pub fn new(
        path: String,
        min_key: String,
        max_key: String,
        data: Vec<(String, String)>,
        size: u64,
        timestamp: i64,
    ) -> Self {
        let mut data_map = SkipMap::new();
        for (key, value) in &data {
            data_map.insert(key.clone(), value.clone());
        }

        SSTable {
            metadata: SSTableMetadata::new(path, min_key, max_key, size, timestamp),
            data,
            map: data_map,
        }
    }

    pub fn read(&self, key: &str) -> Option<&String> {
        self.map.get(key)
    }

    pub fn scan_range(&self, start_key: &str, end_key: &str) -> Result<Vec<(String, String)>> {
        match self.map.get(start_key) {
            Some(map) => Ok(map
                .range((Included(start_key)), Included(end_key))
                .map(|entry| (entry.key().clone(), entry.value().clone()))
                .collect()),
            None => Ok(None),
        }
    }

    pub fn get_range(&self) -> Result<(&String, &String)> {
        let Some((min_key, _)) = self.map.lower_bound(Included(&"a".to_string()));
        let Some((max_key, _)) = self.map.upper_bound(Included(&"z".to_string()));
        Ok((min_key, max_key))
    }

    pub fn contains(&self, key: &str) -> bool {
        self.map.contains_key(key)
    }

    pub fn merge(&mut self, other_sstable: &SSTable) -> Result<()> {
        for entry in other_sstable.map.iter() {
            self.map.insert(entry.key().clone(), entry.value().clone());
        }
        Ok(())
    }

    pub fn save(&mut self) -> Result<()> {
        // save sstable to disk
        flush(&self.metadata.path, &self.data, false)?;

        // update + save metadata to disk
        let mut metadata = Metadata::load_or_create()?;
        metadata.add_metadata(self.metadata.clone());
        metadata.save()?;

        Ok(())
    }
}
