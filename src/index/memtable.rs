use crate::index::sstable::SSTableIndex;
use skiplist::SkipMap;
use std::fs::File;
use std::io::{Error, ErrorKind, Result};

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

    pub fn flush(&self, file_path: &str, index: &mut SSTableIndex) -> Result<(String, String)> {
        let mut file = File::create(file_path)?;

        let mut keys: Vec<String> = self.data.keys().cloned().collect();
        keys.sort();

        let min_key = keys.first().cloned().unwrap_or_else(|| String::new());
        let max_key = keys.last().cloned().unwrap_or_else(|| String::new());

        for (key, value) in &self.data {
            writeln!(file, "{},{}", key, value)?;
        }
        file.sync_all()?;

        index.add_sstable(
            file_path.to_string(),
            min_key.to_string(),
            max_key.to_string(),
        );

        Ok((min_key, max_key))
    }

    pub fn size(&self) -> Result<u64> {
        let size: u64 =
            bincode::serialized_size(&self.data).map_err(|e| Error::new(ErrorKind::Other, e))?;
        Ok(size)
    }
}
