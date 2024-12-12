use crate::file::file_handler;
use crate::file::serializer::serialize;
use crate::index::sstable::SSTableIndex;
use chrono::Utc;
use skiplist::SkipMap;
use std::io::Result;
use std::ops::Bound;

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

    pub fn flush(&self, file_path: &str, index: &mut SSTableIndex) -> Result<()> {
        let data: Vec<_> = self.data.iter().collect();

        // create file path for new sstable
        let Some((min_key, _)) = self.data.lower_bound(Bound::Included(&"a".to_string()));
        let Some((max_key, _)) = self.data.upper_bound(Bound::Included(&"z".to_string()));
        let file_path: String = format!(
            "sstable_{}_{}_{}",
            min_key,
            max_key,
            Utc::now().timestamp_millis().to_string()
        );

        file_handler::flush_to_file(&file_path, &data)?;

        Ok(())
    }

    pub fn size(&self) -> Result<u64> {
        let data: Vec<(String, String)> = self.data.iter().collect();
        let serialized_data: Vec<u8> = serialize(&data)?;
        Ok(serialized_data.len() as u64)
    }
}
