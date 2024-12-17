use crate::file::serialized_size;
use crate::index::sstable::SSTable;
use chrono::Utc;
use skiplist::SkipMap;
use std::io::Result;
use std::ops::Bound;

#[derive(Debug)]
pub struct Memtable {
    pub data: SkipMap<String, String>,
}

impl Memtable {
    pub const SIZE_THRESHOLD_TEN_MB: u64 = 10 * 1024 * 1024;

    pub fn new() -> Self {
        Memtable {
            data: SkipMap::new(),
        }
    }

    pub fn put(&mut self, key: String, value: String) -> Result<()> {
        if self.size()? >= Self::SIZE_THRESHOLD_TEN_MB {
            self.flush()?;
        }
        self.data.insert(key, value);
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    pub fn delete(&mut self, key: &str) {
        self.data.insert(key.to_string(), String::from("TOMBSTONE"));
    }

    fn size(&self) -> Result<u64> {
        Ok(serialized_size(self.data.iter().collect()))?
    }

    fn flush(&mut self) -> Result<()> {
        let data: Vec<(String, String)> = self.data.iter().collect();

        // create file path for new SSTable
        let Some((min_key, _)) = self.data.lower_bound(Bound::Included(&"a".to_string()));
        let Some((max_key, _)) = self.data.upper_bound(Bound::Included(&"z".to_string()));
        let timestamp: i64 = Utc::now().timestamp_millis();
        let path: String = format!("sstable_{}_{}_{}", min_key, max_key, timestamp);

        let size: u64 = self.size()?;
        let mut sstable = SSTable::new(
            path,
            min_key.clone(),
            max_key.clone(),
            data,
            size,
            timestamp,
        );
        sstable.save()?;

        // reset the Memtable
        self.data.clear();

        Ok(())
    }
}
