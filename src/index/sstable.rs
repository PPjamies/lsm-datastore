use skiplist::SkipMap;
use std::io::Result;

#[derive(Debug)]
pub struct SSTable {
    pub path: String,
    pub data: SkipMap<String, String>,
}

impl SSTable {
    pub fn read(&self, key: &str) -> Result<()> {
        Ok(())
    }

    pub fn scan_range(&self, start_key: &str, end_key: &str) -> Result<()> {
        Ok(())
    }

    pub fn get_range(&self) -> Result<()> {
        Ok(())
    }

    pub fn merge(&mut self, other_sstable: &SSTable) -> Result<()> {
        Ok(())
    }

    pub fn contains(&self, key: &str) -> Result<()> {
        Ok(())
    }
}
