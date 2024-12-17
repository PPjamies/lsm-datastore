use crate::file::file_handler::*;
use serde::{Deserialize, Serialize};
use std::io::Result;

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct SSTableSegment {
    pub path: String,
    pub min_key: u64,
    pub max_key: u64,
    pub size: u64,
    pub timestamp: i64,
    pub is_compacted: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Metadata {
    pub metadata_path: String,
    pub database_recovery_path: String,
    pub segments: Vec<SSTableSegment>,
}

impl Metadata {
    fn new(metadata_path: String, database_recovery_path: String) -> Self {
        Metadata {
            metadata_path,
            database_recovery_path,
            segments: Vec::new(),
        }
    }

    pub fn load_or_create(metadata_path: String, database_recovery_path: String) -> Self {
        match load_from_json(&metadata_path) {
            Ok(Some(metadata)) => metadata,
            Ok(None) => Self::new(metadata_path, database_recovery_path),
            Err(e) => panic!("{}", e),
        }
    }

    pub fn add_segment(&mut self, segment: SSTableSegment) {
        self.segments.push(segment);
    }

    pub fn save(&self) -> Result<()> {
        flush(&self.metadata_path, &self, true)?;
        Ok(())
    }
}
