use crate::file::file_handler::*;
use serde::{Deserialize, Serialize};
use std::io::Result;

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct SSTableSegment {
    pub path: String,
    pub min_key: String,
    pub max_key: String,
    pub size: u64,
    pub timestamp: i64,
    pub is_compacted: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Metadata {
    pub path: String,
    pub segments: Vec<SSTableSegment>,
}

impl Metadata {
    fn new(path: String) -> Self {
        Metadata {
            path,
            segments: Vec::new(),
        }
    }

    pub fn load_or_create(path: String) -> Self {
        match load_from_json(&path) {
            Ok(Some(metadata)) => metadata,
            Ok(None) => Self::new(path),
            Err(e) => panic!("{}", e),
        }
    }

    pub fn add_segment(&mut self, segment: SSTableSegment) {
        self.segments.push(segment);
    }

    pub fn save(&self) -> Result<()> {
        flush(&self.path, &self, true)?;
        Ok(())
    }
}
