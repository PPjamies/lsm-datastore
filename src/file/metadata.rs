use crate::file::file_handler::*;
use serde::{Deserialize, Serialize};
use std::io::Result;
use crate::index::SSTableMetadata;

#[derive(Serialize, Deserialize)]
pub struct Metadata {
    pub metadatas: Vec<SSTableMetadata>,
}

impl Metadata {
    const METADATA_PATH: String = String::from("src/metadata/metadata.json");

    fn new() -> Self {
        Metadata {
            metadatas: Vec::new(),
        }
    }

    pub fn load_or_create() -> Result<Self> {
        match load_from_json(&Self::METADATA_PATH) {
            Ok(metadata) => Ok(metadata),
            None => Ok(Self::new()),
        }
    }

    pub fn add_metadata(&mut self, sstable_metadata: SSTableMetadata) {
        self.metadatas.push(sstable_metadata);
    }

    pub fn save(&self) -> Result<()> {
        flush(&Self::METADATA_PATH, self, true)?;
        Ok(())
    }
}
