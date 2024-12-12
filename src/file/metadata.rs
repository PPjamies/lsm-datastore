use serde::{Deserialize, Serialize};
use std::fs::{exists, File, OpenOptions};
use std::io::{BufReader, Read, Result, Write};
use std::path::Path;

#[derive(Serialize, Deserialize)]
pub struct SegmentMetadata {
    pub id: u32,
    pub size: u64,
    pub compacted: bool,
    pub timestamp: i64,
}

#[derive(Serialize, Deserialize)]
pub struct Metadata {
    pub segments: Vec<SegmentMetadata>,
    pub last_segment_id: u32,
}

impl Metadata {
    pub fn new() -> Self {
        Metadata {
            segments: Vec::new(),
            last_segment_id: 0,
        }
    }

    /// creates or loads metadata
    /// input (path: metadata file directory)
    pub fn load(path: &str) -> Result<Self> {
        if exists(Path::new(path)) {
            let file: File = File::open(path)?;
            let mut reader: BufReader<File> = BufReader::new(file);

            let mut content: String = String::new();
            reader.read_to_string(&mut content)?;

            let metadata: Metadata = serde_json::from_str(&content)?;

            Ok(metadata)
        } else {
            Ok(Self::new())
        }
    }

    /// adds a segment to metadata
    pub fn add_segment(&mut self, segment: SegmentMetadata) {
        self.segments.push(segment);
    }

    /// overwrite metadata file on disk
    /// input (path: metadata file directory)
    pub fn save(&self, path: &str) -> Result<()> {
        let data: String = serde_json::to_string_pretty(self)?;
        let mut file: File = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.write_all(data.as_bytes())?;
        file.sync_all()?;

        Ok(())
    }

    /// output (segment_id: u32)
    pub(crate) fn generate_segment_id(&mut self) -> u32 {
        self.last_segment_id += 1;
        self.last_segment_id
    }
}
