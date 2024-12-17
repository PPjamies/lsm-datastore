use crate::{load_from_json, Memtable, Metadata, SSTableSegment};
use std::io::Result;

#[derive(Debug)]
pub struct Datastore {
    pub metadata: Metadata,
    pub memtable: Memtable,
    pub size_threshold: u64,
}

impl Datastore {
    pub fn new() -> Self {
        Datastore {
            metadata: Metadata::load_or_create(String::from("src/metadata/metadata.json")),
            memtable: Memtable::new(),
            size_threshold: 10 * 1024 * 1024, //10 MB
        }
    }

    pub fn put(&mut self, key: String, value: String) -> Result<()> {
        let size: u64 = self.memtable.size()?;
        if size >= self.size_threshold {
            // store memtable to disk
            let (path, min_key, max_key, data, size, timestamp) = self.memtable.flush()?;
            // update + store metadata to disk
            self.metadata.add_segment(SSTableSegment {
                path,
                min_key,
                max_key,
                size,
                timestamp,
                is_compacted: false,
            });
            self.metadata.save()?;
        }
        self.memtable.put(key, value);
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<String>> {
        // check memtable for key
        self.memtable.get(&key).map(|value| Ok(value));

        // check sstable for key
        for segment in self.metadata.segments.iter().rev() {
            if key.to_string() < segment.min_key || key.to_string() > segment.max_key {
                continue;
            }

            match load_from_json(&segment.path) {
                Ok(Some(sstable)) => {
                    if !sstable.contains(&key) {
                        break;
                    }
                    return Ok(Some(sstable.read(&key).unwrap()));
                }
            }
        }
    }
}
