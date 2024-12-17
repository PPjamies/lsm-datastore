use crate::{load_from_json, Memtable, Metadata, SSTableSegment};
use bloom::{BloomFilter, ASMS};
use std::io::Result;

pub struct Datastore {
    pub metadata: Metadata,
    pub bloomfilter: BloomFilter,
    pub memtable: Memtable,
    pub size_threshold: u64,
}

impl Datastore {
    pub fn new() -> Self {
        let metadata_path: String = String::from("src/metadata/metadata.json");
        let false_positive_rate: f32 = 0.01;
        let number_of_elements: u32 = 1_000_000;
        let size_threshold: u64 = 10 * 1024 * 1024; //10 MB

        Datastore {
            metadata: Metadata::load_or_create(metadata_path),
            bloomfilter: BloomFilter::with_rate(false_positive_rate, number_of_elements),
            memtable: Memtable::new(),
            size_threshold,
        }
    }

    pub fn put(&mut self, key: u64, value: String) -> Result<()> {
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
        self.bloomfilter.insert(&key);
        self.memtable.put(key, value);
        Ok(())
    }

    pub fn get(&self, key: &u64) -> Result<Option<String>> {
        if self.bloomfilter.contains(&key) {
            self.memtable.get(&key).map(|value| Ok(value));
        }

        // check sstable for key
        for segment in self.metadata.segments.iter().rev() {
            if key.clone() < segment.min_key || key.clone() > segment.max_key {
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
