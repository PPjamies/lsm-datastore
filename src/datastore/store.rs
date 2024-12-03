use crate::datastore::data::DBData;
use crate::datastore::DBConfig;
use crate::datastore::DBIndex;

use crate::datastore::index::Operation;
use crate::fileutil::log_handler::write;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Result};

pub struct MemIndexBucket {
    pub offset: u64,
    pub length: usize,
}

pub struct DBStore {
    pub config: DBConfig,
    pub indexes: HashMap<String, MemIndexBucket>,
}

impl DBStore {
    pub fn new(config: DBConfig) -> Self {
        Self {
            config,
            indexes: HashMap::new(),
        }
    }

    pub fn put(&mut self, data: DBData) {
        let (offset, length) = write(&self.config.log_path_db, &data).unwrap();

        if (self.index_exists(&data.key)) {
            self.indexes
                .insert(data.key, MemIndexBucket { offset, length });

            write(
                &self.config.log_path_index,
                DBIndex::new(&data.key, offset, length, Operation::UPDATE),
            );
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<DBData>> {
        if (self.index_exists(key)) {
            // todo: use byte offset to grab data from log file
        } else {
            // todo: scan log file
        }
    }

    pub fn set_index(&mut self, key: &str) {
        if (self.index_exists(key)) {
        } else {
            // scan log file for key
            // take note of offset in bytes
            // store key, offset, length in index file
            // store key and offset in hashmap
        }
    }

    pub fn remove_index(&mut self, key: &str) {
        if (self.index_exists(key)) {
            // todo: remove from hashmap
            // todo: add a (key, offset, length, DELETE) to the index log
        }
    }

    fn index_exists(&self, key: &str) -> bool {
        self.indexes.contains_key(key)
    }

    fn restore_indexes(&mut self) -> Result<()> {
        let log_path_indexes: &str = self.get_config().get_log_path_db();

        // move this logic to long handler
        let file: File = File::open(log_path_indexes)?;
        let reader: BufReader<File> = BufReader::new(file);

        let indexes: Vec<DBIndex> = bincode::deserialize_from(reader)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        // iterate res and restore hashmap
        let mut db_indexes: HashMap<String, String> = HashMap::new();
        for index in indexes {
            match index.get_operation() {
                Operation::ADD | Operation::UPDATE => {
                    db_indexes.insert(index.get_key().clone(), index.get_offset().clone());
                }
                Operation::DELETE => {
                    db_indexes.remove(index.get_key());
                }
            }
        }

        self.indexes = db_indexes;

        Ok(())
    }
}
