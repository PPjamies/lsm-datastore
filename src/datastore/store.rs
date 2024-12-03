use crate::datastore::data::DBData;
use crate::datastore::DBConfig;
use crate::datastore::DBIndex;

use crate::datastore::index::Operation;
use crate::datastore::indexable::Indexable;
use crate::fileutil::log_handler::{read, restore, scan, write};
use chrono::Utc;
use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result};

#[derive(Debug)]
pub struct IndexBucket {
    pub offset: u64,
    pub length: usize,
}

#[derive(Debug)]
pub struct DBStore {
    pub config: DBConfig,
    pub indexes: HashMap<String, IndexBucket>,
}

impl DBStore {
    pub fn new(config: DBConfig) -> Self {
        Self {
            config,
            indexes: HashMap::new(),
        }
    }

    pub fn put(&mut self, data: DBData) {
        let (offset, length, timestamp) = write(&self.config.log_path_db, &data).unwrap();

        if self.indexes.contains_key(&data.key) {
            self.indexes
                .insert(data.key.clone(), IndexBucket { offset, length });

            write(
                &self.config.log_path_index,
                DBIndex::new(
                    data.key.clone(),
                    offset,
                    length,
                    Operation::UPDATE,
                    timestamp
                )
            ).expect("Unable to write to index log");
        }
    }

    pub fn get(&self, key: &str) -> Result<DBData> {
        if self.indexes.contains_key(&key) {
            let offset: u64 = self.indexes.get(&key).unwrap().offset;
            let length: usize = self.indexes.get(&key).unwrap().length;
            read(&self.config.log_path_db, offset, length)
        } else {
            // scan db for given key and return just the data
            match scan(&self.config.log_path_db, &key) {
                Ok(Some((data, _, _))) => Ok(data),
                Ok(None) => Err(Error::new(ErrorKind::NotFound, "Key not found in log")),
                Err(err) => Err(err),
            }
        }
    }

    pub fn create_index(&mut self, key: &str) {
        if !self.indexes.contains_key(&key) {
            // scan db for a given key > take note of offset and length of the data once found
            // add the index to in memory hashmap and add it to index log
            match scan(&self.config.log_path_index, &key) {
                Ok(Some((_, offset, length))) => {
                    self.indexes
                        .insert(key.to_string(), IndexBucket { offset, length });

                    write(
                        &self.config.log_path_index,
                        DBIndex::new(
                            key.to_string(),
                            offset,
                            length,
                            Operation::ADD,
                            Utc::now().timestamp_millis(),
                        ),
                    ).expect("Unable to write to index log");
                }
            }
        }
    }

    pub fn delete_index(&mut self, key: &str) {
        if self.indexes.contains_key(&key) {
            self.indexes.remove(&key);
            write(
                &self.config.log_path_index,
                DBIndex::new(
                    key.to_string(),
                    0,
                    0,
                    Operation::DELETE,
                    Utc::now().timestamp_millis(),
                ),
            ).expect("Unable to remove index");
        }
    }

    fn restore_indexes(&mut self) {
        let mut db_indexes: HashMap<String, IndexBucket> = HashMap::new();
        let indexes: Vec<DBIndex> = restore(&self.config.log_path_index).unwrap();
        for index in indexes {
            match index.operation() {
                Operation::ADD | Operation::UPDATE => {
                    db_indexes.insert(
                        index.key().to_string(),
                        IndexBucket {
                            offset: index.offset,
                            length: index.length,
                        },
                    );
                }
                Operation::DELETE => {
                    db_indexes.remove(index.key());
                }
            }
        }

        self.indexes = db_indexes;
    }
}
