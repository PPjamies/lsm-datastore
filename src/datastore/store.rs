use crate::datastore::data::DBData;
use crate::datastore::DBConfig;
use crate::datastore::DBIndex;

use crate::datastore::indexable::Indexable;
use crate::datastore::operation::Operation;
use crate::file::log_handler::{read, restore, scan, write};
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
        match write(&self.config.log_path_db, &data) {
            Ok((offset, length)) => {
                if !self.indexes.contains_key(&data.key) {
                    return;
                }
                // user has an index on this key so update the index
                self.update_index(data.key(), offset, length, Operation::UPDATE);
            }
            Err(e) => panic!("{}", e),
        }
    }

    pub fn get(&self, key: &str) -> Result<DBData> {
        match self.indexes.get(key) {
            Some(data) => {
                // read from byte offset
                read(&self.config.log_path_db, data.offset, data.length)
            }
            None => {
                // scan entire database
                match scan::<DBData>(&self.config.log_path_db, &key) {
                    Ok(Some((data, _, _))) => Ok(data),
                    Ok(None) => Err(Error::new(ErrorKind::NotFound, "Key not found in log")),
                    Err(err) => Err(err),
                }
            }
        }
    }

    pub fn create_index(&mut self, key: &str) -> Result<()> {
        if self.indexes.contains_key(key) {
            return Err(Error::new(ErrorKind::AlreadyExists, "Key already exists"));
        }

        match scan::<DBData>(&self.config.log_path_index, &key) {
            Ok(Some((data, offset, length))) => {
                Ok(self.update_index(data.key(), offset, length, Operation::ADD))
            }
            Ok(None) => Err(Error::new(
                ErrorKind::NotFound,
                "Data not found in database, unable to index key.",
            )),
            Err(err) => Err(err),
        }
    }

    pub fn delete_index(&mut self, key: &str) -> Result<()> {
        if !self.indexes.contains_key(key) {
            return Err(Error::new(ErrorKind::NotFound, "Key not found."));
        }
        Ok(self.update_index(key, 0, 0, Operation::DELETE))
    }

    fn update_index(&mut self, key: &str, offset: u64, length: usize, operation: Operation) {
        match operation {
            Operation::ADD | Operation::UPDATE => {
                self.indexes.insert(
                    key.to_string(),
                    IndexBucket {
                        offset: *offset,
                        length: *length,
                    },
                );

                write(
                    &self.config.log_path_index,
                    &DBIndex::new(
                        key.to_string(),
                        *offset,
                        *length,
                        operation,
                        Utc::now().timestamp_millis(),
                    ),
                )
                .expect("Unable to write to index log file");
            }
            Operation::DELETE => {
                self.indexes.remove(key);

                write(
                    &self.config.log_path_index,
                    &DBIndex::new(
                        key.to_string(),
                        0,
                        0,
                        Operation::DELETE,
                        Utc::now().timestamp_millis(),
                    ),
                )
                .expect("Unable to remove index");
            }
        }
    }

    fn restore_indexes(&mut self) {
        let mut db_indexes: HashMap<String, IndexBucket> = HashMap::new();
        match restore::<DBIndex>(&self.config.log_path_index) {
            Ok(Some(data)) => {
                for index in data {
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
            Err(err) => {}
        }
    }
}
