use crate::datastore::data::DBData;
use crate::datastore::DBConfig;
use crate::datastore::DBIndex;

use crate::datastore::indexable::Indexable;
use crate::datastore::operation::Operation;
use crate::file::log_handler::{read, restore_indexes, scan, write};
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

    /// Creates or updates a key/val pair (DBData)
    pub fn put(&mut self, key: String, val: String) -> Result<(u64, usize)> {
        let data: DBData = DBData::new(key, val, Operation::ADD, Utc::now().timestamp_millis());

        match write::<DBData>(&self.config.log_path_db, &data) {
            Ok((offset, length)) => {
                if self.indexes.contains_key(&data.key) {
                    self.update_index(data.key(), offset, length, Operation::UPDATE)?;
                }
                Ok((offset, length))
            }
            Err(err) => {
                Err(Error::new(
                    ErrorKind::Other,
                    format!(
                        "Error while writing to log file: {}, {}",
                        self.config.log_path_db, err
                    ),
                ))
            }
        }
    }

    /// Read - if key is index then read from byte offset, otherwise, scan entire db for key
    pub fn get(&self, key: &str) -> Result<DBData> {
        if let Some(data) = self.indexes.get(key) {
            println!("Get() {:?}", data);
            return read::<DBData>(&self.config.log_path_db, data.offset, data.length);
        }

        match scan::<DBData>(&self.config.log_path_db, &key) {
            Ok(Some((data, _, _))) => Ok(data),
            Ok(None) => Err(Error::new(ErrorKind::NotFound, "Key not found in log file")),
            Err(err) => Err(err),
        }
    }

    /// Restores in-memory index map from index log file
    fn restore_indexes(&mut self) {
        match restore_indexes(&self.config.log_path_index) {
            Ok(indexes) => {
                self.indexes = indexes;
            }
            Err(err) => {
                eprintln!("Unable to restore indexes. {}", err);
            }
        }
    }

    /// Creates an index and mark index for creation (crash recovery)
    pub fn create_index(&mut self, key: &str) -> Result<(u64, usize)> {
        if self.indexes.contains_key(key) {
            return Err(Error::new(ErrorKind::AlreadyExists, "Key already exists"));
        }

        match scan::<DBData>(&self.config.log_path_db, &key) {
            Ok(Some((data, offset, length))) => {
                self.update_index(data.key(), offset, length, Operation::ADD)?;
                Ok((offset, length))
            }
            Ok(None) => Err(Error::new(
                ErrorKind::NotFound,
                "Key not found in index log",
            )),
            Err(err) => Err(err),
        }
    }

    pub fn write_index_log(
        &self,
        key: &str,
        offset: u64,
        length: usize,
        operation: Operation,
    ) -> Result<()> {
        write::<DBIndex>(&self.config.log_path_index, &DBIndex::new(
            key.to_string(),
            offset,
            length,
            operation,
            Utc::now().timestamp_millis()))?;
        Ok(())
    }

    /// Updates an index
    pub fn update_index(
        &mut self,
        key: &str,
        offset: u64,
        length: usize,
        operation: Operation,
    ) -> Result<()> {
        match &operation {
            Operation::ADD | Operation::UPDATE => {
                self.write_index_log(key, offset, length, operation)?;
                self.indexes
                    .insert(key.to_string(), IndexBucket { offset, length });
            }
            Operation::DELETE => {
                self.write_index_log(key, 0, 0, operation)?;
                self.indexes.remove(key);
            }
        }
        Ok(())
    }

    /// Removes index from in-memory map and mark index for deletion (crash recovery)
    pub fn delete_index(&mut self, key: &str) -> Result<()> {
        self.indexes.remove(key);
        self.write_index_log(key, 0, 0, Operation::DELETE)?;
        Ok(())
    }
}
