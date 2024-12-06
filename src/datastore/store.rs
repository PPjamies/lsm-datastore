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
    pub fn put(&mut self, data: DBData) -> Result<()> {
        match write(&self.config.log_path_db, &data) {
            Ok((offset, length)) => {
                if self.indexes.contains_key(&data.key) {
                    self.update_index(data.key(), offset, length, Operation::UPDATE)?;
                }
            }
            Err(err) => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!(
                        "Error while writing to log file: {}, {}",
                        self.config.log_path_db, err
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Read - if key is index then read from byte offset, otherwise, scan entire db for key
    pub fn get(&self, key: &str) -> Result<DBData> {
        if let Some(data) = self.indexes.get(key) {
            return read(&self.config.log_path_db, data.offset, data.length);
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
    pub fn create_index(&mut self, key: &str) -> Result<()> {
        if self.indexes.contains_key(key) {
            return Err(Error::new(ErrorKind::AlreadyExists, "Key already exists"));
        }

        match scan::<DBData>(&self.config.log_path_index, &key) {
            Ok(Some((data, offset, length))) => {
                self.update_index(data.key(), offset, length, Operation::ADD)?;
                Ok(())
            }
            Ok(None) => Err(Error::new(
                ErrorKind::NotFound,
                "Key not found in index log",
            )),
            Err(err) => Err(err),
        }
    }

    fn write_index_log(
        &self,
        key: &str,
        offset: u64,
        length: usize,
        operation: Operation,
    ) -> Result<()> {
        write(
            &self.config.log_path_index,
            &DBIndex::new(
                key.to_string(),
                offset,
                length,
                operation,
                Utc::now().timestamp_millis(),
            ),
        )?;
        Ok(())
    }

    /// Updates an index
    fn update_index(
        &mut self,
        key: &str,
        offset: u64,
        length: usize,
        operation: Operation,
    ) -> Result<()> {
        match &operation {
            Operation::ADD | Operation::UPDATE => {
                self.indexes
                    .insert(key.to_string(), IndexBucket { offset, length });
                self.write_index_log(key, offset, length, operation)?;
            }
            Operation::DELETE => {
                self.indexes.remove(key);
                self.write_index_log(key, 0, 0, operation)?;
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use super::*;

    fn setup() -> Result<()> {
        // create log file
        // create index file

        // create config
        // create db store

        // create data
        // create offset

        Ok(())
    }

    fn tear_down(log_path: &str, index_path: &str) -> Result<()> {
        let mut path = Path::new(log_path);
        if path.exists() {
            fs::remove_file(path)?;
        }

        path = Path::new(index_path);
        if path.exists() {
            fs::remove_file(path)?;
        }
        Ok(())
    }

    #[test]
    fn store_without_index() {
        // write

        // get

        // todo: remove
    }

    #[test]
    fn store_with_index() {
        // write

        // get

        // todo: remove
    }
}
