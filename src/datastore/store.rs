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
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use std::fs;
//     use std::fs::File;
//     use std::path::Path;
//
//     fn setup() -> Result<(DBStore, DBData, DBData)> {
//         // create log file
//         let mut temp_dir = std::env::temp_dir();
//         let mut temp_path = temp_dir.join("test_log_db.txt");
//         let db_path = temp_path.to_string_lossy().to_string();
//         if !temp_path.exists() {
//             File::create(&temp_path)?;
//         }
//
//         // create index file
//         temp_dir = std::env::temp_dir();
//         temp_path = temp_dir.join("test_log_index.txt");
//         let index_path = temp_path.to_string_lossy().to_string();
//         if !temp_path.exists() {
//             File::create(&temp_path)?;
//         }
//
//         let mut db: DBStore = DBStore::new(DBConfig::new(db_path, index_path));
//
//         let old_data: DBData = DBData::new(
//             String::from("test-key"),
//             String::from("test-value"),
//             Operation::ADD,
//             Utc::now().timestamp_millis(),
//         );
//
//         let new_data: DBData = DBData::new(
//             String::from("test-key-2"),
//             String::from("test-value-2"),
//             Operation::ADD,
//             Utc::now().timestamp_millis(),
//         );
//
//         Ok((db, old_data, new_data))
//     }
//
//     fn tear_down(log_path: &str, index_path: &str) -> Result<()> {
//         let mut path = Path::new(log_path);
//         if path.exists() {
//             fs::remove_file(path)?;
//         }
//
//         path = Path::new(index_path);
//         if path.exists() {
//             fs::remove_file(path)?;
//         }
//         Ok(())
//     }
//
//     #[test]
//     fn store_without_index() {
//         let (mut db, old_data, new_data) = setup().unwrap();
//
//         let key: &str = old_data.key();
//
//         if let Err(err) = db.put(old_data) {
//             panic!("Put failed: {}", err);
//         }
//
//         if let Err(err) = db.put(new_data) {
//             panic!("Put failed: {}", err);
//         }
//
//         match db.get(key) {
//             Ok(data) => {
//                 assert_eq!(
//                     data, new_data,
//                     "Data mismatch. Scanning did not return the latest data."
//                 );
//             }
//             Err(err) => panic!("Get failed: {}", err),
//         }
//
//         tear_down(
//             &db.config.log_path_db,
//             &db.config.log_path_index,
//         )
//         .unwrap()
//     }
//
//     #[test]
//     fn store_with_index() {
//         let (mut db, old_data, new_data) = setup().unwrap();
//
//         let key: &str = old_data.key();
//
//         // put data into log file
//         if let Err(err) = db.put(old_data) {
//             panic!("Put failed: {}", err);
//         }
//
//         if let Err(err) = db.put(new_data) {
//             panic!("Put failed: {}", err);
//         }
//
//         // create index
//         match db.create_index(key) {
//             Ok(_) => {
//                 assert_eq!(db.indexes.len(), 1);
//                 assert!(db.indexes.contains_key(key));
//             }
//             Err(err) => panic!("Create index failed: {}", err),
//         }
//
//         // attempt to create index again should fail
//         let create_index_result = db.create_index(key);
//         assert!(create_index_result.is_err(), "Expected an error but got {:?}", create_index_result);
//
//         match db.get(key) {
//             Ok(data) => {
//                 assert_eq!(
//                     data, new_data,
//                     "Data mismatch. Reading from index did not return the latest data."
//                 );
//             }
//             Err(err) => panic!("Get failed: {}", err),
//         }
//
//         tear_down(
//             &db.config.log_path_db,
//             &db.config.log_path_index,
//         )
//         .unwrap()
//     }
// }
