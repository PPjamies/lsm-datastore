use crate::data::Data;
use crate::datastore::DBConfig;
use crate::datastore::DBIndex;
use crate::log_handler::{append_to_log, find_in_log};

use crate::datastore::index::Operation;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Result};

pub struct DBStore {
    config: DBConfig,
    indexes: HashMap<String, String>,
}

impl DBStore {
    pub fn new(config: DBConfig) -> Self {
        Self {
            config,
            indexes: HashMap::new(),
        }
    }

    pub fn get_config(&self) -> &DBConfig {
        &self.config
    }

    pub fn set(&mut self, data: Data) {
        append_to_log(self.config.get_log_path_db(), &data).expect("Unable to set data!");
    }

    pub fn get(&self, key: &str) -> Result<Option<Data>> {
        find_in_log(self.config.get_log_path_db(), &key)
    }

    fn restore_indexes(&mut self) -> Result<()> {
        let log_path_indexes: &str = self.get_config().get_log_path_db();

        let file: File = File::open(log_path_indexes)?;
        let reader: BufReader<File> = BufReader::new(file);

        let indexes: Vec<DBIndex> = bincode::deserialize_from(reader)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

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
