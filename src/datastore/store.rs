use crate::data::Data;
use crate::log_handler::{append_to_log, find_in_log};
use std::collections::HashMap;
use std::io::Result;

pub struct DBStore {
    pub config: crate::datastore::DBConfig,
    pub indexes: HashMap<String, String>,
}

impl DBStore {
    pub fn new(config: crate::datastore::DBConfig) -> Self {
        Self {
            config,
            indexes: HashMap::new(),
        }
    }

    pub fn set(&mut self, data: Data) {
        append_to_log(self.config.get_db_path(), &data).expect("Unable to set data!");
    }

    pub fn get(&self, key: &str) -> Result<Option<Data>> {
        find_in_log(self.config.get_db_path(), &key)
    }

    pub fn set_index(&mut self, index: &str, data: Data) {}

    pub fn get_index(&self, index: &str) -> String {
        String::new()
    }
}
