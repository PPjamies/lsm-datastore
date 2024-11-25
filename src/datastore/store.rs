use crate::data::Data;
use std::collections::HashMap;

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
    pub fn set(&mut self, data: Data) {}
    pub fn get(&self, key: &str) -> String {
        String::new()
    }
    pub fn set_index(&mut self, index: &str, data: Data) {}
    pub fn get_index(&self, index: &str) -> String {
        String::new()
    }
}
