mod file_handler;

use file_hander::Data;
use std::collections::HashMap;

pub struct DBConfig {
    pub db_name: String,
    pub db_path: String,
    pub db_segments: Vec<String>,
    pub db_recovery: String,
}

impl DBConfig {
    pub fn new(db_name: String, db_path: String, db_segments: Vec<String>, db_recovery: String) -> DBConfig {
        DBConfig {
            db_name,
            db_path,
            db_segments,
            db_recovery,
        }
    }
    pub fn print_config(&self) -> String {
        format!(
            "Database: {}, Path: {}, Recovery: {}, Segments: {:?}",
            self.db_name, self.db_path, self.db_recovery, self.db_segments
        )
    }
}

pub struct SimpleDatastore {
    pub db_config: DBConfig,
    pub db_indexes: HashMap<String, String>,
}

impl SimpleDatastore {
    pub fn new(db_config: DBConfig) -> SimpleDatastore {
        SimpleDatastore {
            db_config,
            db_indexes: HashMap::new(),
        }
    }
    pub fn set(&mut self, data: &Data) {}
    pub fn set_index(&mut self, index: &str, data: &Data) {}
    pub fn get(key: &str) -> String { String::new() }
    pub fn get_index(index: &str) -> String { String::new() }
}
