use std::collections::HashMap;

pub struct SimpleDatastore {
    pub db_name: String,
    pub db_path: String,
    pub db_segments: Vec<String>,
    pub db_recovery: String,
    pub db_indexes: HashMap<String, String>,
}

impl SimpleDatastore {
    pub fn new() -> SimpleDatastore {
        SimpleDatastore {
            db_name: String::new(),
            db_path: String::new(),
            db_segments: Vec::new(),
            db_recovery: String::new(),
            db_indexes: HashMap::new(),
        }
    }

    pub fn set(&mut self, _key: &str, _value: &str) {}
    pub fn set_index(&mut self, _key: &str, _index: &str) {}
    pub fn get(_key: &str) -> String { String::new() }
    pub fn get_index(_key: &str) -> String { String::new() }
    pub fn db_get_metadata(&self) -> String {
        format!(
            "Database: {}, Path: {}, Recovery: {}, Segments: {:?}",
            self.db_name, self.db_path, self.db_recovery, self.db_segments
        )
    }
}


