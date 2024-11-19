use std::collections::HashMap;

struct SimpleDatabase {
    db_name: String,
    db_path: String,
    db_segments: List<String>,
    db_recovery: String,
}

impl SimpleDatabase {
    fn new() -> SimpleDatabase {}
    fn db_set(key: &str, value: &str) {}
    fn db_get(key: &str) -> String {}
    fn db_get_metadata(&self) -> String {}
}


